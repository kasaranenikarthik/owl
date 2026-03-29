package main

import (
	"context"
	"encoding/json"
	"fmt"
	"image"
	"log/slog"
	"math"
	"os"
	"strconv"
	"sync"
	"time"

	"yolo-realtime/internal/config"
	"yolo-realtime/internal/kafka"
	"yolo-realtime/internal/metrics"
	"yolo-realtime/internal/models"
	"yolo-realtime/internal/shutdown"
	"yolo-realtime/internal/triton"

	"gocv.io/x/gocv"
)

var cocoClasses = []string{
	"person", "bicycle", "car", "motorcycle", "airplane", "bus", "train",
	"truck", "boat", "traffic light", "fire hydrant", "stop sign",
	"parking meter", "bench", "bird", "cat", "dog", "horse", "sheep",
	"cow", "elephant", "bear", "zebra", "giraffe", "backpack", "umbrella",
	"handbag", "tie", "suitcase", "frisbee", "skis", "snowboard",
	"sports ball", "kite", "baseball bat", "baseball glove", "skateboard",
	"surfboard", "tennis racket", "bottle", "wine glass", "cup", "fork",
	"knife", "spoon", "bowl", "banana", "apple", "sandwich", "orange",
	"broccoli", "carrot", "hot dog", "pizza", "donut", "cake", "chair",
	"couch", "potted plant", "bed", "dining table", "toilet", "tv",
	"laptop", "mouse", "remote", "keyboard", "cell phone", "microwave",
	"oven", "toaster", "sink", "refrigerator", "book", "clock", "vase",
	"scissors", "teddy bear", "hair drier", "toothbrush",
}

const (
	inputW    = 640
	inputH    = 640
	modelName = "yolov8s"
)

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	logger := newLogger()
	cfg := config.Load()

	ctx, cancel := shutdown.WithSignal(context.Background(), logger)
	defer cancel()

	tritonAddr := envStr("TRITON_HTTP_ADDR", "localhost:8000")
	numWorkers := envInt("NUM_WORKERS", 8)

	ic := mustCreateInferenceClient(tritonAddr, cfg.ConfidenceThreshold, logger)
	defer ic.Close()

	producer := mustCreateProducer(cfg, logger)
	defer producer.Close()

	pool := newWorkerPool(ctx, numWorkers, ic, producer, cfg.DetectionsTopic, logger)
	defer pool.Shutdown()

	consumer := mustCreateConsumer(cfg, pool, logger)

	go func() { metrics.Serve(ctx, cfg.MetricsPort, logger) }()

	logger.Info("inference bridge starting",
		"triton", tritonAddr,
		"workers", numWorkers,
		"group", cfg.ConsumerGroup,
	)
	if err := consumer.Run(ctx); err != nil {
		logger.Error("consumer exited", "error", err)
		os.Exit(1)
	}
}

// ---------------------------------------------------------------------------
// Initialization
// ---------------------------------------------------------------------------

func newLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

func mustCreateInferenceClient(addr string, confThresh float32, logger *slog.Logger) *inferenceClient {
	tc, err := triton.NewClient("http://"+addr, modelName, logger)
	if err != nil {
		logger.Error("triton connect failed", "error", err)
		os.Exit(1)
	}
	return &inferenceClient{client: tc, confThresh: confThresh, logger: logger}
}

func mustCreateProducer(cfg config.Config, logger *slog.Logger) *kafka.Producer {
	p, err := kafka.NewProducer(cfg.KafkaBrokers, logger)
	if err != nil {
		logger.Error("producer failed", "error", err)
		os.Exit(1)
	}
	return p
}

func mustCreateConsumer(cfg config.Config, pool *workerPool, logger *slog.Logger) *kafka.Consumer {
	c, err := kafka.NewConsumer(
		cfg.KafkaBrokers, cfg.ConsumerGroup,
		[]string{cfg.FramesTopic},
		pool.dispatch,
		logger,
	)
	if err != nil {
		logger.Error("consumer failed", "error", err)
		os.Exit(1)
	}
	return c
}

// ---------------------------------------------------------------------------
// Worker pool
// ---------------------------------------------------------------------------

type frameJob struct {
	frame models.FrameMessage
}

type workerPool struct {
	jobs   chan frameJob
	wg     sync.WaitGroup
	ic     *inferenceClient
	prod   *kafka.Producer
	topic  string
	logger *slog.Logger
}

func newWorkerPool(
	ctx context.Context,
	n int,
	ic *inferenceClient,
	prod *kafka.Producer,
	topic string,
	logger *slog.Logger,
) *workerPool {
	p := &workerPool{
		jobs:   make(chan frameJob, n*2),
		ic:     ic,
		prod:   prod,
		topic:  topic,
		logger: logger,
	}
	metrics.WorkerPoolCapacity.Set(float64(n))

	p.wg.Add(n)
	for i := 0; i < n; i++ {
		go p.worker(ctx, i)
	}
	logger.Info("worker pool started", "workers", n, "buffer", n*2)
	return p
}

func (p *workerPool) dispatch(ctx context.Context, key, value []byte) error {
	var frame models.FrameMessage
	if err := json.Unmarshal(value, &frame); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	if time.Since(frame.Timestamp) > 2*time.Second {
		metrics.FramesDropped.Inc()
		return nil
	}
	select {
	case p.jobs <- frameJob{frame: frame}:
		metrics.WorkerPoolQueueDepth.Set(float64(len(p.jobs)))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *workerPool) worker(ctx context.Context, id int) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-p.jobs:
			if !ok {
				return
			}
			metrics.WorkerPoolQueueDepth.Set(float64(len(p.jobs)))
			p.processJob(ctx, job)
		}
	}
}

func (p *workerPool) processJob(ctx context.Context, job frameJob) {
	metrics.WorkerPoolActive.Inc()
	defer metrics.WorkerPoolActive.Dec()

	frame := job.frame
	if time.Since(frame.Timestamp) > 2*time.Second {
		metrics.FramesDropped.Inc()
		return
	}

	dets, ms, err := p.ic.Infer(ctx, frame.Data)
	if err != nil {
		p.logger.Warn("inference failed", "client_id", frame.ClientID, "error", err)
		return
	}

	metrics.FramesProcessed.Inc()
	metrics.InferenceDuration.Observe(ms / 1000.0)
	metrics.DetectionsPerFrame.Observe(float64(len(dets)))

	result := models.InternalResult{
		ClientID:    frame.ClientID,
		FrameID:     frame.FrameID,
		Detections:  dets,
		InferenceMs: ms,
		Timestamp:   time.Now(),
	}
	if err := p.prod.Publish(p.topic, frame.ClientID, result); err != nil {
		p.logger.Warn("publish failed", "client_id", frame.ClientID, "error", err)
	}
}

func (p *workerPool) Shutdown() {
	close(p.jobs)
	p.wg.Wait()
}

// ---------------------------------------------------------------------------
// Inference client — wraps Triton HTTP with pre/post processing
// ---------------------------------------------------------------------------

type inferenceClient struct {
	client     *triton.Client
	confThresh float32
	logger     *slog.Logger
}

func (ic *inferenceClient) Infer(ctx context.Context, jpegData []byte) ([]models.Detection, float64, error) {
	start := time.Now()

	mat, err := gocv.IMDecode(jpegData, gocv.IMReadColor)
	if err != nil {
		return nil, 0, fmt.Errorf("decode: %w", err)
	}
	defer mat.Close()

	origW, origH := float32(mat.Cols()), float32(mat.Rows())
	inputData := preprocess(mat)

	output, err := ic.client.Infer(inputData, 1, 3, inputH, inputW)
	if err != nil {
		return nil, 0, fmt.Errorf("triton: %w", err)
	}

	dets := postprocess(output, origW, origH, ic.confThresh)
	ms := time.Since(start).Seconds() * 1000
	return dets, ms, nil
}

func (ic *inferenceClient) Close() { ic.client.Close() }

// ---------------------------------------------------------------------------
// Preprocessing
// ---------------------------------------------------------------------------

func preprocess(mat gocv.Mat) []float32 {
	resized := gocv.NewMat()
	defer resized.Close()
	gocv.Resize(mat, &resized, image.Pt(inputW, inputH), 0, 0, gocv.InterpolationLinear)

	rgb := gocv.NewMat()
	defer rgb.Close()
	gocv.CvtColor(resized, &rgb, gocv.ColorBGRToRGB)

	fMat := gocv.NewMat()
	defer fMat.Close()
	rgb.ConvertFoWithParams(&fMat, gocv.MatTypeCV32FC3, 1.0/255.0, 0)

	data := make([]float32, 3*inputH*inputW)
	for y := 0; y < inputH; y++ {
		for x := 0; x < inputW; x++ {
			px := fMat.GetVecfAt(y, x)
			idx := y*inputW + x
			data[0*inputH*inputW+idx] = px[0]
			data[1*inputH*inputW+idx] = px[1]
			data[2*inputH*inputW+idx] = px[2]
		}
	}
	return data
}

// ---------------------------------------------------------------------------
// Postprocessing
// ---------------------------------------------------------------------------

type rawDet struct {
	x1, y1, x2, y2, conf float32
	cls                  int
}

func postprocess(out []float32, origW, origH, confThresh float32) []models.Detection {
	const numClasses, numBoxes = 80, 8400

	var cands []rawDet
	scX, scY := origW/float32(inputW), origH/float32(inputH)

	for i := 0; i < numBoxes; i++ {
		cx := out[0*numBoxes+i]
		cy := out[1*numBoxes+i]
		w := out[2*numBoxes+i]
		h := out[3*numBoxes+i]

		bestScore, bestCls := float32(0), 0
		for c := 0; c < numClasses; c++ {
			s := out[(4+c)*numBoxes+i]
			if s > bestScore {
				bestScore, bestCls = s, c
			}
		}
		if bestScore < confThresh {
			continue
		}
		cands = append(cands, rawDet{
			x1: (cx - w/2) * scX, y1: (cy - h/2) * scY,
			x2: (cx + w/2) * scX, y2: (cy + h/2) * scY,
			conf: bestScore, cls: bestCls,
		})
	}

	kept := nms(cands, 0.45)
	dets := make([]models.Detection, len(kept))
	for i, d := range kept {
		cn := "unknown"
		if d.cls < len(cocoClasses) {
			cn = cocoClasses[d.cls]
		}
		dets[i] = models.Detection{BoundingBox: models.BoundingBox{
			X1: d.x1, Y1: d.y1, X2: d.x2, Y2: d.y2,
			Confidence: d.conf, ClassID: d.cls, ClassName: cn,
		}}
	}
	return dets
}

func nms(dets []rawDet, thresh float32) []rawDet {
	for i := range dets {
		for j := i + 1; j < len(dets); j++ {
			if dets[j].conf > dets[i].conf {
				dets[i], dets[j] = dets[j], dets[i]
			}
		}
	}
	supp := make([]bool, len(dets))
	var kept []rawDet
	for i := range dets {
		if supp[i] {
			continue
		}
		kept = append(kept, dets[i])
		for j := i + 1; j < len(dets); j++ {
			if !supp[j] && iou(dets[i], dets[j]) > thresh {
				supp[j] = true
			}
		}
	}
	return kept
}

func iou(a, b rawDet) float32 {
	x1 := float32(math.Max(float64(a.x1), float64(b.x1)))
	y1 := float32(math.Max(float64(a.y1), float64(b.y1)))
	x2 := float32(math.Min(float64(a.x2), float64(b.x2)))
	y2 := float32(math.Min(float64(a.y2), float64(b.y2)))
	inter := float32(math.Max(0, float64(x2-x1))) * float32(math.Max(0, float64(y2-y1)))
	aA := (a.x2 - a.x1) * (a.y2 - a.y1)
	aB := (b.x2 - b.x1) * (b.y2 - b.y1)
	u := aA + aB - inter
	if u == 0 {
		return 0
	}
	return inter / u
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func envStr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envInt(k string, d int) int {
	v, err := strconv.Atoi(os.Getenv(k))
	if err != nil {
		return d
	}
	return v
}
