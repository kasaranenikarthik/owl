package main

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/jpeg"
	"log/slog"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"yolo-realtime/internal/config"
	"yolo-realtime/internal/kafka"
	"yolo-realtime/internal/metrics"
	"yolo-realtime/internal/models"
	"yolo-realtime/internal/shutdown"
	"yolo-realtime/internal/triton"

	"golang.org/x/image/draw"
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

var (
	rgbaPool = sync.Pool{
		New: func() any {
			return image.NewRGBA(image.Rect(0, 0, inputW, inputH))
		},
	}
	inputTensorPool = sync.Pool{
		New: func() any {
			return make([]float32, 3*inputH*inputW)
		},
	}
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
		"frames_topic_encoding", "raw_bytes_with_headers_v1",
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
	frame      models.FrameMessage
	enqueuedAt time.Time
}

type workerPool struct {
	jobs        chan string
	wg          sync.WaitGroup
	ic          *inferenceClient
	prod        *kafka.Producer
	topic       string
	logger      *slog.Logger
	maxFrameAge time.Duration
	batchSize   int
	batchWait   time.Duration

	pendingMu sync.Mutex
	pending   map[string]frameJob
	queued    map[string]bool
}

func newWorkerPool(
	ctx context.Context,
	n int,
	ic *inferenceClient,
	prod *kafka.Producer,
	topic string,
	logger *slog.Logger,
) *workerPool {
	maxFrameAge := time.Duration(envInt("STALE_FRAME_THRESHOLD_MS", 5000)) * time.Millisecond
	batchSize := envInt("MICROBATCH_MAX_SIZE", 4)
	if batchSize < 1 {
		batchSize = 1
	}
	batchWaitMs := envInt("MICROBATCH_WAIT_MS", 2)
	if batchWaitMs < 0 {
		batchWaitMs = 0
	}
	queueSize := n * 4
	p := &workerPool{
		jobs:        make(chan string, queueSize),
		ic:          ic,
		prod:        prod,
		topic:       topic,
		logger:      logger,
		maxFrameAge: maxFrameAge,
		batchSize:   batchSize,
		batchWait:   time.Duration(batchWaitMs) * time.Millisecond,
		pending:     make(map[string]frameJob),
		queued:      make(map[string]bool),
	}
	metrics.WorkerPoolCapacity.Set(float64(n))

	p.wg.Add(n)
	for i := 0; i < n; i++ {
		go p.worker(ctx, i)
	}
	logger.Info("worker pool started",
		"workers", n,
		"queue_slots", queueSize,
		"stale_threshold_ms", maxFrameAge.Milliseconds(),
		"microbatch_max_size", batchSize,
		"microbatch_wait_ms", batchWaitMs,
	)
	return p
}

func frameFromKafkaMessage(msg kafka.Message) (models.FrameMessage, error) {
	clientID := string(msg.Key)
	if clientID == "" {
		return models.FrameMessage{}, fmt.Errorf("missing client key")
	}

	frameIDBytes, ok := msg.Headers["frame_id"]
	if !ok {
		return models.FrameMessage{}, fmt.Errorf("missing frame_id header")
	}
	frameID, err := strconv.ParseUint(string(frameIDBytes), 10, 64)
	if err != nil {
		return models.FrameMessage{}, fmt.Errorf("parse frame_id: %w", err)
	}

	timestampBytes, ok := msg.Headers["timestamp_unix_nano"]
	if !ok {
		return models.FrameMessage{}, fmt.Errorf("missing timestamp_unix_nano header")
	}
	timestampNanos, err := strconv.ParseInt(string(timestampBytes), 10, 64)
	if err != nil {
		return models.FrameMessage{}, fmt.Errorf("parse timestamp_unix_nano: %w", err)
	}

	return models.FrameMessage{
		ClientID:  clientID,
		FrameID:   frameID,
		Timestamp: time.Unix(0, timestampNanos),
		Data:      msg.Value,
	}, nil
}

func (p *workerPool) dispatch(ctx context.Context, msg kafka.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	frame, err := frameFromKafkaMessage(msg)
	if err != nil {
		return fmt.Errorf("decode frame message: %w", err)
	}
	metrics.InferenceFramesConsumed.Inc()
	if time.Since(frame.Timestamp) > p.maxFrameAge {
		metrics.FramesDropped.Inc()
		metrics.FramesDroppedByReason.WithLabelValues("stale").Inc()
		return nil
	}

	p.pendingMu.Lock()
	hadPending := false
	if _, ok := p.pending[frame.ClientID]; ok {
		hadPending = true
	}
	p.pending[frame.ClientID] = frameJob{frame: frame, enqueuedAt: time.Now()}
	if p.queued[frame.ClientID] {
		p.pendingMu.Unlock()
		if hadPending {
			metrics.FramesDropped.Inc()
			metrics.FramesDroppedByReason.WithLabelValues("superseded").Inc()
		}
		return nil
	}
	select {
	case p.jobs <- frame.ClientID:
		p.queued[frame.ClientID] = true
		queueDepth := len(p.jobs)
		p.pendingMu.Unlock()
		metrics.WorkerPoolQueueDepth.Set(float64(queueDepth))
		return nil
	default:
		delete(p.pending, frame.ClientID)
		p.pendingMu.Unlock()
		metrics.FramesDropped.Inc()
		metrics.FramesDroppedByReason.WithLabelValues("queue_full").Inc()
		return nil
	}
}

func (p *workerPool) worker(ctx context.Context, id int) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case clientID, ok := <-p.jobs:
			if !ok {
				return
			}

			batch := p.collectBatch(ctx, clientID)
			if len(batch) == 0 {
				continue
			}
			p.processBatch(ctx, batch)
		}
	}
}

func (p *workerPool) takePendingJob(clientID string) (frameJob, bool, int) {
	p.pendingMu.Lock()
	defer p.pendingMu.Unlock()

	job, exists := p.pending[clientID]
	delete(p.pending, clientID)
	p.queued[clientID] = false
	return job, exists, len(p.jobs)
}

func (p *workerPool) collectBatch(ctx context.Context, firstClientID string) []frameJob {
	assemblyStart := time.Now()
	batch := make([]frameJob, 0, p.batchSize)

	appendJob := func(clientID string) bool {
		job, exists, queueDepth := p.takePendingJob(clientID)
		metrics.WorkerPoolQueueDepth.Set(float64(queueDepth))
		if !exists {
			return false
		}
		metrics.WorkerQueueWaitDuration.Observe(time.Since(job.enqueuedAt).Seconds())
		batch = append(batch, job)
		return true
	}

	appendJob(firstClientID)
	if len(batch) == 0 || p.batchSize == 1 {
		metrics.MicrobatchSize.Observe(float64(len(batch)))
		metrics.MicrobatchAssemblyDuration.Observe(time.Since(assemblyStart).Seconds())
		return batch
	}

	for len(batch) < p.batchSize {
		select {
		case clientID, ok := <-p.jobs:
			if !ok {
				metrics.MicrobatchSize.Observe(float64(len(batch)))
				metrics.MicrobatchAssemblyDuration.Observe(time.Since(assemblyStart).Seconds())
				return batch
			}
			appendJob(clientID)
		default:
			goto maybeWait
		}
	}

	metrics.MicrobatchSize.Observe(float64(len(batch)))
	metrics.MicrobatchAssemblyDuration.Observe(time.Since(assemblyStart).Seconds())
	return batch

maybeWait:
	if p.batchWait <= 0 {
		metrics.MicrobatchSize.Observe(float64(len(batch)))
		metrics.MicrobatchAssemblyDuration.Observe(time.Since(assemblyStart).Seconds())
		return batch
	}

	timer := time.NewTimer(p.batchWait)
	defer timer.Stop()

	for len(batch) < p.batchSize {
		select {
		case <-ctx.Done():
			metrics.MicrobatchSize.Observe(float64(len(batch)))
			metrics.MicrobatchAssemblyDuration.Observe(time.Since(assemblyStart).Seconds())
			return batch
		case <-timer.C:
			metrics.MicrobatchSize.Observe(float64(len(batch)))
			metrics.MicrobatchAssemblyDuration.Observe(time.Since(assemblyStart).Seconds())
			return batch
		case clientID, ok := <-p.jobs:
			if !ok {
				metrics.MicrobatchSize.Observe(float64(len(batch)))
				metrics.MicrobatchAssemblyDuration.Observe(time.Since(assemblyStart).Seconds())
				return batch
			}
			appendJob(clientID)
		}
	}

	metrics.MicrobatchSize.Observe(float64(len(batch)))
	metrics.MicrobatchAssemblyDuration.Observe(time.Since(assemblyStart).Seconds())
	return batch
}

func (p *workerPool) processBatch(ctx context.Context, jobs []frameJob) {
	metrics.WorkerPoolActive.Inc()
	defer metrics.WorkerPoolActive.Dec()

	frames := make([]models.FrameMessage, len(jobs))
	for i, job := range jobs {
		frames[i] = job.frame
	}

	detectionsByFrame, ms, err := p.ic.InferBatch(ctx, frames)
	if err != nil {
		p.logger.Warn("inference batch failed", "batch_size", len(jobs), "error", err)
		return
	}

	for index, frame := range frames {
		dets := detectionsByFrame[index]
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
		publishStart := time.Now()
		if err := p.prod.Publish(p.topic, frame.ClientID, result); err != nil {
			metrics.ResultPublishDuration.Observe(time.Since(publishStart).Seconds())
			metrics.ResultPublishFailures.Inc()
			p.logger.Warn("publish failed", "client_id", frame.ClientID, "frame_id", frame.FrameID, "error", err)
			continue
		}
		metrics.ResultPublishDuration.Observe(time.Since(publishStart).Seconds())
		metrics.InferenceResultsPublished.Inc()
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

func (ic *inferenceClient) InferBatch(ctx context.Context, frames []models.FrameMessage) ([][]models.Detection, float64, error) {
	start := time.Now()
	if len(frames) == 0 {
		return nil, 0, nil
	}

	batchSize := len(frames)
	const inputSampleSize = 3 * inputH * inputW
	batchedInput := make([]float32, batchSize*inputSampleSize)
	origDims := make([]struct {
		width  float32
		height float32
	}, batchSize)

	releases := make([]func(), 0, batchSize)
	defer func() {
		for _, release := range releases {
			release()
		}
	}()

	for index, frame := range frames {
		decodeStart := time.Now()
		img, err := jpeg.Decode(bytes.NewReader(frame.Data))
		metrics.DecodeDuration.Observe(time.Since(decodeStart).Seconds())
		if err != nil {
			return nil, 0, fmt.Errorf("decode frame %d: %w", index, err)
		}

		bounds := img.Bounds()
		origDims[index].width = float32(bounds.Dx())
		origDims[index].height = float32(bounds.Dy())

		preprocessStart := time.Now()
		inputData, release, err := preprocess(img)
		metrics.PreprocessDuration.Observe(time.Since(preprocessStart).Seconds())
		if err != nil {
			return nil, 0, fmt.Errorf("preprocess frame %d: %w", index, err)
		}
		releases = append(releases, release)
		copy(batchedInput[index*inputSampleSize:(index+1)*inputSampleSize], inputData)
	}

	tritonStart := time.Now()
	output, err := ic.client.Infer(batchedInput, batchSize, 3, inputH, inputW)
	metrics.TritonRoundTripDuration.Observe(time.Since(tritonStart).Seconds())
	if err != nil {
		return nil, 0, fmt.Errorf("triton: %w", err)
	}

	const outputSampleSize = (4 + 80) * 8400
	if len(output) < batchSize*outputSampleSize {
		return nil, 0, fmt.Errorf("unexpected output size: got %d floats for batch=%d", len(output), batchSize)
	}

	detectionsByFrame := make([][]models.Detection, batchSize)
	for index := range frames {
		postprocessStart := time.Now()
		startOffset := index * outputSampleSize
		endOffset := startOffset + outputSampleSize
		detectionsByFrame[index] = postprocess(
			output[startOffset:endOffset],
			origDims[index].width,
			origDims[index].height,
			ic.confThresh,
		)
		metrics.PostprocessDuration.Observe(time.Since(postprocessStart).Seconds())
	}

	ms := time.Since(start).Seconds() * 1000
	return detectionsByFrame, ms, nil
}

func (ic *inferenceClient) Close() { ic.client.Close() }

// ---------------------------------------------------------------------------
// Preprocessing
// ---------------------------------------------------------------------------

func preprocess(img image.Image) ([]float32, func(), error) {
	// Resize to input dimensions
	resized := rgbaPool.Get().(*image.RGBA)
	draw.ApproxBiLinear.Scale(resized, resized.Bounds(), img, img.Bounds(), draw.Over, nil)

	// Convert to float32 array (normalized 0-1, channels first: R, G, B).
	// Use direct RGBA pixel buffer access to avoid per-pixel interface calls.
	data := inputTensorPool.Get().([]float32)
	stride := resized.Stride
	pix := resized.Pix
	for y := 0; y < inputH; y++ {
		row := y * stride
		for x := 0; x < inputW; x++ {
			off := row + x*4
			r := pix[off+0]
			g := pix[off+1]
			b := pix[off+2]
			idx := y*inputW + x
			data[0*inputH*inputW+idx] = float32(r) / 255.0
			data[1*inputH*inputW+idx] = float32(g) / 255.0
			data[2*inputH*inputW+idx] = float32(b) / 255.0
		}
	}
	cleanup := func() {
		rgbaPool.Put(resized)
		inputTensorPool.Put(data)
	}
	return data, cleanup, nil
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
	sort.Slice(dets, func(i, j int) bool { return dets[i].conf > dets[j].conf })
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
