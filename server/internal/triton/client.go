package triton

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"time"
)

// Client talks to Triton Inference Server over HTTP.
type Client struct {
	baseURL       string
	modelName     string
	inputDatatype string
	httpClient    *http.Client
	logger        *slog.Logger
}

// NewClient creates a Triton HTTP client and verifies the server and model are ready.
func NewClient(baseURL, modelName, inputDatatype string, logger *slog.Logger) (*Client, error) {
	dtype := strings.ToUpper(strings.TrimSpace(inputDatatype))
	if dtype == "" {
		dtype = "FP32"
	}
	if dtype != "FP32" && dtype != "FP16" {
		return nil, fmt.Errorf("unsupported Triton input datatype: %s", inputDatatype)
	}

	c := &Client{
		baseURL:       baseURL,
		modelName:     modelName,
		inputDatatype: dtype,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		logger: logger,
	}

	if err := c.waitForReady(60 * time.Second); err != nil {
		return nil, err
	}

	logger.Info("triton connected", "url", baseURL, "model", modelName, "input_datatype", dtype)
	return c, nil
}

// Infer sends a preprocessed float32 tensor to Triton and returns the raw output.
// Input shape: [1, 3, H, W] float32
// Output: raw float32 slice from Triton
func (c *Client) Infer(inputData []float32, batchSize, channels, height, width int) ([]float32, error) {
	inputSizeBytes := len(inputData) * 4
	tensorBytes := float32SliceToBytes(inputData)
	if c.inputDatatype == "FP16" {
		inputSizeBytes = len(inputData) * 2
		tensorBytes = float32SliceToFloat16Bytes(inputData)
	}

	// Build the inference request with binary tensor extension
	reqBody := inferRequest{
		Inputs: []inferInput{
			{
				Name:     "images",
				Datatype: c.inputDatatype,
				Shape:    []int{batchSize, channels, height, width},
				Parameters: map[string]any{
					"binary_data_size": inputSizeBytes,
				},
			},
		},
		Outputs: []inferOutput{
			{
				Name: "output0",
				Parameters: map[string]any{
					"binary_data": true,
				},
			},
		},
	}

	jsonBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	// Binary extension: JSON header + raw tensor bytes appended

	body := make([]byte, 0, len(jsonBytes)+len(tensorBytes))
	body = append(body, jsonBytes...)
	body = append(body, tensorBytes...)

	url := fmt.Sprintf("%s/v2/models/%s/infer", c.baseURL, c.modelName)
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Inference-Header-Content-Length", fmt.Sprintf("%d", len(jsonBytes)))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("triton request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("triton returned %d: %s", resp.StatusCode, string(errBody))
	}

	// Parse response — JSON header followed by raw binary output
	return c.parseInferResponse(resp)
}

// Close releases HTTP client resources.
func (c *Client) Close() {
	c.httpClient.CloseIdleConnections()
}

// ---------------------------------------------------------------------------
// Health checks
// ---------------------------------------------------------------------------

func (c *Client) waitForReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if c.isServerReady() && c.isModelReady() {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("triton not ready after %v", timeout)
}

func (c *Client) isServerReady() bool {
	resp, err := c.httpClient.Get(fmt.Sprintf("%s/v2/health/ready", c.baseURL))
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (c *Client) isModelReady() bool {
	url := fmt.Sprintf("%s/v2/models/%s/ready", c.baseURL, c.modelName)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// ---------------------------------------------------------------------------
// Response parsing
// ---------------------------------------------------------------------------

func (c *Client) parseInferResponse(resp *http.Response) ([]float32, error) {
	// The response uses the binary data extension:
	// - JSON header (length indicated by Inference-Header-Content-Length)
	// - Raw binary tensor data follows

	headerLenStr := resp.Header.Get("Inference-Header-Content-Length")

	allBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	var headerLen int
	if headerLenStr != "" {
		fmt.Sscanf(headerLenStr, "%d", &headerLen)
	} else {
		// Fallback: try to find where JSON ends
		headerLen = findJSONEnd(allBytes)
	}

	if headerLen <= 0 || headerLen > len(allBytes) {
		return nil, fmt.Errorf("invalid header length: %d (body: %d bytes)", headerLen, len(allBytes))
	}

	// Parse JSON header to validate
	var inferResp inferResponse
	if err := json.Unmarshal(allBytes[:headerLen], &inferResp); err != nil {
		return nil, fmt.Errorf("parse response header: %w", err)
	}

	if len(inferResp.Outputs) == 0 {
		return nil, fmt.Errorf("no outputs in response")
	}

	// Extract binary tensor data after JSON header
	binaryData := allBytes[headerLen:]
	if len(binaryData) == 0 {
		return nil, fmt.Errorf("no binary data in response")
	}

	if len(inferResp.Outputs) > 0 && strings.EqualFold(inferResp.Outputs[0].Datatype, "FP16") {
		return bytesToFloat16AsFloat32Slice(binaryData), nil
	}

	return bytesToFloat32Slice(binaryData), nil
}

// ---------------------------------------------------------------------------
// Request/response types
// ---------------------------------------------------------------------------

type inferRequest struct {
	Inputs  []inferInput  `json:"inputs"`
	Outputs []inferOutput `json:"outputs"`
}

type inferInput struct {
	Name       string         `json:"name"`
	Datatype   string         `json:"datatype"`
	Shape      []int          `json:"shape"`
	Parameters map[string]any `json:"parameters,omitempty"`
}

type inferOutput struct {
	Name       string         `json:"name"`
	Parameters map[string]any `json:"parameters,omitempty"`
}

type inferResponse struct {
	ModelName    string                `json:"model_name"`
	ModelVersion string                `json:"model_version"`
	Outputs      []inferResponseOutput `json:"outputs"`
}

type inferResponseOutput struct {
	Name       string         `json:"name"`
	Datatype   string         `json:"datatype"`
	Shape      []int          `json:"shape"`
	Parameters map[string]any `json:"parameters,omitempty"`
}

// ---------------------------------------------------------------------------
// Binary helpers
// ---------------------------------------------------------------------------

func float32SliceToBytes(data []float32) []byte {
	buf := make([]byte, len(data)*4)
	for i, v := range data {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(v))
	}
	return buf
}

func float32SliceToFloat16Bytes(data []float32) []byte {
	buf := make([]byte, len(data)*2)
	for i, v := range data {
		binary.LittleEndian.PutUint16(buf[i*2:], float32ToFloat16Bits(v))
	}
	return buf
}

func bytesToFloat32Slice(buf []byte) []float32 {
	data := make([]float32, len(buf)/4)
	for i := range data {
		data[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[i*4:]))
	}
	return data
}

func bytesToFloat16AsFloat32Slice(buf []byte) []float32 {
	data := make([]float32, len(buf)/2)
	for i := range data {
		data[i] = float16BitsToFloat32(binary.LittleEndian.Uint16(buf[i*2:]))
	}
	return data
}

func float32ToFloat16Bits(f float32) uint16 {
	b := math.Float32bits(f)
	sign := uint16((b >> 16) & 0x8000)
	exp := int((b>>23)&0xFF) - 127 + 15
	mant := b & 0x7FFFFF

	if exp <= 0 {
		if exp < -10 {
			return sign
		}
		mant = (mant | 0x800000) >> uint32(1-exp)
		if (mant & 0x1000) != 0 {
			mant += 0x2000
		}
		return sign | uint16(mant>>13)
	}

	if exp >= 31 {
		if mant == 0 {
			return sign | 0x7C00
		}
		return sign | 0x7C00 | uint16(mant>>13)
	}

	if (mant & 0x1000) != 0 {
		mant += 0x2000
		if (mant & 0x800000) != 0 {
			mant = 0
			exp++
			if exp >= 31 {
				return sign | 0x7C00
			}
		}
	}

	return sign | uint16(exp<<10) | uint16(mant>>13)
}

func float16BitsToFloat32(h uint16) float32 {
	sign := uint32(h&0x8000) << 16
	exp := (h >> 10) & 0x1F
	mant := uint32(h & 0x03FF)

	if exp == 0 {
		if mant == 0 {
			return math.Float32frombits(sign)
		}
		// Normalize subnormal half to float32.
		exp32 := uint32(127 - 15 + 1)
		for (mant & 0x0400) == 0 {
			mant <<= 1
			exp32--
		}
		mant &= 0x03FF
		bits := sign | (exp32 << 23) | (mant << 13)
		return math.Float32frombits(bits)
	}

	if exp == 0x1F {
		bits := sign | 0x7F800000 | (mant << 13)
		return math.Float32frombits(bits)
	}

	exp32 := uint32(exp) - 15 + 127
	bits := sign | (exp32 << 23) | (mant << 13)
	return math.Float32frombits(bits)
}

func findJSONEnd(data []byte) int {
	depth := 0
	for i, b := range data {
		if b == '{' {
			depth++
		} else if b == '}' {
			depth--
			if depth == 0 {
				return i + 1
			}
		}
	}
	return 0
}
