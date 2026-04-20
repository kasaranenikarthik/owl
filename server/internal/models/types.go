// =============================================================================
// internal/models/types.go
// =============================================================================

package models

import "time"

// FrameMessage is the in-memory frame representation used by the inference bridge.
type FrameMessage struct {
	ClientID  string    `json:"client_id"`
	FrameID   uint64    `json:"frame_id"` // gateway-assigned, monotonically increasing per client
	Timestamp time.Time `json:"timestamp"`
	Data      []byte    `json:"data"`
}

// BoundingBox is a single detected object.
type BoundingBox struct {
	X1         float32 `json:"x1"`
	Y1         float32 `json:"y1"`
	X2         float32 `json:"x2"`
	Y2         float32 `json:"y2"`
	Confidence float32 `json:"confidence"`
	ClassID    int     `json:"class_id"`
	ClassName  string  `json:"class_name"`
}

// Detection holds one object.
type Detection struct {
	BoundingBox BoundingBox `json:"bounding_box"`
}

// InternalResult is the Kafka message from inference → gateway.
type InternalResult struct {
	ClientID    string      `json:"client_id"`
	FrameID     uint64      `json:"frame_id"`
	Detections  []Detection `json:"detections"`
	InferenceMs float64     `json:"inference_ms"`
	Timestamp   time.Time   `json:"timestamp"`
}

// ClientResult is what the client receives over WebSocket.
type ClientResult struct {
	FrameID     uint64      `json:"frame_id"`
	Detections  []Detection `json:"detections"`
	InferenceMs float64     `json:"inference_ms"`
	FromCache   bool        `json:"from_cache"`
	Timestamp   time.Time   `json:"timestamp"`
}
