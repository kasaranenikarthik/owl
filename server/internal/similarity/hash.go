// =============================================================================
// internal/similarity/hash.go
// =============================================================================

package similarity

import (
	"bytes"
	"fmt"
	"image/jpeg"
)

// AverageHash computes a 64-bit perceptual hash of a JPEG image.
// Resize to 8x8 grayscale, compare each pixel to the mean.
// Cost: ~0.1ms per frame — negligible compared to inference.
func AverageHash(jpegData []byte) (uint64, error) {
	// Decode JPEG to grayscale 8x8
	img, err := decodeAndResize(jpegData, 8, 8)
	if err != nil {
		return 0, err
	}

	// Compute mean pixel value
	var sum float64
	for _, v := range img {
		sum += float64(v)
	}
	mean := sum / 64.0

	// Build hash: 1 bit per pixel (above/below mean)
	var hash uint64
	for i, v := range img {
		if float64(v) >= mean {
			hash |= 1 << uint(63-i)
		}
	}
	return hash, nil
}

// HammingDistance counts differing bits between two hashes.
// Returns 0 for identical images, 64 for completely different.
func HammingDistance(a, b uint64) int {
	xor := a ^ b
	count := 0
	for xor != 0 {
		count++
		xor &= xor - 1 // clear lowest set bit
	}
	return count
}

// decodeAndResize converts JPEG bytes to an 8x8 grayscale pixel array.
func decodeAndResize(jpegData []byte, w, h int) ([]uint8, error) {
	img, err := jpeg.Decode(bytes.NewReader(jpegData))
	if err != nil {
		return nil, fmt.Errorf("decode jpeg: %w", err)
	}

	b := img.Bounds()
	if b.Dx() == 0 || b.Dy() == 0 {
		return nil, fmt.Errorf("invalid image dimensions")
	}

	pixels := make([]uint8, w*h)
	for y := 0; y < h; y++ {
		sy := b.Min.Y + ((y*2+1)*b.Dy())/(2*h)
		for x := 0; x < w; x++ {
			sx := b.Min.X + ((x*2+1)*b.Dx())/(2*w)
			r, g, b, _ := img.At(sx, sy).RGBA()
			gray := (299*r + 587*g + 114*b) / 1000
			pixels[y*w+x] = uint8(gray >> 8)
		}
	}

	return pixels, nil
}
