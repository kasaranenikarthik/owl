// =============================================================================
// internal/similarity/hash.go
// =============================================================================

package similarity

import (
	"image"

	"gocv.io/x/gocv"
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
	// Using gocv for fast JPEG decode + resize
	mat, err := gocvIMDecode(jpegData)
	if err != nil {
		return nil, err
	}
	return mat, nil
}

// gocvIMDecode wraps gocv operations.
func gocvIMDecode(data []byte) ([]uint8, error) {
	// Import is in the build file — here's the logic:
	mat, err := gocv.IMDecode(data, gocv.IMReadGrayScale)
	if err != nil {
		return nil, err
	}
	resized := gocv.NewMat()
	gocv.Resize(mat, &resized, image.Pt(8, 8), 0, 0, gocv.InterpolationLinear)
	pixels := make([]uint8, 64)
	for y := 0; y < 8; y++ {
		for x := 0; x < 8; x++ {
			pixels[y*8+x] = resized.GetUCharAt(y, x)
		}
	}
	return pixels, nil

}
