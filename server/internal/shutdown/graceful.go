// =============================================================================
// internal/shutdown/graceful.go
// =============================================================================

package shutdown

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func WithSignal(parent context.Context, logger *slog.Logger) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case sig := <-ch:
			logger.Info("shutting down", "signal", sig)
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}
