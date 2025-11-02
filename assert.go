package xgrpc

import (
	"context"

	"github.com/facebookincubator/go-belt/tool/logger"
)

func assert(ctx context.Context, condition bool, args ...any) {
	if !condition {
		logger.Panic(ctx, "assertion failed", args)
	}
}
