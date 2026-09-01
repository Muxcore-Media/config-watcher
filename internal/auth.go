package internal

import (
	"context"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Muxcore-Media/config-watcher/internal/adminv1"
)

const callerIDMetadataKey = "x-caller-id"

func moduleTokenFromEnv() string {
	for _, k := range []string{"CONFIG_WATCHER_MODULE_TOKEN", "MUXCORE_MODULE_TOKEN"} {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return ""
}

func authUnaryInterceptor(moduleToken string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod != adminv1.ConfigWatcherAdminService_ReportModuleChange_FullMethodName {
			return handler(ctx, req)
		}
		if err := authorizeReportModuleChange(ctx, moduleToken); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func authorizeReportModuleChange(ctx context.Context, moduleToken string) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "mesh identity or module token required")
	}
	if ids := md.Get(callerIDMetadataKey); len(ids) > 0 {
		id := strings.TrimSpace(ids[0])
		if id != "" && id != "_public" {
			return nil
		}
	}
	if moduleToken != "" {
		if vals := md.Get("authorization"); len(vals) > 0 {
			token := bearerToken(vals[0])
			if token != "" && token == moduleToken {
				return nil
			}
		}
	}
	return status.Error(codes.Unauthenticated, "mesh identity or module token required")
}

func bearerToken(header string) string {
	const prefix = "Bearer "
	header = strings.TrimSpace(header)
	if len(header) > len(prefix) && strings.EqualFold(header[:len(prefix)], prefix) {
		return strings.TrimSpace(header[len(prefix):])
	}
	return header
}
