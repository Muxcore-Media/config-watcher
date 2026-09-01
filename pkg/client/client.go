package client

import (
	"context"
	"sync"

	"google.golang.org/grpc"

	"github.com/Muxcore-Media/core/pkg/contracts"
	configwatcherv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/configwatcher/v1"
)

// Client implements contracts.ConfigWatcher over ConfigWatcherService gRPC.
type Client struct {
	rpc configwatcherv1.ConfigWatcherServiceClient
}

// New returns a ConfigWatcher client backed by conn.
func New(conn grpc.ClientConnInterface) *Client {
	return &Client{rpc: configwatcherv1.NewConfigWatcherServiceClient(conn)}
}

var _ contracts.ConfigWatcher = (*Client)(nil)

// OnChange registers a handler that fires when modules matching capability
// are added or removed. Returns a cancel function to stop watching.
func (c *Client) OnChange(ctx context.Context, capability string, handler func()) (cancel func()) {
	watchCtx, cancelFn := context.WithCancel(ctx)
	var once sync.Once
	stop := func() {
		once.Do(cancelFn)
	}

	go func() {
		defer stop()
		stream, err := c.rpc.WatchChanges(watchCtx, &configwatcherv1.WatchChangesRequest{
			Capability: capability,
		})
		if err != nil {
			return
		}
		for {
			if _, err := stream.Recv(); err != nil {
				return
			}
			handler()
		}
	}()

	return stop
}
