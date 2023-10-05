package cache

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/cache"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/logging"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/test/integration/test_framework"
	"github.com/google/uuid"
)

func TestRefresher_Refresh(t *testing.T) {
	type args struct {
		key      string
		size     uint64
		interval time.Duration
	}
	tests := []struct {
		name   string
		setter cache.UInt64Setter
		args   args
	}{
		{
			name:   "refresh 10/s",
			setter: test_framework.Cache,
			args: args{
				key:      uuid.NewString(),
				size:     10,
				interval: time.Second,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := 3
			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			errChan := make(chan error)
			buf := bytes.Buffer{}
			l := logging.New("cache_test", logging.WithWriter(&buf))
			refresher, err := cache.NewRefresher(ctx, test_framework.Cache, cache.WithLogger(l))
			if err != nil {
				t.Fatalf("fatal: NewRefresher(%+v) err %v", test_framework.Cache, err)
			}
			go func() {
				if err := refresher.Refresh(ctx, tt.args.key, tt.args.size, tt.args.interval); err != nil {
					errChan <- err
				}
			}()
			tick := time.Tick(tt.args.interval)
			for {
				select {
				case <-tick:
					n--
					if n <= 0 {
						return
					}
					got, err := test_framework.RedisClient.Get(ctx, tt.args.key).Uint64()
					if err != nil {
						t.Fatalf("Get(%s) err %v", tt.args.key, err)
					}
					if got != tt.args.size {
						t.Errorf("Refresh(key: %s, size: %v, interval: %s) = %v, want %v", tt.args.key, tt.args.size, tt.args.interval, got, tt.args.size)
					}
				case err := <-errChan:
					t.Fatalf("Refresh(key: %s, size: %v, interval: %s) error = %v", tt.args.key, tt.args.size, tt.args.interval, err)
				case <-ctx.Done():
					return
				}
			}
		})
	}
}
