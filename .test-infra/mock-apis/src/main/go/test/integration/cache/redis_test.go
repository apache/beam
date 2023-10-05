package cache

import (
	"context"
	"testing"
	"time"

	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/cache"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/test/integration/test_framework"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestRedisCache_Alive(t *testing.T) {
	tests := []struct {
		name    string
		client  *cache.RedisCache
		wantErr bool
	}{
		{
			name: "wrong address",
			client: (*cache.RedisCache)(redis.NewClient(&redis.Options{
				Addr: "badaddress:5555",
			})),
			wantErr: true,
		},
		{
			name:    "correct address",
			client:  test_framework.Cache,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()
			if err := tt.client.Alive(ctx); (err != nil) != tt.wantErr {
				t.Errorf("Alive() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRedisCache_Decrement(t *testing.T) {
	type args struct {
		setupKey   string
		setupValue uint64
		decrKey    string
	}
	tests := []struct {
		name    string
		args    args
		want    int64
		wantErr bool
	}{
		{
			name: "previous=100",
			args: args{
				setupKey:   uuid.NewString(),
				setupValue: 100,
			},
			want:    99,
			wantErr: false,
		},
		{
			name: "setupKey does not exist",
			args: args{
				setupKey:   uuid.NewString(),
				setupValue: 1,
				decrKey:    uuid.NewString(),
			},
			want:    -1,
			wantErr: true,
		},
		{
			name: "previous=0",
			args: args{
				setupKey:   uuid.NewString(),
				setupValue: 0,
			},
			want:    -1,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if err := test_framework.RedisClient.Set(ctx, tt.args.setupKey, tt.args.setupValue, 0).Err(); err != nil {
				t.Fatalf("fatal: Set(%s, %v, 0) err %v", tt.args.setupKey, tt.args.setupValue, err)
			}
			defer func() {
				if err := test_framework.RedisClient.Del(ctx, tt.args.setupKey).Err(); err != nil {
					t.Fatalf("fatal: Del(%s) err %v", tt.args.setupKey, err)
				}
			}()
			var key string
			if tt.args.decrKey == "" {
				key = tt.args.setupKey
			}
			got, err := test_framework.Cache.Decrement(ctx, key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Decrement() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Decrement() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRedisCache_Set(t *testing.T) {
	type args struct {
		key      string
		setValue uint64
		expiry   time.Duration
	}
	tests := []struct {
		name    string
		args    args
		want    uint64
		wantErr bool
	}{
		{
			name: "set value as normal",
			args: args{
				key:      uuid.NewString(),
				setValue: 100,
				expiry:   time.Minute,
			},
			want:    100,
			wantErr: false,
		},
		{
			name: "value expired after setting",
			args: args{
				key:      uuid.NewString(),
				setValue: 100,
				expiry:   time.Millisecond,
			},
			want:    0,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if err := test_framework.Cache.Set(ctx, tt.args.key, tt.args.setValue, tt.args.expiry); (err != nil) != tt.wantErr {
				t.Errorf("Set() error = %v, wantErr %v", err, tt.wantErr)
			}
			got, err := test_framework.RedisClient.Get(ctx, tt.args.key).Uint64()
			if err != nil {
				t.Fatalf("fatal: Get(%s) err %v", tt.args.key, err)
			}
			if got != tt.args.setValue {
				t.Errorf("Set(%s) = %v, want %v", tt.args.key, got, tt.want)
			}
		})
	}
}
