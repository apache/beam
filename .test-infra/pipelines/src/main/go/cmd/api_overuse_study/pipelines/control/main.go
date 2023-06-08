package main

import (
	"context"
	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/logging"
	quotav1 "github.com/apache/beam/test-infra/pipelines/src/main/go/internal/proto/quota/v1"
	uuid2 "github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/apimachinery/pkg/util/json"
	"os"
)

var (
	logger = logging.MustLogger(context.Background(), "pipelines")
)

func main() {
	if err := run(context.Background()); err != nil {
		logger.Error(context.Background(), err.Error())
	}
}

func run(ctx context.Context) error {
	conn, err := grpc.Dial("127.0.0.1:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := quotav1.NewQuotaServiceClient(conn)
	id := uuid2.New().String()
	resp, err := client.Create(ctx, &quotav1.CreateQuotaRequest{
		Quota: &quotav1.Quota{
			Id:                          id,
			Size:                        10,
			RefreshMillisecondsInterval: 10000,
		},
	})
	if err != nil {
		return err
	}

	return json.NewEncoder(os.Stdout).Encode(resp.Quota)
}
