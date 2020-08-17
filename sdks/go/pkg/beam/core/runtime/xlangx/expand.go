package xlangx

import (
	"context"

	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	jobpb "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
	"google.golang.org/grpc"
)

func Expand(ctx context.Context, req *jobpb.ExpansionRequest, expansionAddr string) (*jobpb.ExpansionResponse, error) {
	// Querying Expansion Service

	// Setting grpc client
	conn, err := grpc.Dial(expansionAddr, grpc.WithInsecure())
	if err != nil {
		return nil, errors.Wrapf(err, "unable to connect to expansion service at %v", expansionAddr)
	}
	defer conn.Close()
	client := jobpb.NewExpansionServiceClient(conn)

	// Handling ExpansionResponse
	res, err := client.Expand(ctx, req)
	if err != nil {
		return nil, errors.Wrapf(err, "expansion failed")
	}
	return res, nil
}
