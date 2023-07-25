// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"os"

	pb "beam.apache.org/learning/tour-of-beam/backend/playground_api/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func GetSnippet(ctx context.Context, snippetId string) (*pb.GetSnippetResponse, error) {
	routerHost := os.Getenv("PLAYGROUND_ROUTER_HOST")
	conn, err := grpc.DialContext(ctx, routerHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial grpc: %w", err)
	}
	client := pb.NewPlaygroundServiceClient(conn)

	req := &pb.GetSnippetRequest{Id: snippetId}
	resp, err := client.GetSnippet(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("get snippet: %w", err)
	}
	return resp, nil
}
