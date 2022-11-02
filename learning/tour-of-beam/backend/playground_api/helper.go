package playground

import (
	context "context"

	grpc "google.golang.org/grpc"
)

func GetMockClient() PlaygroundServiceClient {

	return &PlaygroundServiceClientMock{
		SaveSnippetFunc: func(ctx context.Context, in *SaveSnippetRequest, opts ...grpc.CallOption) (*SaveSnippetResponse, error) {
			return &SaveSnippetResponse{Id: "snippet_id_1"}, nil
		},
		GetSnippetFunc: func(ctx context.Context, in *GetSnippetRequest, opts ...grpc.CallOption) (*GetSnippetResponse, error) {
			return &GetSnippetResponse{
				Files: []*SnippetFile{
					{Name: "main.py", Content: "import sys; sys.exit(0)", IsMain: true},
				},
				Sdk:             Sdk_SDK_PYTHON,
				PipelineOptions: "some opts",
			}, nil
		},
	}
}
