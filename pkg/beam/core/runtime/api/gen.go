package api

// NOTE: install protoc as described on grpc.io before running go generate.

//go:generate protoc -I. beam_runner_api.proto endpoints.proto --go_out=pipeline_v1
//go:generate protoc -I. beam_fn_api.proto --go_out=Mbeam_runner_api.proto=github.com/apache/beam/sdks/go/pkg/beam/core/runtime/api/pipeline_v1,Mendpoints.proto=github.com/apache/beam/sdks/go/pkg/beam/core/runtime/api/pipeline_v1,plugins=grpc:fnexecution_v1
