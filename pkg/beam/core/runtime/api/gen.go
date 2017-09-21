package api

// NOTE: install protoc as described on grpc.io before running go generate.

//go:generate protoc -I. -I../../../vendor/ beam_fn_api.proto --go_out=Mbeam_runner_api.proto=github.com/apache/beam/sdks/go/pkg/beam/core/runtime/api/org_apache_beam_runner_v1,plugins=grpc:org_apache_beam_fn_v1
//go:generate protoc -I. -I../../../vendor/ beam_runner_api.proto --go_out=org_apache_beam_runner_api_v1
