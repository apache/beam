package fnapi

// NOTE: install protoc as described on grpc.io before running go generate.

//go:generate protoc -I. -I../runnerapi -I../vendor/ beam_fn_api.proto --go_out=Mbeam_runner_api.proto=github.com/apache/beam/sdks/go/pkg/beam/runnerapi/org_apache_beam_runner_v1,plugins=grpc:org_apache_beam_fn_v1
