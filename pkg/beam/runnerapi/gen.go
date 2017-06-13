package fnapi

// NOTE: install protoc as described on grpc.io before running go generate.

//go:generate protoc -I. -I../vendor/ beam_runner_api.proto --go_out=org_apache_beam_runner_v1
