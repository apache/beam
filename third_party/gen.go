package third_party

// NOTE: install protoc as described on grpc.io before running go generate.

//go:generate protoc -I beam/ beam/beam_fn_api.proto --go_out=plugins=grpc:beam/org_apache_beam_fn_v1
