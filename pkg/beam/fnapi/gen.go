package fnapi

// NOTE: install protoc as described on grpc.io before running go generate.

//go:generate protoc -I. -I../vendor/ beam_fn_api.proto --go_out=plugins=grpc:org_apache_beam_fn_v1
