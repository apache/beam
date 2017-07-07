package harness

//go:generate protoc -I. -I../api -I../../../vendor/ session.proto --go_out=Mbeam_fn_api.proto=github.com/apache/beam/sdks/go/pkg/beam/core/runtime/api/org_apache_beam_fn_v1:session
