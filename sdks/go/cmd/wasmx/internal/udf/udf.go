package udf

import (
	_ "embed"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

const (
	// TODO: Replace with proto const after https://github.com/apache/beam/pull/29898
	wasmUri = "beam:dofn:wasm:1.0"
)

//go:embed add/add.wasm
var AddWasm WasmFn

type WasmFn []byte

func (fn WasmFn) FunctionSpec() *pipeline_v1.FunctionSpec {
	return &pipeline_v1.FunctionSpec{
		Urn:     wasmUri,
		Payload: fn,
	}
}
