package udf

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/protox"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

const (
	// TODO: Replace with proto const after https://github.com/apache/beam/pull/29898
	wasmUrn  = "beam:dofn:wasm:1.0"
	addFnUrn = "beam:transform:add:1.0"
)

var (
	RegistryInMemory Registry = inMemoryRegistry{
		addFnUrn: AddWasm,
	}
)

//go:embed add/add.wasm
var AddWasm WasmFn

type WasmFn []byte

func (fn WasmFn) FunctionSpec() *pipeline_v1.FunctionSpec {
	pardo := &pipeline_v1.ParDoPayload{
		DoFn: &pipeline_v1.FunctionSpec{
			Urn:     wasmUrn,
			Payload: fn,
		},
	}
	return &pipeline_v1.FunctionSpec{
		Urn:     graphx.URNParDo,
		Payload: protox.MustEncode(pardo),
	}
}

type Registry interface {
	Get(ctx context.Context, urn string) (WasmFn, error)
}

type inMemoryRegistry map[string]WasmFn

func (reg inMemoryRegistry) Get(_ context.Context, urn string) (WasmFn, error) {
	if fn, ok := reg[urn]; ok {
		return fn, nil
	}
	return nil, fmt.Errorf("%s is not registered with this service", urn)
}
