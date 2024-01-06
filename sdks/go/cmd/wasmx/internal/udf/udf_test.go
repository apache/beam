package udf

import (
	"context"
	"encoding/binary"
	extism "github.com/extism/go-sdk"
	"testing"
)

func TestWasmFn_wordcount(t *testing.T) {
	ctx := context.Background()
	manifest := extism.Manifest{
		Wasm: []extism.Wasm{
			extism.WasmData{
				Data: wordCountWasm,
				Name: "wordcount.wasm",
			},
		},
	}
	config := extism.PluginConfig{
		EnableWasi: true,
	}
	plugin, err := extism.NewPlugin(ctx, manifest, config, []extism.HostFunction{})
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("The play is the thing to catch the conscience of the King")
	_, out, err := plugin.Call("ProcessElement", data)
	if err != nil {
		t.Fatal(err)
	}
	var want uint32 = 12
	got := binary.LittleEndian.Uint32(out)
	if got != want {
		t.Errorf("ProcessElement() = %v, want %v", got, want)
	}
}
