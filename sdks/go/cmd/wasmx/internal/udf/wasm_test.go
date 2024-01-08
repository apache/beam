package udf

import (
	"context"
	"encoding/binary"
	extism "github.com/extism/go-sdk"
	"github.com/google/go-cmp/cmp"
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
	var want uint64 = 12
	got := binary.LittleEndian.Uint64(out)
	if got != want {
		t.Errorf("ProcessElement() = %v, want %v", got, want)
	}
}

func TestWasmFn_add(t *testing.T) {
	ctx := context.Background()
	manifest := extism.Manifest{
		Wasm: []extism.Wasm{
			extism.WasmData{
				Data: addWasm,
				Name: "add.wasm",
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

	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, 2)
	_, out, err := plugin.Call("ProcessElement", data)
	if err != nil {
		t.Fatal(err)
	}
	var want uint64 = 4
	got := binary.LittleEndian.Uint64(out)
	if got != want {
		t.Errorf("ProcessElement() = %v, want %v", got, want)
	}
}

func TestWasmSource_Parse(t *testing.T) {
	tests := []struct {
		name string
		src  WasmSource
		want *WasmSourceMetadata
	}{
		{
			name: "add.go",
			src:  addSrc,
			want: &WasmSourceMetadata{
				BeamInput:  "beam:coder:varint:v1",
				BeamOutput: "beam:coder:varint:v1",
				Exports: []string{
					"ProcessElement",
				},
			},
		},
		{
			name: "wordcount.go",
			src:  wordCountSrc,
			want: &WasmSourceMetadata{
				BeamInput:  "beam:coder:string_utf8:v1",
				BeamOutput: "beam:coder:varint:v1",
				Exports: []string{
					"ProcessElement",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.src)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("Parse() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
