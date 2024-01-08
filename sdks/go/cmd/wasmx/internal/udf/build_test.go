package udf

import (
	"bytes"
	"context"
	"encoding/binary"
	extism "github.com/extism/go-sdk"
	"github.com/google/go-cmp/cmp"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func Test_goBuilder_setupDir(t *testing.T) {
	bldr := goBuilder()
	defer bldr.Teardown(context.Background())
	if err := bldr.setupDir(); err != nil {
		t.Fatal(err)
	}
	got := filepath.Dir(bldr.tempDir) + "/"
	if got != os.TempDir() {
		t.Errorf("setupDir() = %s, want %s", got, os.TempDir())
	}
}

func Test_goBuilder_setupSrcFile(t *testing.T) {
	bldr := goBuilder()
	defer bldr.Teardown(context.Background())
	if err := bldr.setupDir(); err != nil {
		t.Fatal(err)
	}
	if err := bldr.setupSrcFile(); err != nil {
		t.Fatal(err)
	}
	got := bldr.codeSrcPath
	want := filepath.Join(bldr.tempDir, goSrcName)
	if got != want {
		t.Errorf("setupSrcFile() codeSrcPath = %s, want %s", got, want)
	}

	f, err := os.Open(bldr.codeSrcPath)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), tinyGoGenerate) {
		t.Errorf("setupSrcFile() header =\n%s\nwant: %s", string(b), tinyGoGenerate)
	}
}

func Test_goBuilder_Setup(t *testing.T) {
	ctx := context.Background()
	bldr := goBuilder()
	defer bldr.Teardown(context.Background())
	if err := bldr.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(bldr.tempDir)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]struct{}{
		goSrcName: {},
		"go.mod":  {},
		"go.sum":  {},
	}
	got := map[string]struct{}{}
	for _, f := range entries {
		got[f.Name()] = struct{}{}
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Setup() mismatch (-want +got):\n%s", diff)
	}
}

func Test_goBuilder_Build(t *testing.T) {
	ctx := context.Background()
	bldr := goBuilder()
	defer bldr.Teardown(context.Background())
	if err := bldr.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	src := bytes.NewBufferString(goSrc)
	dst := bytes.Buffer{}
	if err := bldr.Build(ctx, &dst, src); err != nil {
		t.Fatal(err)
	}

	manifest := extism.Manifest{
		Wasm: []extism.Wasm{
			extism.WasmData{
				Data: dst.Bytes(),
				Name: wasmDstName,
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

const goSrc = `
package main

import (
	"encoding/binary"
	"github.com/extism/go-pdk"
)

//export ProcessElement
func ProcessElement() uint32 {
	out := make([]byte, 8)
	data := pdk.Input()
	element := binary.LittleEndian.Uint64(data)
	binary.LittleEndian.PutUint64(out, element+element)
	pdk.Output(out)

	return 0
}

func main() {}
`
