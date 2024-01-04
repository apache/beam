package main

import (
	"github.com/apache/beam/sdks/v2/go/cmd/wasmx/cmd"
)

func main() {
	if err := cmd.Root.Execute(); err != nil {
		panic(err)
	}
}
