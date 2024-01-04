//go:generate tinygo build -o add.wasm -target=wasi add.go
package main

//export ProcessElement
func ProcessElement(x uint32) uint32 {
	return x + x
}

// main is required for the `wasi` target, even if it isn't used.
// See https://wazero.io/languages/tinygo/#why-do-i-have-to-define-main
func main() {}
