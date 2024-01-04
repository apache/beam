package cmd

import (
	"github.com/spf13/cobra"
)

var (
	Root = &cobra.Command{
		Use:   "wasmx",
		Short: "wasmx manages and exposes wasm UDFs for execution within a Beam context",
	}
)

func init() {
	Root.AddCommand(serveCmd)
}
