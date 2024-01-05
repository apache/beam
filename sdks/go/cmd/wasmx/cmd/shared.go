package cmd

import (
	"fmt"
	"github.com/apache/beam/sdks/v2/go/cmd/wasmx/internal/environment"
	"github.com/apache/beam/sdks/v2/go/cmd/wasmx/internal/udf"
	"github.com/spf13/cobra"
	"net/url"
	"os"
	"path/filepath"
)

var (
	port    environment.Variable = "PORT"
	address string

	registry    udf.Registry
	registryEnv environment.Variable = "REGISTRY_URL"
)

func init() {
	configDir, err := os.UserConfigDir()
	if err != nil {
		panic(err)
	}
	path, err := filepath.Abs(configDir)
	if err != nil {
		panic(err)
	}
	path = filepath.Join(path, "wasmx")
	registryEnv.MustDefault(fmt.Sprintf("file://%s", path))
}

func servePreE(cmd *cobra.Command, _ []string) error {
	if err := environment.Missing(port, registryEnv); err != nil {
		return err
	}
	p, err := port.Int()
	if err != nil {
		return err
	}

	address = fmt.Sprintf(":%v", p)

	location, err := url.Parse(registryEnv.Value())
	if err != nil {
		return err
	}

	registry, err = udf.NewRegistry(cmd.Context(), location)
	return err
}
