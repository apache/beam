package cmd

import (
	"fmt"
	"github.com/apache/beam/sdks/v2/go/cmd/wasmx/internal/environment"
	"github.com/apache/beam/sdks/v2/go/cmd/wasmx/internal/udf"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/credentials/insecure"
	"net/url"
)

const (
	addressFlagName = "address"
)

var (
	port    environment.Variable = "PORT"
	urn     string
	address string

	//TODO(damondouglas): only works with localhost non-ssl endpoints
	credentials = insecure.NewCredentials()

	registry    udf.Registry
	registryEnv environment.Variable = "REGISTRY_URL"
)

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

func urnArgs(_ *cobra.Command, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing URN")
	}
	urn = args[0]

	return nil
}
