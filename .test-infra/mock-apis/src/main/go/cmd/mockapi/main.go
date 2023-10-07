package main

import (
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/cmd/mockapi/echo"
	"github.com/spf13/cobra"
)

var (
	cmd = &cobra.Command{
		Use:   "mockapi",
		Short: "mockapi",
	}
)

func init() {
	cmd.AddCommand(echo.Command)
}

func main() {
	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}
