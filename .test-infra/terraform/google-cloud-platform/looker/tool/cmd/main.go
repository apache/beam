package main

import (
	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/cmd/adminconfig"
	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/cmd/gcm"
	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/cmd/jars"
	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/cmd/sync"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "lookerctl",
		Short: "Utilities to help with Looker installations",
	}
)

func init() {
	rootCmd.AddCommand(adminconfig.Command, jars.Command, gcm.Command, sync.Command)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}
