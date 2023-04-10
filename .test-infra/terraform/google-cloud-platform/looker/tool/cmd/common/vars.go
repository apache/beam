package common

import (
	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/internal/k8s"
	"github.com/spf13/cobra"
)

const (
	SecretKind = "secret"
)

var (
	DryRunFlag           bool
	PrintEnvironmentFlag bool
	K8sClient            *k8s.Client
)

func AddPrintEnvironmentFlag(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&PrintEnvironmentFlag, "env", false, "Print environment variables")
}

func AddDryRunFlag(cmd *cobra.Command) {
	cmd.Flags().BoolVarP(&DryRunFlag, "dry-run", "d", false, "Run command in dry run mode")
}
