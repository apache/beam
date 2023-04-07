package common

import (
	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/internal/secret"
	"github.com/spf13/cobra"
)

const (
	lookerNamespace = "looker"
)

var (
	PrintEnvironmentFlag bool
	SecretReader         *secret.Reader
	SecretWriter         *secret.Writer
)

func PreRun(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return err
	}
	SecretReader = (*secret.Reader)(client)
	SecretWriter = (*secret.Writer)(client)
	return nil
}
