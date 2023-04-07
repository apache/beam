package gcm

import (
	"fmt"

	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/cmd/common"
	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/internal/secret"
	"github.com/spf13/cobra"
)

var (
	Command = &cobra.Command{
		Use:   "gcm",
		Short: "Manage GCM key for AES-256 encryption",
	}

	createCmd = &cobra.Command{
		Use:     "create",
		Short:   "Create GCM key and store in Google Secret Manager",
		PreRunE: preRunE,
		RunE:    createRunE,
	}

	requiredVars = []common.EnvironmentVariable{
		common.ProjectId,
		common.GcmSecretId,
	}
)

func init() {
	common.AddPrintEnvironmentFlag(createCmd)
	Command.AddCommand(createCmd)
}

func preRunE(cmd *cobra.Command, args []string) error {
	if common.PrintEnvironmentFlag {
		return common.PrintEnvironment(nil, requiredVars)
	}
	if err := common.Missing(requiredVars[0], requiredVars[1:]...); err != nil {
		return err
	}
	return common.PreRun(cmd, args)
}

func createRunE(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	if common.PrintEnvironmentFlag {
		return nil
	}
	gcm, err := secret.NewGcmKey()
	if err != nil {
		return fmt.Errorf("error creating gcm key, error %w", err)
	}

	if err := common.SecretWriter.Write(ctx, common.ProjectId.Value(), common.GcmSecretId.Value(), gcm); err != nil {
		return err
	}

	fmt.Printf("created %s in project %s\n", common.GcmSecretId.Value(), common.ProjectId.Value())

	return nil
}
