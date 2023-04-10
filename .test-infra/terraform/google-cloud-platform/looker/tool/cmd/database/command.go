package database

import (
	"encoding"
	"fmt"

	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/cmd/common"
	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/internal/model"
	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/internal/prompt"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
)

var (
	Command = &cobra.Command{
		Use:   "database",
		Short: "Manage Looker database resources",
	}

	credentialsCmd = &cobra.Command{
		Use:   "credentials",
		Short: "Manager Looker database credentials",
	}

	credentialsCreateCmd = &cobra.Command{
		Use:     "create",
		Short:   "Create Looker database credentials and save in a Kubernetes secret",
		PreRunE: preRunE,
		RunE:    credentialsCreateRunE,
	}

	requiredVars = []common.EnvironmentVariable{
		common.LookerNamespace,
		common.LookerDatabaseCredentialsSecretId,
		common.LookerDatabaseCredentialsDataKey,
	}
)

func init() {
	common.AddPrintEnvironmentFlag(credentialsCreateCmd)
	common.AddDryRunFlag(credentialsCreateCmd)
	credentialsCmd.AddCommand(credentialsCreateCmd)
	Command.AddCommand(credentialsCmd)
}

func preRunE(cmd *cobra.Command, args []string) error {
	if common.PrintEnvironmentFlag {
		return common.PrintEnvironment(nil, requiredVars)
	}
	if err := common.Missing(requiredVars[0], requiredVars[1:]...); err != nil {
		return err
	}
	return common.K8sPreRunE(cmd, args)
}

func credentialsCreateRunE(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	if common.PrintEnvironmentFlag {
		return nil
	}
	meta := metav1.ObjectMeta{
		Name:      common.LookerDatabaseCredentialsSecretId.Value(),
		Namespace: common.LookerNamespace.Value(),
	}
	confirmationMessage := prompt.ConfirmationMessage(common.SecretKind, meta)
	credentials, err := prompt.DatabaseCredentials(signals.SetupSignalHandler(), nil, confirmationMessage)
	if prompt.IsCancelErr(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if common.DryRunFlag {
		fmt.Println("dry run mode, not saving")
		return nil
	}

	secret, op, err := common.K8sClient.Secrets().CreateOrUpdateData(ctx, meta, map[string]encoding.BinaryMarshaler{
		common.LookerDatabaseCredentialsDataKey.Value(): credentials,
	})
	if err != nil {
		return err
	}

	meta = secret.ObjectMeta
	fmt.Println(prompt.OperationMessage(common.SecretKind, op, meta))

	meta = metav1.ObjectMeta{
		Name:      common.BitamiMySqlCredentialsSecretId.Value(),
		Namespace: common.LookerNamespace.Value(),
	}

	data := map[string]encoding.BinaryMarshaler{
		common.BitamiRootPasswordDataKey.Value():        model.CredentialsString(credentials.Password),
		common.BitamiReplicationPasswordDataKey.Value(): model.CredentialsString(credentials.Password),
		common.BitamiMySqlPasswordDataKey.Value():       model.CredentialsString(credentials.Password),
	}

	secret, op, err = common.K8sClient.Secrets().CreateOrUpdateData(ctx, meta, data)
	if err != nil {
		return err
	}

	meta = secret.ObjectMeta
	fmt.Println(prompt.OperationMessage(common.SecretKind, op, meta))

	return nil
}
