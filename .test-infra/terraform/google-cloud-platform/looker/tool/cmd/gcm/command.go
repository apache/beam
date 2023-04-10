package gcm

import (
	"encoding"
	"fmt"

	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/cmd/common"
	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/internal/model"
	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/internal/prompt"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	Command = &cobra.Command{
		Use:   "gcm",
		Short: "Manage GCM key for AES-256 encryption",
	}

	createCmd = &cobra.Command{
		Use:     "create",
		Short:   "Create GCM key and store in a Kubernetes secret",
		PreRunE: preRunE,
		RunE:    createRunE,
	}

	requiredVars = []common.EnvironmentVariable{
		common.LookerNamespace,
		common.GcmKeySecretId,
		common.GcmKeySecretDataKey,
	}
)

func init() {
	common.AddPrintEnvironmentFlag(createCmd)
	common.AddDryRunFlag(createCmd)
	Command.AddCommand(createCmd)
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

func createRunE(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	if common.PrintEnvironmentFlag {
		return nil
	}
	gcm, err := model.NewGcmKey()
	if err != nil {
		return fmt.Errorf("error creating gcm key, error %w", err)
	}

	meta := metav1.ObjectMeta{
		Name:      common.GcmKeySecretId.Value(),
		Namespace: common.LookerNamespace.Value(),
	}

	confirmationMessage := prompt.ConfirmationMessage(common.SecretKind, meta)
	ok := prompt.YesNo(confirmationMessage)
	if !ok {
		return nil
	}

	if common.DryRunFlag {
		fmt.Println("dry run mode, not saving")
		return nil
	}

	data := map[string]encoding.BinaryMarshaler{
		common.GcmKeySecretDataKey.Value(): gcm,
	}

	secret, op, err := common.K8sClient.Secrets().CreateOrUpdateData(ctx, meta, data)
	if err != nil {
		return err
	}

	meta = secret.ObjectMeta
	fmt.Println(prompt.OperationMessage(common.SecretKind, op, meta))

	return nil
}
