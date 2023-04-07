package sync

import (
	"fmt"
	"strings"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/cmd/common"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	k8s "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

var (
	namespace = "looker"
	Command   = &cobra.Command{
		Use:   "sync",
		Short: "Synchronize between kubernetes and Google Secret Manager secrets",
	}
	gcpTok8sCmd = &cobra.Command{
		Use:     "gcp2k8s SECRET_MANAGER_ID KUBERNETES_SECRET<name/key>",
		Short:   "Copy a secret from Google Secret Manager to kubernetes secret",
		Args:    cobra.MaximumNArgs(2),
		PreRunE: preRunE,
		RunE:    gcp2k8sRunE,
	}

	requiredVars = []common.EnvironmentVariable{
		common.ProjectId,
	}
)

func init() {
	Command.AddCommand(gcpTok8sCmd)
	common.AddPrintEnvironmentFlag(gcpTok8sCmd)
	Command.PersistentFlags().StringVarP(&namespace, "namespace", "n", namespace, "Kubernetes namespace")
}

func preRunE(_ *cobra.Command, _ []string) error {
	if common.PrintEnvironmentFlag {
		return common.PrintEnvironment(nil, requiredVars)
	}
	return common.Missing(requiredVars[0], requiredVars[1:]...)
}

func gcp2k8sRunE(cmd *cobra.Command, args []string) error {
	if common.PrintEnvironmentFlag {
		return nil
	}
	if len(args) < 2 {
		return fmt.Errorf("missing positional args")
	}
	ctx := cmd.Context()
	secretManagerSecretId := args[0]
	gcpSmClient, err := secretmanager.NewClient(ctx)
	if err != nil {
		return err
	}
	secretName := fmt.Sprintf("projects/%s/secrets/%s/versions/latest", common.ProjectId.Value(), secretManagerSecretId)
	resp, err := gcpSmClient.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{
		Name: secretName,
	})
	if err != nil {
		return fmt.Errorf("error access secret: %s, error %w", secretName, err)
	}
	if resp.Payload == nil || len(resp.Payload.Data) == 0 {
		return fmt.Errorf("payload empty for secret: %s", secretName)
	}
	split := strings.Split(args[1], "/")
	if len(split) < 2 {
		return fmt.Errorf("KUBERNETES_SECRET not in expected format <name>/<key>, got: %s", args[1])
	}
	k8sSecretName, k8sSecretKey := split[0], split[1]
	k8sClient, err := k8s.New(config.GetConfigOrDie(), k8s.Options{})
	if err != nil {
		return err
	}
	secret := &corev1.Secret{
		ObjectMeta: v1.ObjectMeta{
			Name:      k8sSecretName,
			Namespace: namespace,
		},
		Data: map[string][]byte{
			k8sSecretKey: resp.Payload.Data,
		},
	}
	if err := k8sClient.Create(ctx, secret); err != nil {
		return err
	}
	fmt.Printf("secret/%s created\n", secret.Name)
	return nil
}
