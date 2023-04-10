package common

import (
	"fmt"

	"github.com/apache/beam/testinfra/terraform/googlecloudplatform/looker/internal/k8s"
	"github.com/spf13/cobra"
)

func K8sPreRunE(cmd *cobra.Command, _ []string) error {
	var err error
	ctx := cmd.Context()
	K8sClient, err = k8s.NewDefaultClient()
	if err != nil {
		return err
	}
	_, op, err := K8sClient.Namespace().GetOrCreate(ctx, LookerNamespace.Value())
	if err != nil {
		return err
	}
	if op == k8s.OperationCreate {
		fmt.Printf("created namespace/%s\n", LookerNamespace.Value())
	}
	return nil
}
