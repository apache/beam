package k8s

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8s "sigs.k8s.io/controller-runtime/pkg/client"
)

type Namespace struct {
	internal k8s.Client
}

func (client *Client) Namespace() *Namespace {
	return &Namespace{
		internal: client.internal,
	}
}

func (client *Namespace) GetOrCreate(ctx context.Context, namespace string) (*corev1.Namespace, Operation, error) {
	result := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
		},
	}
	err := client.internal.Create(ctx, result)
	if errors.IsAlreadyExists(err) {
		return result, OperationNoOp, nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("error creating namespace/%s, error %w", namespace, err)
	}
	return result, OperationCreate, nil
}
