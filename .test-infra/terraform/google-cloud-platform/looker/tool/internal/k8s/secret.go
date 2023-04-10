package k8s

import (
	"context"
	"encoding"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8s "sigs.k8s.io/controller-runtime/pkg/client"
)

type Secrets struct {
	internal k8s.Client
}

func (client *Client) Secrets() *Secrets {
	return &Secrets{
		internal: client.internal,
	}
}

func (client *Secrets) CreateOrUpdateData(ctx context.Context, meta metav1.ObjectMeta, kv map[string]encoding.BinaryMarshaler) (*corev1.Secret, Operation, error) {
	data := map[string][]byte{}
	for k, v := range kv {
		b, err := v.MarshalBinary()
		if err != nil {
			return nil, OperationError, err
		}
		data[k] = b
	}
	secret := &corev1.Secret{
		ObjectMeta: meta,
		Data:       data,
	}
	err := client.internal.Create(ctx, secret)
	if err == nil {
		return secret, OperationCreate, nil
	}
	if errors.IsAlreadyExists(err) {
		return client.update(ctx, secret)
	}
	return secret, OperationError, fmt.Errorf("error: could not create or update secrets/%s in namespace: %s, error %w", secret.Name, secret.Namespace, err)
}

func (client *Secrets) ReadInto(ctx context.Context, meta metav1.ObjectMeta, key string, value encoding.BinaryUnmarshaler) error {
	secret := &corev1.Secret{
		ObjectMeta: meta,
	}
	objectKey := k8s.ObjectKeyFromObject(secret)
	if err := client.internal.Get(ctx, objectKey, secret); err != nil {
		return err
	}
	if secret.Data == nil {
		return fmt.Errorf("data nil from acquired secret/%s in namespace %s", meta.Name, meta.Namespace)
	}
	b, ok := secret.Data[key]
	if !ok {
		return fmt.Errorf("key %s does not exist in data from acquired secret/%s in namespace %s", key, meta.Name, meta.Namespace)
	}
	if err := value.UnmarshalBinary(b); err != nil {
		return err
	}
	return nil
}

func (client *Secrets) update(ctx context.Context, secret *corev1.Secret) (*corev1.Secret, Operation, error) {
	if err := client.internal.Update(ctx, secret); err != nil {
		return nil, OperationError, err
	}
	return secret, OperationUpdate, nil
}
