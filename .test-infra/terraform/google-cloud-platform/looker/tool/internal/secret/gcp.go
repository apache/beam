package secret

import (
	"context"
	"encoding"
	"fmt"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"gopkg.in/yaml.v3"
)

const (
	latestVersion = "latest"
)

type Reader secretmanager.Client
type Writer secretmanager.Client

func secretVersionName(project, name, version string) string {
	return fmt.Sprintf("projects/%s/secrets/%s/versions/%s", project, name, version)
}

func (reader *Reader) ReadIntoLatestVersion(ctx context.Context, project string, name string, value interface{}) error {
	return reader.ReadInto(ctx, project, name, latestVersion, value)
}

func (reader *Reader) ReadInto(ctx context.Context, project string, name string, version string, value interface{}) error {
	client := (*secretmanager.Client)(reader)
	secretVersionPath := secretVersionName(project, name, version)
	req := &secretmanagerpb.AccessSecretVersionRequest{
		Name: secretVersionPath,
	}
	resp, err := client.AccessSecretVersion(ctx, req)
	if err != nil {
		return fmt.Errorf("error accessing secret: %s, error %w", req.Name, err)
	}
	if resp.Payload == nil {
		return fmt.Errorf("payload nil when accessing secret: %s", req.Name)
	}
	if err := yaml.Unmarshal(resp.Payload.Data, value); err != nil {
		return fmt.Errorf("error encoding payload into %T, error: %w", value, err)
	}
	return nil
}

func (w *Writer) Write(ctx context.Context, project string, name string, value encoding.BinaryMarshaler) error {
	client := (*secretmanager.Client)(w)
	b, err := value.MarshalBinary()
	if err != nil {
		return fmt.Errorf("error marshaling %T, error %w", value, err)
	}
	parent := fmt.Sprintf("projects/%s/secrets/%s", project, name)
	req := &secretmanagerpb.AddSecretVersionRequest{
		Parent: parent,
		Payload: &secretmanagerpb.SecretPayload{
			Data: b,
		},
	}

	if _, err = client.AddSecretVersion(ctx, req); err != nil {
		return fmt.Errorf("error adding new secret version to %s of %T, payload: %s, error %w", name, value, string(b), err)
	}

	return nil
}
