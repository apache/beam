package dataflow

import (
	"context"
	"fmt"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/storage/v1"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
)

// NewStorageClient creates a new GCS client with default application credentials.
func NewStorageClient(ctx context.Context) (*storage.Service, error) {
	cl, err := google.DefaultClient(ctx, storage.DevstorageFullControlScope)
	if err != nil {
		return nil, err
	}
	return storage.New(cl)
}

// Upload writes the given content to GCS. If the specified bucket does not
// exist, it is created first. Returns the full path of the object.
func Upload(client *storage.Service, project, bucket, object string, r io.Reader) (string, error) {
	exists, err := BucketExists(client, bucket)
	if err != nil {
		return "", err
	}
	if !exists {
		if err = CreateBucket(client, project, bucket); err != nil {
			return "", err
		}
	}

	if err := WriteObject(client, bucket, object, r); err != nil {
		return "", err
	}
	return fmt.Sprintf("gs://%s/%s", bucket, object), nil
}

// CreateBucket creates a bucket in GCS.
func CreateBucket(client *storage.Service, project, bucket string) error {
	b := &storage.Bucket{
		Name: bucket,
	}
	_, err := client.Buckets.Insert(project, b).Do()
	return err
}

// BucketExists returns true iff the given bucket exists.
func BucketExists(client *storage.Service, bucket string) (bool, error) {
	_, err := client.Buckets.Get(bucket).Do()
	if e, ok := err.(*googleapi.Error); ok && e.Code == http.StatusNotFound {
		return false, nil
	}
	return err == nil, err
}

// WriteObject writes the given content to the specified
// object. If the object already exist, it is overwritten.
func WriteObject(client *storage.Service, bucket, object string, r io.Reader) error {
	obj := &storage.Object{
		Name:   object,
		Bucket: bucket,
	}
	_, err := client.Objects.Insert(bucket, obj).Media(r).Do()
	return err
}

// ReadObject reads the content of the given object in full.
func ReadObject(client *storage.Service, bucket, object string) ([]byte, error) {
	resp, err := client.Objects.Get(bucket, object).Download()
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(resp.Body)
}

// ObjectExists returns true iff the given object exists.
func ObjectExists(client *storage.Service, bucket, object string) (bool, error) {
	_, err := client.Objects.Get(bucket, object).Do()
	if e, ok := err.(*googleapi.Error); ok && e.Code == http.StatusNotFound {
		return false, nil
	}
	return err == nil, err
}

// ParseObject deconstructs a GCS object name into (bucket, name).
func ParseObject(object string) (bucket, path string, err error) {
	parsed, err := url.Parse(object)
	if err != nil {
		return "", "", err
	}

	if parsed.Scheme != "gs" {
		return "", "", fmt.Errorf("object %s must have 'gs' scheme", object)
	}
	if parsed.Host == "" {
		return "", "", fmt.Errorf("object %s must have bucket", object)
	}
	if parsed.Path == "" {
		return parsed.Host, "", nil
	}

	// remove leading "/" in URL path
	return parsed.Host, parsed.Path[1:], nil
}
