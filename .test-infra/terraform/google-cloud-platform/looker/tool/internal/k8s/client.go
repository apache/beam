package k8s

import (
	"fmt"

	"k8s.io/client-go/rest"
	k8s "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

type Client struct {
	internal k8s.Client
}

func NewDefaultClient() (*Client, error) {
	return New(config.GetConfigOrDie(), k8s.Options{})
}

func New(config *rest.Config, options k8s.Options) (*Client, error) {
	internal, err := k8s.New(config, options)
	if err != nil {
		return nil, fmt.Errorf("error instantiating kubernetes client: %w", err)
	}
	return &Client{
		internal: internal,
	}, nil
}
