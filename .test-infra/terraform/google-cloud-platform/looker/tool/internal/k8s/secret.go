package k8s

import (
	k8s "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

func foo() error {
	_, err := k8s.New(config.GetConfigOrDie(), k8s.Options{})
	return err
}
