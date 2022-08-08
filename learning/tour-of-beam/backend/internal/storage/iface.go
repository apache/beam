package storage

import (
	"context"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
)

type Iface interface {
	GetContentTree(ctx context.Context, sdk tob.Sdk) (tob.ContentTree, error)
}
