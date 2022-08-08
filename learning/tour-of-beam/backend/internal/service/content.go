package service

import (
	"context"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
	"beam.apache.org/learning/tour-of-beam/backend/internal/storage"
)

type IContent interface {
	GetContentTree(ctx context.Context, sdk tob.Sdk, userId *string) (tob.ContentTree, error)
	// GetUnitContent(ctx context.Context, unitId string, userId *string) (tob.UnitContent, error)
}

type Svc struct {
	Repo storage.Iface
}

func (s *Svc) GetContentTree(ctx context.Context, sdk tob.Sdk, userId *string) (ct tob.ContentTree, err error) {
	return s.Repo.GetContentTree(ctx, sdk)
}
