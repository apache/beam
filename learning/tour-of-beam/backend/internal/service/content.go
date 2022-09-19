// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"context"
	"errors"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
	"beam.apache.org/learning/tour-of-beam/backend/internal/storage"
)

var ErrNoUnit = errors.New("unit not found")

type IContent interface {
	GetContentTree(ctx context.Context, sdk tob.Sdk, userId *string) (tob.ContentTree, error)
	GetUnitContent(ctx context.Context, sdk tob.Sdk, unitId string, userId *string) (tob.Unit, error)
}

type Svc struct {
	Repo storage.Iface
}

func (s *Svc) GetContentTree(ctx context.Context, sdk tob.Sdk, userId *string) (ct tob.ContentTree, err error) {
	// TODO enrich tree with user-specific state (isCompleted)
	return s.Repo.GetContentTree(ctx, sdk)
}

func (s *Svc) GetUnitContent(ctx context.Context, sdk tob.Sdk, unitId string, userId *string) (tob.Unit, error) {
	// TODO enrich unit with user-specific state: isCompleted, userSnippetId
	unit, err := s.Repo.GetUnitContent(ctx, sdk, unitId)
	if err != nil {
		return tob.Unit{}, err
	}
	if unit == nil {
		return tob.Unit{}, ErrNoUnit
	}
	return *unit, nil
}
