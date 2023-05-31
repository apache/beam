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
	"fmt"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
	"beam.apache.org/learning/tour-of-beam/backend/internal/storage"
	pb "beam.apache.org/learning/tour-of-beam/backend/playground_api/api/v1"
)

type IContent interface {
	GetContentTree(ctx context.Context, sdk tob.Sdk) (tob.ContentTree, error)
	GetUnitContent(ctx context.Context, sdk tob.Sdk, unitId string) (tob.Unit, error)
	GetUserProgress(ctx context.Context, sdk tob.Sdk, userId string) (tob.SdkProgress, error)
	SetUnitComplete(ctx context.Context, sdk tob.Sdk, unitId, uid string) error
	SaveUserCode(ctx context.Context, sdk tob.Sdk, unitId, uid string, userRequest tob.UserCodeRequest) error
	DeleteProgress(ctx context.Context, uid string) error
}

type Svc struct {
	Repo     storage.Iface
	PgClient pb.PlaygroundServiceClient
}

func (s *Svc) GetContentTree(ctx context.Context, sdk tob.Sdk) (ct tob.ContentTree, err error) {
	return s.Repo.GetContentTree(ctx, sdk)
}

func (s *Svc) GetUnitContent(ctx context.Context, sdk tob.Sdk, unitId string) (tob.Unit, error) {
	unit, err := s.Repo.GetUnitContent(ctx, sdk, unitId)
	if err != nil {
		return tob.Unit{}, err
	}
	if unit == nil {
		return tob.Unit{}, tob.ErrNoUnit
	}
	return *unit, nil
}

func (s *Svc) GetUserProgress(ctx context.Context, sdk tob.Sdk, userId string) (tob.SdkProgress, error) {
	progress, err := s.Repo.GetUserProgress(ctx, sdk, userId)
	if errors.Is(err, tob.ErrNoUser) {
		// make an empty list a default response
		return tob.SdkProgress{Units: make([]tob.UnitProgress, 0)}, nil
	}
	if err != nil {
		return tob.SdkProgress{}, err
	}
	if progress == nil {
		panic("progress is nil, no err")
	}

	return *progress, nil
}

func (s *Svc) SetUnitComplete(ctx context.Context, sdk tob.Sdk, unitId, uid string) error {
	if err := s.Repo.CheckUnitExists(ctx, sdk, unitId); err != nil {
		return err
	}

	return s.Repo.SetUnitComplete(ctx, sdk, unitId, uid)
}

func (s *Svc) SaveUserCode(ctx context.Context, sdk tob.Sdk, unitId, uid string, userRequest tob.UserCodeRequest) error {
	if err := s.Repo.CheckUnitExists(ctx, sdk, unitId); err != nil {
		return err
	}

	// two-phased commit for 2 resources: datastore tb_user_progress entity and Playground::SaveSnippet call
	// In a datastore transaction:
	// 1. Get tb_user_progress for (sdk, unit, user), if any.
	//    Otherwise, fill in the default parameters and generate a new persistence_key
	// 2. Call Playground::SaveSnippet GRPC call, return a SnippetId
	// 3. Upsert into tb_user_progress with the obtained SnippetId and persistence_key

	// callback: Playground::SaveSnippet GRPC call, return snippetID
	savePgSnippet := func(persistenceKey string) (string, error) {
		req := MakePgSaveRequest(userRequest, sdk, persistenceKey)
		resp, err := s.PgClient.SaveSnippet(ctx, &req)
		if err != nil {
			return "", err
		}
		fmt.Println("SaveSnippet response:", resp)
		return resp.GetId(), nil
	}

	return s.Repo.SaveUserSnippetId(ctx, sdk, unitId, uid, savePgSnippet)
}

func (s *Svc) DeleteProgress(ctx context.Context, uid string) error {
	return s.Repo.DeleteProgress(ctx, uid)
}
