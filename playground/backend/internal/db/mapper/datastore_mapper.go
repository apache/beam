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

package mapper

import (
	"context"
	"time"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/constants"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/utils"
)

type DatastoreMapper struct {
	ctx    context.Context
	appEnv *environment.ApplicationEnvs
	props  *environment.Properties
}

func NewDatastoreMapper(ctx context.Context, appEnv *environment.ApplicationEnvs, props *environment.Properties) *DatastoreMapper {
	return &DatastoreMapper{ctx: ctx, appEnv: appEnv, props: props}
}

func (m *DatastoreMapper) ToSnippet(info *pb.SaveSnippetRequest) *entity.Snippet {
	nowDate := time.Now()

	origin := constants.UserSnippetOrigin
	if info.PersistenceKey > "" {
		origin = constants.TbUserSnippetOrigin
	}
	snippet := entity.Snippet{
		IDMeta: &entity.IDMeta{Salt: m.props.Salt, IdLength: m.props.IdLength},
		//OwnerId property will be used in Tour of Beam project
		Snippet: &entity.SnippetEntity{
			SchVer:         utils.GetSchemaVerKey(m.ctx, m.appEnv.SchemaVersion()),
			Sdk:            utils.GetSdkKey(m.ctx, info.Sdk.String()),
			PipeOpts:       info.PipelineOptions,
			Created:        nowDate,
			LVisited:       nowDate,
			Origin:         origin,
			NumberOfFiles:  len(info.Files),
			Complexity:     info.Complexity.String(),
			PersistenceKey: info.PersistenceKey,
		},
	}
	return &snippet
}

func (m *DatastoreMapper) ToFileEntity(info *pb.SaveSnippetRequest, file *pb.SnippetFile) (*entity.FileEntity, error) {
	var isMain bool
	if len(info.Files) == 1 {
		isMain = true
	} else {
		isMain = utils.IsFileMain(file.Content, info.Sdk)
	}
	fileName, err := utils.GetFileName(file.Name, file.Content, info.Sdk)
	if err != nil {
		return nil, err
	}
	return &entity.FileEntity{
		Name:     fileName,
		Content:  file.Content,
		CntxLine: 1,
		IsMain:   isMain,
	}, nil
}
