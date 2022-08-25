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
	pb "beam.apache.org/playground/backend/internal/api/v1"
	datastoreDb "beam.apache.org/playground/backend/internal/db/datastore"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/utils"
	"time"
)

type DatastoreMapper struct {
	appEnv *environment.ApplicationEnvs
	props  *environment.Properties
}

func New(appEnv *environment.ApplicationEnvs, props *environment.Properties) *DatastoreMapper {
	return &DatastoreMapper{appEnv: appEnv, props: props}
}

func (m *DatastoreMapper) ToSnippet(info *pb.SaveSnippetRequest) *entity.Snippet {
	nowDate := time.Now()
	snippet := entity.Snippet{
		IDMeta: &entity.IDMeta{Salt: m.props.Salt, IdLength: m.props.IdLength},
		//OwnerId property will be used in Tour of Beam project
		Snippet: &entity.SnippetEntity{
			SchVer:        utils.GetNameKey(datastoreDb.SchemaKind, m.appEnv.SchemaVersion(), datastoreDb.Namespace, nil),
			Sdk:           utils.GetNameKey(datastoreDb.SdkKind, info.Sdk.String(), datastoreDb.Namespace, nil),
			PipeOpts:      info.PipelineOptions,
			Created:       nowDate,
			LVisited:      nowDate,
			Origin:        "PG_USER",
			NumberOfFiles: len(info.Files),
		},
	}
	return &snippet
}

func (m *DatastoreMapper) ToFileEntity(info *pb.SaveSnippetRequest, file *pb.SnippetFile) *entity.FileEntity {
	var isMain bool
	if len(info.Files) == 1 {
		isMain = true
	} else {
		isMain = utils.IsFileMain(file.Content, info.Sdk)
	}
	return &entity.FileEntity{
		Name:     utils.GetFileName(file.Name, info.Sdk),
		Content:  file.Content,
		CntxLine: 1,
		IsMain:   isMain,
	}
}
