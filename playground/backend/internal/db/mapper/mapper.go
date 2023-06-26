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
	"beam.apache.org/playground/backend/internal/db/dto"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/db/entity"
)

type EntityMapper interface {
	ToSnippet(info *pb.SaveSnippetRequest) *entity.Snippet
	ToFileEntity(info *pb.SaveSnippetRequest, file *pb.SnippetFile) (*entity.FileEntity, error)
}

type ResponseMapper interface {
	ToArrayCategories(catalogDTO *dto.CatalogDTO) []*pb.Categories
	ToObjectInfo(exampleDTO *dto.ExampleDTO) *dto.ObjectInfo
	ToDefaultPrecompiledObjects(defaultExamplesDTO *dto.DefaultExamplesDTO) map[pb.Sdk]*pb.PrecompiledObject
	ToPrecompiledObj(exampleId string, exampleDTO *dto.ExampleDTO) *pb.PrecompiledObject
	ToDatasetBySnippetIDMap(datasets []*entity.DatasetEntity, snippets []*entity.SnippetEntity) (map[string][]*dto.DatasetDTO, error)
}
