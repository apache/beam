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

package dto

import (
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/db/entity"
)

type ObjectInfo struct {
	Name            string
	CloudPath       string
	Description     string                   `protobuf:"bytes,3,opt,name=description,proto3" json:"description,omitempty"`
	Type            pb.PrecompiledObjectType `protobuf:"varint,4,opt,name=type,proto3,enum=api.v1.PrecompiledObjectType" json:"type,omitempty"`
	Categories      []string                 `json:"categories,omitempty"`
	PipelineOptions string                   `protobuf:"bytes,3,opt,name=pipeline_options,proto3" json:"pipeline_options,omitempty"`
	Link            string                   `protobuf:"bytes,3,opt,name=link,proto3" json:"link,omitempty"`
	Multifile       bool                     `protobuf:"varint,7,opt,name=multifile,proto3" json:"multifile,omitempty"`
	ContextLine     int32                    `protobuf:"varint,7,opt,name=context_line,proto3" json:"context_line,omitempty"`
	DefaultExample  bool                     `protobuf:"varint,7,opt,name=default_example,json=defaultExample,proto3" json:"default_example,omitempty"`
}

type PrecompiledObjects []ObjectInfo
type CategoryToPrecompiledObjects map[string]PrecompiledObjects
type SdkToCategories map[string]CategoryToPrecompiledObjects

type CatalogDTO struct {
	Examples   []*entity.ExampleEntity
	Snippets   []*entity.SnippetEntity
	Files      []*entity.FileEntity
	SdkCatalog []*entity.SDKEntity
}

type DefaultExamplesDTO struct {
	Examples []*entity.ExampleEntity
	Snippets []*entity.SnippetEntity
	Files    []*entity.FileEntity
}

func (c *CatalogDTO) GetSdkCatalogAsMap() map[string]string {
	sdkToExample := make(map[string]string)
	for _, sdk := range c.SdkCatalog {
		sdkToExample[sdk.Name] = sdk.DefaultExample
	}
	return sdkToExample
}

type ExampleDTO struct {
	Example            *entity.ExampleEntity
	Snippet            *entity.SnippetEntity
	Files              []*entity.FileEntity
	DefaultExampleName string
}

func (e *ExampleDTO) HasMultiFiles() bool {
	return e.Snippet.NumberOfFiles > 1
}

func (e *ExampleDTO) IsDefault() bool {
	return e.Example.Name == e.DefaultExampleName
}

func (e *ExampleDTO) GetType() pb.PrecompiledObjectType {
	return pb.PrecompiledObjectType(pb.PrecompiledObjectType_value[e.Example.Type])
}

func (e *ExampleDTO) GetContextLine() int32 {
	return e.Files[0].CntxLine
}
