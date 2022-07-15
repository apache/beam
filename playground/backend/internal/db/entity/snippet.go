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

package entity

import (
	"beam.apache.org/playground/backend/internal/utils"
	"cloud.google.com/go/datastore"
	"fmt"
	"sort"
	"strings"
	"time"
)

type FileEntity struct {
	Name     string `datastore:"name"`
	Content  string `datastore:"content,noindex"`
	CntxLine int32  `datastore:"cntxLine"`
	IsMain   bool   `datastore:"isMain"`
}

type SnippetEntity struct {
	OwnerId       string         `datastore:"ownerId"`
	Sdk           *datastore.Key `datastore:"sdk"`
	PipeOpts      string         `datastore:"pipeOpts"`
	Created       time.Time      `datastore:"created"`
	LVisited      time.Time      `datastore:"lVisited"`
	Origin        string         `datastore:"origin"`
	VisitCount    int            `datastore:"visitCount"`
	SchVer        *datastore.Key `datastore:"schVer"`
	NumberOfFiles int            `datastore:"numberOfFiles"`
}

type Snippet struct {
	*IDMeta
	Snippet *SnippetEntity
	Files   []*FileEntity
}

// ID generates id according to content of the entity
func (s *Snippet) ID() (string, error) {
	var files []string
	for _, v := range s.Files {
		files = append(files, strings.TrimSpace(v.Content)+strings.TrimSpace(v.Name))
	}
	sort.Strings(files)
	var contentBuilder strings.Builder
	for i, file := range files {
		contentBuilder.WriteString(file)
		if i == len(files)-1 {
			contentBuilder.WriteString(fmt.Sprintf("%v%s", s.Snippet.Sdk, strings.TrimSpace(s.Snippet.PipeOpts)))
		}
	}
	id, err := utils.ID(s.Salt, contentBuilder.String(), s.IdLength)
	if err != nil {
		return "", err
	}
	return id, nil
}
