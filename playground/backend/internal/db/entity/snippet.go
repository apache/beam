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
	"crypto/sha256"
	"encoding/base64"
	"io"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/datastore"

	"beam.apache.org/playground/backend/internal/errors"
	"beam.apache.org/playground/backend/internal/logger"
)

type IDMeta struct {
	Salt     string
	IdLength int8
}

type FileEntity struct {
	Name     string `datastore:"name"`
	Content  string `datastore:"content,noindex"`
	CntxLine int32  `datastore:"cntxLine"`
	IsMain   bool   `datastore:"isMain"`
}

type SnippetEntity struct {
	Key            *datastore.Key         `datastore:"__key__"`
	OwnerId        string                 `datastore:"ownerId"`
	Sdk            *datastore.Key         `datastore:"sdk"`
	PipeOpts       string                 `datastore:"pipeOpts"`
	Created        time.Time              `datastore:"created"`
	LVisited       time.Time              `datastore:"lVisited"`
	Origin         string                 `datastore:"origin"`
	VisitCount     int                    `datastore:"visitCount"`
	SchVer         *datastore.Key         `datastore:"schVer"`
	NumberOfFiles  int                    `datastore:"numberOfFiles"`
	Complexity     string                 `datastore:"complexity"`
	PersistenceKey string                 `datastore:"persistenceKey,omitempty"`
	Datasets       []*DatasetNestedEntity `datastore:"datasets,omitempty"`
}

type DatasetEntity struct {
	Key  *datastore.Key `datastore:"__key__"`
	Path string         `datastore:"path"`
}

type DatasetNestedEntity struct {
	Config   string         `datastore:"config"`
	Dataset  *datastore.Key `datastore:"dataset"`
	Emulator string         `datastore:"emulator"`
}

type Snippet struct {
	*IDMeta
	Snippet *SnippetEntity
	Files   []*FileEntity
}

// ID generates id according to content of the entity
func (s *Snippet) ID() (string, error) {
	id, err := generateIDBasedOnContent(s.Salt, combineUniqueSnippetContent(s), s.IdLength)
	if err != nil {
		return "", err
	}
	return id, nil
}

func combineUniqueSnippetContent(snippet *Snippet) string {
	var files []string
	for _, file := range snippet.Files {
		files = append(files, strings.TrimSpace(file.Content)+strings.TrimSpace(file.Name))
	}
	sort.Strings(files)
	var contentBuilder strings.Builder
	for _, file := range files {
		contentBuilder.WriteString(file)
	}
	contentBuilder.WriteString(snippet.Snippet.Sdk.String())
	contentBuilder.WriteString(strings.TrimSpace(snippet.Snippet.PipeOpts))
	contentBuilder.WriteString(strings.TrimSpace(snippet.Snippet.Complexity))
	contentBuilder.WriteString(snippet.Snippet.PersistenceKey)

	return contentBuilder.String()
}

func generateIDBasedOnContent(salt, content string, length int8) (string, error) {
	hash := sha256.New()
	if _, err := io.WriteString(hash, salt); err != nil {
		logger.Errorf("ID(): error during hash generation: %s", err.Error())
		return "", errors.InternalError("Error during hash generation", "Error writing hash and salt")
	}
	hash.Write([]byte(content))
	sum := hash.Sum(nil)
	b := make([]byte, base64.URLEncoding.EncodedLen(len(sum)))
	base64.URLEncoding.Encode(b, sum)
	hashLen := int(length)
	for hashLen <= len(b) && b[hashLen-1] == '_' {
		hashLen++
	}
	return string(b)[:hashLen], nil
}
