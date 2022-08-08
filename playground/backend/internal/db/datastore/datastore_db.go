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

package datastore

import (
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/logger"
	"beam.apache.org/playground/backend/internal/utils"
	"cloud.google.com/go/datastore"
	"context"
	"fmt"
	"time"
)

const (
	Namespace = "Playground"

	SnippetKind = "pg_snippets"
	SchemaKind  = "pg_schema_versions"
	SdkKind     = "pg_sdks"
	FileKind    = "pg_files"
)

type Datastore struct {
	Client *datastore.Client
}

func New(ctx context.Context, projectId string) (*Datastore, error) {
	client, err := datastore.NewClient(ctx, projectId)
	if err != nil {
		logger.Errorf("Datastore: connection to store: error during connection, err: %s\n", err.Error())
		return nil, err
	}

	return &Datastore{Client: client}, nil
}

// PutSnippet puts the snippet entity to datastore
func (d *Datastore) PutSnippet(ctx context.Context, snipId string, snip *entity.Snippet) error {
	if snip == nil {
		logger.Errorf("Datastore: PutSnippet(): snippet is nil")
		return nil
	}
	snipKey := utils.GetNameKey(SnippetKind, snipId, Namespace, nil)
	tx, err := d.Client.NewTransaction(ctx)
	if err != nil {
		logger.Errorf("Datastore: PutSnippet(): error during the transaction creating, err: %s\n", err.Error())
		return err
	}
	if _, err = tx.Put(snipKey, snip.Snippet); err != nil {
		if rollBackErr := tx.Rollback(); rollBackErr != nil {
			err = rollBackErr
		}
		logger.Errorf("Datastore: PutSnippet(): error during the snippet entity saving, err: %s\n", err.Error())
		return err
	}

	var fileKeys []*datastore.Key
	for index := range snip.Files {
		fileId := fmt.Sprintf("%s_%d", snipId, index)
		fileKeys = append(fileKeys, utils.GetNameKey(FileKind, fileId, Namespace, nil))
	}

	if _, err = tx.PutMulti(fileKeys, snip.Files); err != nil {
		if rollBackErr := tx.Rollback(); rollBackErr != nil {
			err = rollBackErr
		}
		logger.Errorf("Datastore: PutSnippet(): error during the file entity saving, err: %s\n", err.Error())
		return err
	}

	if _, err = tx.Commit(); err != nil {
		logger.Errorf("Datastore: PutSnippet(): error during the transaction committing, err: %s\n", err.Error())
		return err
	}

	return nil
}

// GetSnippet returns the snippet entity by identifier
func (d *Datastore) GetSnippet(ctx context.Context, id string) (*entity.SnippetEntity, error) {
	key := utils.GetNameKey(SnippetKind, id, Namespace, nil)
	snip := new(entity.SnippetEntity)
	tx, err := d.Client.NewTransaction(ctx)
	if err != nil {
		logger.Errorf("Datastore: GetSnippet(): error during the transaction creating, err: %s\n", err.Error())
		return nil, err
	}
	if err = tx.Get(key, snip); err != nil {
		if rollBackErr := tx.Rollback(); rollBackErr != nil {
			err = rollBackErr
		}
		logger.Errorf("Datastore: GetSnippet(): error during snippet getting, err: %s\n", err.Error())
		return nil, err
	}
	snip.LVisited = time.Now()
	snip.VisitCount += 1
	if _, err = tx.Put(key, snip); err != nil {
		if rollBackErr := tx.Rollback(); rollBackErr != nil {
			err = rollBackErr
		}
		logger.Errorf("Datastore: GetSnippet(): error during snippet setting, err: %s\n", err.Error())
		return nil, err
	}
	if _, err = tx.Commit(); err != nil {
		logger.Errorf("Datastore: GetSnippet(): error during the transaction committing, err: %s\n", err.Error())
		return nil, err
	}
	return snip, nil
}

// PutSchemaVersion puts the schema entity to datastore
func (d *Datastore) PutSchemaVersion(ctx context.Context, id string, schema *entity.SchemaEntity) error {
	if schema == nil {
		logger.Errorf("Datastore: PutSchemaVersion(): schema version is nil")
		return nil
	}
	key := utils.GetNameKey(SchemaKind, id, Namespace, nil)
	if _, err := d.Client.Put(ctx, key, schema); err != nil {
		logger.Errorf("Datastore: PutSchemaVersion(): error during entity saving, err: %s\n", err.Error())
		return err
	}
	return nil
}

// PutSDKs puts the SDK entity to datastore
func (d *Datastore) PutSDKs(ctx context.Context, sdks []*entity.SDKEntity) error {
	if sdks == nil || len(sdks) == 0 {
		logger.Errorf("Datastore: PutSDKs(): sdks are empty")
		return nil
	}
	var keys []*datastore.Key
	for _, sdk := range sdks {
		keys = append(keys, utils.GetNameKey(SdkKind, sdk.Name, Namespace, nil))
	}
	if _, err := d.Client.PutMulti(ctx, keys, sdks); err != nil {
		logger.Errorf("Datastore: PutSDK(): error during entity saving, err: %s\n", err.Error())
		return err
	}
	return nil
}

//GetFiles returns the file entities by a snippet identifier
func (d *Datastore) GetFiles(ctx context.Context, snipId string, numberOfFiles int) ([]*entity.FileEntity, error) {
	if numberOfFiles == 0 {
		logger.Errorf("The number of files must be more than zero")
		return []*entity.FileEntity{}, nil
	}
	tx, err := d.Client.NewTransaction(ctx, datastore.ReadOnly)
	if err != nil {
		logger.Errorf("Datastore: GetFiles(): error during the transaction creating, err: %s\n", err.Error())
		return nil, err
	}
	var fileKeys []*datastore.Key
	for i := 0; i < numberOfFiles; i++ {
		fileId := fmt.Sprintf("%s_%d", snipId, i)
		fileKeys = append(fileKeys, utils.GetNameKey(FileKind, fileId, Namespace, nil))
	}
	var files = make([]*entity.FileEntity, numberOfFiles)
	if err = tx.GetMulti(fileKeys, files); err != nil {
		if rollBackErr := tx.Rollback(); rollBackErr != nil {
			err = rollBackErr
		}
		logger.Errorf("Datastore: GetFiles(): error during file getting, err: %s\n", err.Error())
		return nil, err
	}
	if _, err = tx.Commit(); err != nil {
		logger.Errorf("Datastore: GetFiles(): error during the transaction committing, err: %s\n", err.Error())
		return nil, err
	}
	return files, nil
}

//GetSDK returns the sdk entity by an identifier
func (d *Datastore) GetSDK(ctx context.Context, id string) (*entity.SDKEntity, error) {
	sdkId := utils.GetNameKey(SdkKind, id, Namespace, nil)
	sdk := new(entity.SDKEntity)
	if err := d.Client.Get(ctx, sdkId, sdk); err != nil {
		logger.Errorf("Datastore: GetSDK(): error during sdk getting, err: %s\n", err.Error())
		return nil, err
	}
	return sdk, nil
}
