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
	"context"
	"time"

	"cloud.google.com/go/datastore"

	"beam.apache.org/playground/backend/internal/constants"
	"beam.apache.org/playground/backend/internal/db/dto"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/db/mapper"
	"beam.apache.org/playground/backend/internal/logger"
	"beam.apache.org/playground/backend/internal/utils"
)

const (
	errorMsgTemplateCreatingTx = "error during creating transaction, err: %s\n"
	errorMsgTemplateTxRollback = "error during transaction rollback, err: %s\n"
	errorMsgTemplateTxCommit   = "error during transaction commit, err: %s\n"
)

type Datastore struct {
	Client         *datastore.Client
	ResponseMapper mapper.ResponseMapper
}

func New(ctx context.Context, responseMapper mapper.ResponseMapper, projectId string) (*Datastore, error) {
	client, err := datastore.NewClient(ctx, projectId)
	if err != nil {
		logger.Errorf("Datastore: connection to store: error during connection, err: %s\n", err.Error())
		return nil, err
	}
	return &Datastore{Client: client, ResponseMapper: responseMapper}, nil
}

// PutSnippet puts the snippet entity to datastore
func (d *Datastore) PutSnippet(ctx context.Context, snipId string, snip *entity.Snippet) error {
	if snip == nil {
		logger.Errorf("Datastore: PutSnippet(): snippet is nil")
		return nil
	}
	snipKey := utils.GetSnippetKey(ctx, snipId)
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
		fileKeys = append(fileKeys, utils.GetFileKey(ctx, snipId, index))
	}

	if _, err = tx.PutMulti(fileKeys, snip.Files); err != nil {
		if rollBackErr := tx.Rollback(); rollBackErr != nil {
			err = rollBackErr
		}
		logger.Errorf("Datastore: PutSnippet(): error during the file entity saving, err: %s\n", err.Error())
		return err
	}

	if _, err = tx.Commit(); err != nil {
		logger.Errorf(errorMsgTemplateTxCommit, err.Error())
		return err
	}

	return nil
}

// GetSnippet returns the snippet entity by identifier
func (d *Datastore) GetSnippet(ctx context.Context, id string) (*entity.SnippetEntity, error) {
	key := utils.GetSnippetKey(ctx, id)
	snip := new(entity.SnippetEntity)
	tx, err := d.Client.NewTransaction(ctx)
	if err != nil {
		logger.Errorf(errorMsgTemplateCreatingTx, err.Error())
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
		logger.Errorf(errorMsgTemplateTxCommit, err.Error())
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
	key := utils.GetSchemaVerKey(ctx, id)
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
		keys = append(keys, utils.GetSdkKey(ctx, sdk.Name))
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
	defer rollback(tx)

	if err != nil {
		logger.Errorf("Datastore: GetFiles(): error during the transaction creating, err: %s\n", err.Error())
		return nil, err
	}
	var fileKeys []*datastore.Key
	for fileIndx := 0; fileIndx < numberOfFiles; fileIndx++ {
		fileKeys = append(fileKeys, utils.GetFileKey(ctx, snipId, fileIndx))
	}
	var files = make([]*entity.FileEntity, numberOfFiles)
	if err = tx.GetMulti(fileKeys, files); err != nil {
		logger.Errorf("Datastore: GetFiles(): error during file getting, err: %s\n", err.Error())
		return nil, err
	}
	return files, nil
}

//GetSDKs returns sdk entities by an identifier
func (d *Datastore) GetSDKs(ctx context.Context) ([]*entity.SDKEntity, error) {
	var sdkKeys []*datastore.Key
	for sdkName := range pb.Sdk_value {
		if sdkName != pb.Sdk_SDK_UNSPECIFIED.String() {
			sdkKeys = append(sdkKeys, utils.GetSdkKey(ctx, sdkName))
		}
	}
	var sdks = make([]*entity.SDKEntity, len(sdkKeys))
	if err := d.Client.GetMulti(ctx, sdkKeys, sdks); err != nil {
		logger.Errorf("Datastore: GetSDKs(): error during the getting sdks, err: %s\n", err.Error())
		return nil, err
	}
	for sdkIndex, sdk := range sdks {
		sdk.Name = sdkKeys[sdkIndex].Name
	}
	return sdks, nil
}

//GetCatalog returns all examples
func (d *Datastore) GetCatalog(ctx context.Context, sdkCatalog []*entity.SDKEntity) ([]*pb.Categories, error) {
	//Retrieving examples
	exampleQuery := datastore.NewQuery(constants.ExampleKind).Namespace(utils.GetNamespace(ctx))
	var examples []*entity.ExampleEntity
	exampleKeys, err := d.Client.GetAll(ctx, exampleQuery, &examples)
	if err != nil {
		logger.Errorf("Datastore: GetCatalog(): error during the getting examples, err: %s\n", err.Error())
		return nil, err
	}

	//Retrieving snippets
	var snippetKeys []*datastore.Key
	for _, exampleKey := range exampleKeys {
		snippetKeys = append(snippetKeys, utils.GetSnippetKey(ctx, exampleKey.Name))
	}
	snippets := make([]*entity.SnippetEntity, len(snippetKeys))
	if err = d.Client.GetMulti(ctx, snippetKeys, snippets); err != nil {
		logger.Errorf("Datastore: GetCatalog(): error during the getting snippets, err: %s\n", err.Error())
		return nil, err
	}

	//Retrieving files
	var fileKeys []*datastore.Key
	for snpIndx, snippet := range snippets {
		for fileIndx := 0; fileIndx < snippet.NumberOfFiles; fileIndx++ {
			fileKey := utils.GetFileKey(ctx, exampleKeys[snpIndx].Name, fileIndx)
			fileKeys = append(fileKeys, fileKey)
		}
	}
	files := make([]*entity.FileEntity, len(fileKeys))
	if err = d.Client.GetMulti(ctx, fileKeys, files); err != nil {
		logger.Errorf("Datastore: GetCatalog(): error during the getting files, err: %s\n", err.Error())
		return nil, err
	}

	return d.ResponseMapper.ToArrayCategories(&dto.CatalogDTO{
		Examples:   examples,
		Snippets:   snippets,
		Files:      files,
		SdkCatalog: sdkCatalog,
	}), nil
}

//DeleteUnusedSnippets deletes all unused snippets
func (d *Datastore) DeleteUnusedSnippets(ctx context.Context, dayDiff int32) error {
	var hoursDiff = dayDiff * 24
	boundaryDate := time.Now().Add(-time.Hour * time.Duration(hoursDiff))
	snippetQuery := datastore.NewQuery(constants.SnippetKind).
		Namespace(utils.GetNamespace(ctx)).
		Filter("lVisited <= ", boundaryDate).
		Filter("origin =", constants.UserSnippetOrigin).
		Project("numberOfFiles")
	var snpDtos []*dto.SnippetDeleteDTO
	snpKeys, err := d.Client.GetAll(ctx, snippetQuery, &snpDtos)
	if err != nil {
		logger.Errorf("Datastore: DeleteUnusedSnippets(): error during deleting unused snippets, err: %s\n", err.Error())
		return err
	}
	var fileKeys []*datastore.Key
	for snpIndex, snpKey := range snpKeys {
		for fileIndex := 0; fileIndex < snpDtos[snpIndex].NumberOfFiles; fileIndex++ {
			fileKeys = append(fileKeys, utils.GetFileKey(ctx, snpKey.Name, fileIndex))
		}
	}
	_, err = d.Client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		err = tx.DeleteMulti(fileKeys)
		err = tx.DeleteMulti(snpKeys)
		return err
	})
	if err != nil {
		logger.Errorf("Datastore: DeleteUnusedSnippets(): error during deleting unused snippets, err: %s\n", err.Error())
		return err
	}
	return nil
}

func rollback(tx *datastore.Transaction) {
	err := tx.Rollback()
	if err != nil {
		logger.Errorf(errorMsgTemplateTxRollback, err.Error())
	}
}
