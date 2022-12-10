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
	"errors"
	"fmt"
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

// Delete unused snippets by given persistenceKey
func (d *Datastore) deleteObsoleteSnippets(ctx context.Context, snipKey *datastore.Key, persistenceKey string) error {
	if persistenceKey == "" || snipKey == nil {
		logger.Debugf("no persistence key or no current snippet key")
		return nil
	}
	// If persistenceKey is given, find the previous snippet version
	snippetQuery := datastore.NewQuery(constants.SnippetKind).
		Namespace(utils.GetNamespace(ctx)).
		FilterField("persistenceKey", "=", persistenceKey)

		// At the moment, datastore emulator doesn't allow != filters,
		// hence this crutches
		// https://cloud.google.com/datastore/docs/tools/datastore-emulator#known_issues
		// When it's fixed, post-query filter could be replaced with
		//
		// FilterField("__key__", "!=", snipKey)

	return d.deleteSnippets(ctx, snippetQuery, snipKey)
}

// PutSnippet puts the snippet entity to datastore
func (d *Datastore) PutSnippet(ctx context.Context, snipId string, snip *entity.Snippet) error {
	logger.Debugf("putting snippet %q, persistent key %q...", snipId, snip.Snippet.PersistenceKey)
	if snip == nil {
		logger.Errorf("Datastore: PutSnippet(): snippet is nil")
		return nil
	}
	snipKey := utils.GetSnippetKey(ctx, snipId)

	// Create the new snippet
	_, err := d.Client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		if _, err := tx.Put(snipKey, snip.Snippet); err != nil {
			logger.Errorf("Datastore: PutSnippet(): error during the snippet entity saving, err: %s\n", err.Error())
			return err
		}

		var fileKeys []*datastore.Key
		for index := range snip.Files {
			fileKeys = append(fileKeys, utils.GetFileKey(ctx, snipId, index))
		}

		if _, err := tx.PutMulti(fileKeys, snip.Files); err != nil {
			logger.Errorf("Datastore: PutSnippet(): error during the file entity saving, err: %s\n", err.Error())
			return err
		}
		return nil
	})
	if err != nil {
		logger.Errorf("Datastore: PutSnippet(): error during commit, err: %s\n", err.Error())
		return err
	}

	return d.deleteObsoleteSnippets(ctx, snipKey, snip.Snippet.PersistenceKey)
}

// GetSnippet returns the snippet entity by identifier
func (d *Datastore) GetSnippet(ctx context.Context, id string) (*entity.SnippetEntity, error) {
	key := utils.GetSnippetKey(ctx, id)
	snip := new(entity.SnippetEntity)

	_, err := d.Client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		if err := tx.Get(key, snip); err != nil {
			logger.Errorf("Datastore: GetSnippet(): error during snippet getting, err: %s\n", err.Error())
			return err
		}
		snip.LVisited = time.Now()
		snip.VisitCount += 1
		if _, err := tx.Put(key, snip); err != nil {
			logger.Errorf("Datastore: GetSnippet(): error during snippet setting, err: %s\n", err.Error())
			return err
		}
		return nil
	})
	if err != nil {
		logger.Errorf("Datastore: GetSnippet: error updating snippet: %s\n", err.Error())
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

	//Retrieving datasets
	datastoreQuery := datastore.NewQuery(constants.DatasetKind).Namespace(utils.GetNamespace(ctx))
	var datasets []*entity.DatasetEntity
	if _, err = d.Client.GetAll(ctx, datastoreQuery, &datasets); err != nil {
		logger.Errorf("Datastore: GetCatalog(): error during the getting datasets, err: %s\n", err.Error())
		return nil, err
	}

	var datasetBySnippetIDMap map[string][]*dto.DatasetDTO
	if len(datasets) != 0 {
		datasetBySnippetIDMap, err = d.ResponseMapper.ToDatasetBySnippetIDMap(datasets, snippets)
		if err != nil {
			return nil, err
		}
	}

	return d.ResponseMapper.ToArrayCategories(&dto.CatalogDTO{
		Examples:              examples,
		Snippets:              snippets,
		Files:                 files,
		SdkCatalog:            sdkCatalog,
		DatasetBySnippetIDMap: datasetBySnippetIDMap,
	}), nil
}

//GetDefaultExamples returns the default examples
func (d *Datastore) GetDefaultExamples(ctx context.Context, sdks []*entity.SDKEntity) (map[pb.Sdk]*pb.PrecompiledObject, error) {
	tx, err := d.Client.NewTransaction(ctx, datastore.ReadOnly)
	if err != nil {
		logger.Errorf(errorMsgTemplateCreatingTx, err.Error())
		return nil, err
	}
	defer rollback(tx)

	//Retrieving examples
	var exampleKeys []*datastore.Key
	for _, sdk := range sdks {
		exampleKeys = append(exampleKeys, utils.GetExampleKey(ctx, sdk.Name, sdk.DefaultExample))
	}
	examples, err := getEntities[entity.ExampleEntity](tx, exampleKeys)
	if err != nil {
		return nil, err
	}

	if len(examples) == 0 {
		logger.Error("no default example")
		return nil, fmt.Errorf("no default example")
	}

	//Retrieving snippets
	var snippetKeys []*datastore.Key
	for _, exampleKey := range exampleKeys {
		snippetKeys = append(snippetKeys, utils.GetSnippetKey(ctx, exampleKey.Name))
	}
	snippets, err := getEntities[entity.SnippetEntity](tx, snippetKeys)
	if err != nil {
		return nil, err
	}

	//Retrieving files
	var fileKeys []*datastore.Key
	for snpIndx, snippet := range snippets {
		for fileIndx := 0; fileIndx < snippet.NumberOfFiles; fileIndx++ {
			fileKey := utils.GetFileKey(ctx, examples[snpIndx].Sdk.Name, examples[snpIndx].Name, fileIndx)
			fileKeys = append(fileKeys, fileKey)
		}
	}
	files := make([]*entity.FileEntity, len(fileKeys))
	if err = tx.GetMulti(fileKeys, files); err != nil {
		logger.Errorf("error during the getting files, err: %s\n", err.Error())
		return nil, err
	}

	return d.ResponseMapper.ToDefaultPrecompiledObjects(&dto.DefaultExamplesDTO{
		Examples: examples,
		Snippets: snippets,
		Files:    files,
	}), nil
}

func (d *Datastore) GetExample(ctx context.Context, id string, sdks []*entity.SDKEntity) (*pb.PrecompiledObject, error) {
	tx, err := d.Client.NewTransaction(ctx, datastore.ReadOnly)
	if err != nil {
		logger.Errorf(errorMsgTemplateCreatingTx, err.Error())
		return nil, err
	}
	defer rollback(tx)

	exampleKey := utils.GetExampleKey(ctx, id)
	var example = new(entity.ExampleEntity)
	if err = tx.Get(exampleKey, example); err != nil {
		if err == datastore.ErrNoSuchEntity {
			logger.Warnf("error during getting example by identifier, err: %s", err.Error())
			return nil, err
		}
		logger.Errorf("error during getting example by identifier, err: %s", err.Error())
		return nil, err
	}

	snpKey := utils.GetSnippetKey(ctx, id)
	var snippet = new(entity.SnippetEntity)
	if err = tx.Get(snpKey, snippet); err != nil {
		logger.Errorf("error during getting snippet by identifier, err: %s", err.Error())
		return nil, err
	}

	fileKey := utils.GetFileKey(ctx, id, 0)
	var file = new(entity.FileEntity)
	if err = tx.Get(fileKey, file); err != nil {
		logger.Errorf("error during getting file by identifier, err: %s", err.Error())
		return nil, err
	}

	var dataset = new(entity.DatasetEntity)
	if len(snippet.Datasets) != 0 {
		datasetKey := snippet.Datasets[0].Dataset
		if err = tx.Get(datasetKey, dataset); err != nil {
			logger.Errorf("error during getting dataset by identifier, err: %s", err.Error())
			return nil, err
		}
	}

	sdkToExample := make(map[string]string)
	for _, sdk := range sdks {
		sdkToExample[sdk.Name] = sdk.DefaultExample
	}

	var datasetBySnippetIDMap map[string][]*dto.DatasetDTO
	if len(snippet.Datasets) != 0 {
		datasetBySnippetIDMap, err = d.ResponseMapper.ToDatasetBySnippetIDMap([]*entity.DatasetEntity{dataset}, []*entity.SnippetEntity{snippet})
		if err != nil {
			return nil, err
		}
	}
	var datasetDTOs []*dto.DatasetDTO
	if len(datasetBySnippetIDMap) != 0 {
		datasetDTOsVal, ok := datasetBySnippetIDMap[snippet.Key.Name]
		if ok {
			datasetDTOs = datasetDTOsVal
		}
	}

	return d.ResponseMapper.ToPrecompiledObj(&dto.ExampleDTO{
		Example:            example,
		Snippet:            snippet,
		Files:              []*entity.FileEntity{file},
		DefaultExampleName: sdkToExample[example.Sdk.Name],
		Datasets:           datasetDTOs,
	}), err
}

func (d *Datastore) GetExampleCode(ctx context.Context, id string) (string, error) {
	tx, err := d.Client.NewTransaction(ctx, datastore.ReadOnly)
	if err != nil {
		logger.Errorf(errorMsgTemplateCreatingTx, err.Error())
		return "", err
	}
	defer rollback(tx)

	fileKey := utils.GetFileKey(ctx, id, 0)
	var file = new(entity.FileEntity)
	if err = tx.Get(fileKey, file); err != nil {
		if err == datastore.ErrNoSuchEntity {
			logger.Warnf("error during getting example code by identifier, err: %s", err.Error())
			return "", err
		}
		logger.Errorf("error during getting example code by identifier, err: %s", err.Error())
		return "", err
	}
	return file.Content, nil
}

func (d *Datastore) GetExampleOutput(ctx context.Context, id string) (string, error) {
	tx, err := d.Client.NewTransaction(ctx, datastore.ReadOnly)
	if err != nil {
		logger.Errorf(errorMsgTemplateCreatingTx, err.Error())
		return "", err
	}
	defer rollback(tx)

	pcObjKey := utils.GetPCObjectKey(ctx, id, constants.PCOutputType)
	var pcObj = new(entity.PrecompiledObjectEntity)
	if err = tx.Get(pcObjKey, pcObj); err != nil {
		if err == datastore.ErrNoSuchEntity {
			logger.Warnf("error during getting example output by identifier, err: %s", err.Error())
			return "", err
		}
		logger.Errorf("error during getting example output by identifier, err: %s", err.Error())
		return "", err
	}
	return pcObj.Content, nil
}

func (d *Datastore) GetExampleLogs(ctx context.Context, id string) (string, error) {
	pcObjKey := utils.GetPCObjectKey(ctx, id, constants.PCLogType)
	var pcObj = new(entity.PrecompiledObjectEntity)
	if err := d.Client.Get(ctx, pcObjKey, pcObj); err != nil {
		if err == datastore.ErrNoSuchEntity {
			logger.Warnf("error during getting example logs by identifier, err: %s", err.Error())
			return "", err
		}
		logger.Errorf("error during getting example logs by identifier, err: %s", err.Error())
		return "", err
	}
	return pcObj.Content, nil
}

func (d *Datastore) GetExampleGraph(ctx context.Context, id string) (string, error) {
	pcObjKey := utils.GetPCObjectKey(ctx, id, constants.PCGraphType)
	var pcObj = new(entity.PrecompiledObjectEntity)
	if err := d.Client.Get(ctx, pcObjKey, pcObj); err != nil {
		if err == datastore.ErrNoSuchEntity {
			logger.Warnf("error during getting example graph by identifier, err: %s", err.Error())
			return "", err
		}
		logger.Errorf("error during getting example graph by identifier, err: %s", err.Error())
		return "", err
	}
	return pcObj.Content, nil
}

func (d *Datastore) deleteSnippets(ctx context.Context, snippetQuery *datastore.Query, skipKey *datastore.Key) error {
	snippetQuery = snippetQuery.
		Project("numberOfFiles")
	var snpDtos []*dto.SnippetDeleteDTO
	snpKeys, err := d.Client.GetAll(ctx, snippetQuery, &snpDtos)
	if err != nil {
		logger.Errorf("Datastore: deleteSnippets(): query snippets, err: %s\n", err.Error())
		return err
	}
	logger.Debugf("deleting %d unused snippets: %v", len(snpKeys), snpKeys)
	for snpIndex, snpKey := range snpKeys {
		if snpKey != nil && skipKey != nil && *snpKey == *skipKey {
			logger.Debugf("skipping the current snippet %v", snpKey)
			continue
		}
		var fileKeys []*datastore.Key
		for fileIndex := 0; fileIndex < snpDtos[snpIndex].NumberOfFiles; fileIndex++ {
			fileKeys = append(fileKeys, utils.GetFileKey(ctx, snpKey.Name, fileIndex))
		}
		// delete snippet & its artifacts in a transaction
		_, err = d.Client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
			err = tx.DeleteMulti(fileKeys)
			err = tx.Delete(snpKey)
			return err
		})
		if err != nil {
			logger.Errorf("Datastore: deleteSnippets(): error deleting unused snippet, err: %s\n", err.Error())
			return err
		}
	}
	return nil
}

//DeleteUnusedSnippets deletes all unused snippets
func (d *Datastore) DeleteUnusedSnippets(ctx context.Context, dayDiff int32) error {
	var hoursDiff = dayDiff * 24
	boundaryDate := time.Now().Add(-time.Hour * time.Duration(hoursDiff))
	snippetQuery := datastore.NewQuery(constants.SnippetKind).
		Namespace(utils.GetNamespace(ctx)).
		FilterField("lVisited", "<=", boundaryDate).
		FilterField("origin", "=", constants.UserSnippetOrigin)

	return d.deleteSnippets(ctx, snippetQuery, nil)
}

func rollback(tx *datastore.Transaction) {
	err := tx.Rollback()
	if err != nil {
		logger.Errorf(errorMsgTemplateTxRollback, err.Error())
	}
}

func getEntities[V entity.DatastoreEntity](tx *datastore.Transaction, keys []*datastore.Key) ([]*V, error) {
	var entitiesWithNils = make([]*V, len(keys))
	entities := make([]*V, 0)
	if err := tx.GetMulti(keys, entitiesWithNils); err != nil {
		if errorsVal, ok := err.(datastore.MultiError); ok {
			for _, errVal := range errorsVal {
				if errors.Is(datastore.ErrNoSuchEntity, errVal) {
					for _, entityVal := range entitiesWithNils {
						if entityVal != nil {
							entities = append(entities, entityVal)
						}
					}
					break
				}
			}
		} else {
			logger.Errorf("error during the getting entities, err: %s\n", err.Error())
			return nil, err
		}
	} else {
		entities = entitiesWithNils
	}
	return entities, nil
}
