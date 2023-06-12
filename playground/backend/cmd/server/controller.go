// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"github.com/google/uuid"
	"strconv"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/cache"
	"beam.apache.org/playground/backend/internal/code_processing"
	"beam.apache.org/playground/backend/internal/components"
	"beam.apache.org/playground/backend/internal/db"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/db/mapper"
	"beam.apache.org/playground/backend/internal/emulators"
	"beam.apache.org/playground/backend/internal/environment"
	cerrors "beam.apache.org/playground/backend/internal/errors"
	"beam.apache.org/playground/backend/internal/logger"
	"beam.apache.org/playground/backend/internal/setup_tools/life_cycle"
	"beam.apache.org/playground/backend/internal/utils"
)

const (
	errorTitleGetSnippet        = "Error during getting snippet"
	errorTitleSaveSnippet       = "Error during saving snippet"
	errorTitleGetCatalog        = "Error during getting catalog"
	errorTitleGetExample        = "Error during getting example"
	errorTitleGetExampleCode    = "Error during getting example code"
	errorTitleGetExampleOutput  = "Error during getting example output"
	errorTitleGetExampleLogs    = "Error during getting example logs"
	errorTitleGetExampleGraph   = "Error during getting example graph"
	errorTitleGetDefaultExample = "Error during getting default example"
	errorTitleRunCode           = "Error during run code"

	userCloudConnectionErrMsg  = "Cloud connection error"
	resourceNotFoundErrMsg     = "Resource is not found"
	resourceInconsistentErrMsg = "Resource is not consistent"
)

// playgroundController processes `gRPC' requests from clients.
// Contains methods to process receiving code, monitor current status of code processing and receive compile/run output.
type playgroundController struct {
	env          *environment.Environment
	cacheService cache.Cache
	// Database setup only if the server doesn't suppose to run code, i.e. SDK is unspecified
	db             db.Database
	props          *environment.Properties
	entityMapper   mapper.EntityMapper
	cacheComponent *components.CacheComponent

	pb.UnimplementedPlaygroundServiceServer
}

// RunCode is running code from requests using a particular SDK
//   - In case of incorrect sdk returns codes.InvalidArgument
//   - In case of exceeded number of parallel jobs returns codes.ResourceExhausted
//   - In case of error during preparing files/folders returns codes.Internal
//   - In case of no errors saves playground.Status_STATUS_EXECUTING as cache.Status into cache and sets expiration time
//     for all cache values which will be saved into cache during processing received code.
//     Returns id of code processing (pipelineId)
func (controller *playgroundController) RunCode(ctx context.Context, info *pb.RunCodeRequest) (*pb.RunCodeResponse, error) {
	// check if we can take a new RunCode request
	if !utils.CheckNumOfTheParallelJobs(controller.env.ApplicationEnvs.WorkingDir(), controller.env.BeamSdkEnvs.NumOfParallelJobs()) {
		logger.Warnf("RunCode(): number of parallel jobs is exceeded\n")
		return nil, cerrors.ResourceExhaustedError("Error during preparing", "Number of parallel jobs is exceeded")
	}
	// check for correct sdk
	if info.Sdk != controller.env.BeamSdkEnvs.ApacheBeamSdk {
		logger.Errorf("RunCode(): request contains incorrect sdk: %s\n", info.Sdk)
		return nil, cerrors.InvalidArgumentError("Error during preparing", "Incorrect sdk. Want to receive %s, but the request contains %s", controller.env.BeamSdkEnvs.ApacheBeamSdk.String(), info.Sdk.String())
	}
	switch info.Sdk {
	case pb.Sdk_SDK_UNSPECIFIED:
		logger.Errorf("RunCode(): unimplemented sdk: %s\n", info.Sdk)
		return nil, cerrors.InvalidArgumentError("Error during preparing", "Sdk is not implemented yet: %s", info.Sdk.String())
	}

	cacheExpirationTime := controller.env.ApplicationEnvs.CacheEnvs().KeyExpirationTime()
	pipelineId := uuid.New()

	sources := make([]entity.FileEntity, 0)
	if len(info.Files) > 0 {
		for _, file := range info.Files {
			sources = append(sources, entity.FileEntity{
				Name:     file.Name,
				Content:  file.Content,
				IsMain:   file.IsMain,
				CntxLine: 1,
			})
		}
	} else {
		fileName, err := utils.GetFileName("", info.Code, info.Sdk)
		if err != nil {
			return nil, cerrors.InternalError(errorTitleRunCode, "Failed to get default filename")
		}
		sources = append(sources, entity.FileEntity{
			Name:     fileName,
			Content:  info.Code,
			IsMain:   true,
			CntxLine: 1,
		})
	}

	emulatorConfiguration := emulators.EmulatorConfiguration{
		Datasets:                    info.Datasets,
		DatasetsPath:                controller.env.ApplicationEnvs.DatasetsPath(),
		KafkaEmulatorExecutablePath: controller.env.ApplicationEnvs.KafkaExecutablePath(),
	}

	lc, err := life_cycle.Setup(info.Sdk, sources, pipelineId, controller.env.ApplicationEnvs.WorkingDir(), controller.env.ApplicationEnvs.PipelinesFolder(), controller.env.BeamSdkEnvs.PreparedModDir(), emulatorConfiguration)
	if err != nil {
		logger.Errorf("RunCode(): error during setup file system: %s\n", err.Error())
		return nil, cerrors.InternalError("Error during preparing", "Error during setup file system for the code processing: %s", err.Error())
	}

	if err = utils.SetToCache(controller.cacheService, pipelineId, cache.Status, pb.Status_STATUS_VALIDATING); err != nil {
		code_processing.DeleteResources(pipelineId, lc)
		return nil, cerrors.InternalError("Error during preparing", "Error during saving status of the code processing")
	}
	if err = utils.SetToCache(controller.cacheService, pipelineId, cache.RunOutputIndex, 0); err != nil {
		code_processing.DeleteResources(pipelineId, lc)
		return nil, cerrors.InternalError("Error during preparing", "Error during saving initial run output")
	}
	if err = utils.SetToCache(controller.cacheService, pipelineId, cache.LogsIndex, 0); err != nil {
		code_processing.DeleteResources(pipelineId, lc)
		return nil, cerrors.InternalError("Error during preparing", "Error during saving value for the logs output")
	}
	if err = utils.SetToCache(controller.cacheService, pipelineId, cache.Canceled, false); err != nil {
		code_processing.DeleteResources(pipelineId, lc)
		return nil, cerrors.InternalError("Error during preparing", "Error during saving initial cancel flag")
	}
	if err = controller.cacheService.SetExpTime(ctx, pipelineId, cacheExpirationTime); err != nil {
		logger.Errorf("%s: RunCode(): cache.SetExpTime(): %s\n", pipelineId, err.Error())
		code_processing.DeleteResources(pipelineId, lc)
		return nil, cerrors.InternalError("Error during preparing", "Internal error")
	}

	go code_processing.Process(context.Background(), controller.cacheService, lc, pipelineId, &controller.env.ApplicationEnvs, &controller.env.BeamSdkEnvs, info.PipelineOptions)

	pipelineInfo := pb.RunCodeResponse{PipelineUuid: pipelineId.String()}
	return &pipelineInfo, nil
}

// CheckStatus is checking status for the specific pipeline by PipelineUuid
func (controller *playgroundController) CheckStatus(ctx context.Context, info *pb.CheckStatusRequest) (*pb.CheckStatusResponse, error) {
	pipelineId, err := uuid.Parse(info.PipelineUuid)
	errorMessage := "Error during getting status of the code processing"
	if err != nil {
		logger.Errorf("%s: CheckStatus(): pipelineId has incorrect value and couldn't be parsed as uuid value: %s", info.PipelineUuid, err.Error())
		return nil, cerrors.InvalidArgumentError(errorMessage, "pipelineId has incorrect value and couldn't be parsed as uuid value: %s", info.PipelineUuid)
	}
	status, err := code_processing.GetProcessingStatus(ctx, controller.cacheService, pipelineId, errorMessage)
	if err != nil {
		return nil, err
	}
	return &pb.CheckStatusResponse{Status: status}, nil
}

// GetRunOutput is returning output of execution for specific pipeline by PipelineUuid
func (controller *playgroundController) GetRunOutput(ctx context.Context, info *pb.GetRunOutputRequest) (*pb.GetRunOutputResponse, error) {
	pipelineId, err := uuid.Parse(info.PipelineUuid)
	errorMessage := "Error during getting run output of the code processing"
	if err != nil {
		logger.Errorf("%s: GetRunOutput(): pipelineId has incorrect value and couldn't be parsed as uuid value: %s", info.PipelineUuid, err.Error())
		return nil, cerrors.InvalidArgumentError(errorMessage, "pipelineId has incorrect value and couldn't be parsed as uuid value: %s", info.PipelineUuid)
	}
	lastIndex, err := code_processing.GetLastIndex(ctx, controller.cacheService, pipelineId, cache.RunOutputIndex, errorMessage)
	if err != nil {
		return nil, err
	}
	runOutput, err := code_processing.GetProcessingOutput(ctx, controller.cacheService, pipelineId, cache.RunOutput, errorMessage)
	if err != nil {
		return nil, err
	}
	newRunOutput := ""
	if len(runOutput) > lastIndex {
		newRunOutput = runOutput[lastIndex:]
		if err := utils.SetToCache(controller.cacheService, pipelineId, cache.RunOutputIndex, lastIndex+len(newRunOutput)); err != nil {
			return nil, cerrors.InternalError(errorMessage, "Error during saving pagination value")
		}
	}

	pipelineResult := pb.GetRunOutputResponse{Output: newRunOutput}

	return &pipelineResult, nil
}

// GetLogs is returning logs of execution for specific pipeline by PipelineUuid
func (controller *playgroundController) GetLogs(ctx context.Context, info *pb.GetLogsRequest) (*pb.GetLogsResponse, error) {
	errorTitle := utils.GetFuncName(controller.GetRunOutput)
	pipelineId, err := uuid.Parse(info.PipelineUuid)
	errorMessage := "Error during getting logs of the code processing"
	if err != nil {
		logger.Errorf("%s: %s: pipelineId has incorrect value and couldn't be parsed as uuid value: %s", info.PipelineUuid, errorTitle, err.Error())
		return nil, cerrors.InvalidArgumentError(errorMessage, "pipelineId has incorrect value and couldn't be parsed as uuid value: %s", info.PipelineUuid)
	}
	lastIndex, err := code_processing.GetLastIndex(ctx, controller.cacheService, pipelineId, cache.LogsIndex, errorMessage)
	if err != nil {
		return nil, err
	}
	logs, err := code_processing.GetProcessingOutput(ctx, controller.cacheService, pipelineId, cache.Logs, errorMessage)
	if err != nil {
		return nil, err
	}
	newLogs := ""
	if len(logs) > lastIndex {
		newLogs = logs[lastIndex:]
		if err := utils.SetToCache(controller.cacheService, pipelineId, cache.LogsIndex, lastIndex+len(newLogs)); err != nil {
			return nil, cerrors.InternalError(errorMessage, "Error during saving pagination value")
		}
	}

	pipelineResult := pb.GetLogsResponse{Output: newLogs}

	return &pipelineResult, nil
}

// GetRunError is returning error output of execution for specific pipeline by PipelineUuid
func (controller *playgroundController) GetRunError(ctx context.Context, info *pb.GetRunErrorRequest) (*pb.GetRunErrorResponse, error) {
	pipelineId, err := uuid.Parse(info.PipelineUuid)
	errorMessage := "Error during getting error output of the code processing"
	if err != nil {
		logger.Errorf("%s: GetRunError(): pipelineId has incorrect value and couldn't be parsed as uuid value: %s", info.PipelineUuid, err.Error())
		return nil, cerrors.InvalidArgumentError(errorMessage, "pipelineId has incorrect value and couldn't be parsed as uuid value: %s", info.PipelineUuid)
	}
	runError, err := code_processing.GetProcessingOutput(ctx, controller.cacheService, pipelineId, cache.RunError, errorMessage)
	if err != nil {
		return nil, err
	}
	return &pb.GetRunErrorResponse{Output: runError}, nil
}

// GetValidationOutput is returning output of validation for specific pipeline by PipelineUuid
func (controller *playgroundController) GetValidationOutput(ctx context.Context, info *pb.GetValidationOutputRequest) (*pb.GetValidationOutputResponse, error) {
	pipelineId, err := uuid.Parse(info.PipelineUuid)
	errorMessage := "Error during getting compilation output"
	if err != nil {
		logger.Errorf("%s: GetValidationOutput(): pipelineId has incorrect value and couldn't be parsed as uuid value: %s", info.PipelineUuid, err.Error())
		return nil, cerrors.InvalidArgumentError(errorMessage, "pipelineId has incorrect value and couldn't be parsed as uuid value: %s", info.PipelineUuid)
	}
	validationOutput, err := code_processing.GetProcessingOutput(ctx, controller.cacheService, pipelineId, cache.ValidationOutput, errorMessage)
	if err != nil {
		return nil, err
	}
	return &pb.GetValidationOutputResponse{Output: validationOutput}, nil
}

// GetPreparationOutput is returning output of prepare step for specific pipeline by PipelineUuid
func (controller *playgroundController) GetPreparationOutput(ctx context.Context, info *pb.GetPreparationOutputRequest) (*pb.GetPreparationOutputResponse, error) {
	pipelineId, err := uuid.Parse(info.PipelineUuid)
	errorMessage := "Error during getting compilation output"
	if err != nil {
		logger.Errorf("%s: GetPreparationOutput(): pipelineId has incorrect value and couldn't be parsed as uuid value: %s", info.PipelineUuid, err.Error())
		return nil, cerrors.InvalidArgumentError(errorMessage, "pipelineId has incorrect value and couldn't be parsed as uuid value: %s", info.PipelineUuid)
	}
	preparationOutput, err := code_processing.GetProcessingOutput(ctx, controller.cacheService, pipelineId, cache.PreparationOutput, errorMessage)
	if err != nil {
		return nil, err
	}
	return &pb.GetPreparationOutputResponse{Output: preparationOutput}, nil
}

// GetCompileOutput is returning output of compilation for specific pipeline by PipelineUuid
func (controller *playgroundController) GetCompileOutput(ctx context.Context, info *pb.GetCompileOutputRequest) (*pb.GetCompileOutputResponse, error) {
	pipelineId, err := uuid.Parse(info.PipelineUuid)
	errorMessage := "Error during getting compilation output"
	if err != nil {
		logger.Errorf("%s: GetCompileOutput(): pipelineId has incorrect value and couldn't be parsed as uuid value: %s", info.PipelineUuid, err.Error())
		return nil, cerrors.InvalidArgumentError(errorMessage, "pipelineId has incorrect value and couldn't be parsed as uuid value: %s", info.PipelineUuid)
	}
	compileOutput, err := code_processing.GetProcessingOutput(ctx, controller.cacheService, pipelineId, cache.CompileOutput, errorMessage)
	if err != nil {
		return nil, err
	}
	return &pb.GetCompileOutputResponse{Output: compileOutput}, nil
}

// GetGraph is returning graph of execution for specific pipeline by PipelineUuid
func (controller *playgroundController) GetGraph(ctx context.Context, info *pb.GetGraphRequest) (*pb.GetGraphResponse, error) {
	pipelineId, err := uuid.Parse(info.PipelineUuid)
	errorMessage := "Error during getting graph output"
	if err != nil {
		logger.Errorf("%s: GetGraph(): pipelineId has incorrect value and couldn't be parsed as uuid value: %s", info.PipelineUuid, err.Error())
		return nil, cerrors.InvalidArgumentError(errorMessage, "pipelineId has incorrect value and couldn't be parsed as uuid value: %s", info.PipelineUuid)
	}
	graph, err := code_processing.GetGraph(ctx, controller.cacheService, pipelineId, errorMessage)
	if err != nil {
		return nil, err
	}
	return &pb.GetGraphResponse{Graph: graph}, nil
}

// Cancel is setting cancel flag to stop code processing
func (controller *playgroundController) Cancel(ctx context.Context, info *pb.CancelRequest) (*pb.CancelResponse, error) {
	pipelineId, err := uuid.Parse(info.PipelineUuid)
	errorMessage := "Error during canceling the code processing"
	if err != nil {
		logger.Errorf("%s: Cancel(): pipelineId has incorrect value and couldn't be parsed as uuid value: %s", info.PipelineUuid, err.Error())
		return nil, cerrors.InvalidArgumentError(errorMessage, "pipelineId has incorrect value and couldn't be parsed as uuid value: %s", info.PipelineUuid)
	}
	if err := utils.SetToCache(controller.cacheService, pipelineId, cache.Canceled, true); err != nil {
		return nil, cerrors.InternalError(errorMessage, "Error during saving cancel flag value")
	}
	return &pb.CancelResponse{}, nil
}

// GetPrecompiledObjects returns the catalog with examples
// Tries to get the whole catalog from the cache
// - If there is no catalog in the cache, gets the catalog from the Datastore and saves it to the cache
// - If SDK or category is specified in the request, gets the catalog from the cache and filters it by SDK and category
func (controller *playgroundController) GetPrecompiledObjects(ctx context.Context, info *pb.GetPrecompiledObjectsRequest) (*pb.GetPrecompiledObjectsResponse, error) {
	if err := controller.verifyRouter(); err != nil {
		logger.Errorf("GetPrecompiledObjects() error: %s", err.Error())
		return nil, err
	}
	catalog, err := controller.cacheComponent.GetCatalogFromCacheOrDatastore(ctx, controller.env.ApplicationEnvs.CacheRequestTimeout())
	if err != nil {
		return nil, cerrors.InternalError(errorTitleGetCatalog, userCloudConnectionErrMsg)
	}
	return &pb.GetPrecompiledObjectsResponse{
		SdkCategories: utils.FilterCatalog(catalog, info.Sdk, info.Category),
	}, nil
}

// GetPrecompiledObject returns precompiled object from the Datastore or the cache
func (controller *playgroundController) GetPrecompiledObject(ctx context.Context, info *pb.GetPrecompiledObjectRequest) (*pb.GetPrecompiledObjectResponse, error) {
	if err := controller.verifyRouter(); err != nil {
		logger.Errorf("GetPrecompiledObject() error: %s", err.Error())
		return nil, err
	}
	exampleId := info.GetCloudPath()
	sdks, err := controller.cacheComponent.GetSdkCatalogFromCacheOrDatastore(ctx, controller.env.ApplicationEnvs.CacheRequestTimeout())
	if err != nil {
		return nil, cerrors.InternalError(errorTitleGetExample, userCloudConnectionErrMsg)
	}
	precompiledObject, err := controller.db.GetExample(ctx, exampleId, sdks)
	if err != nil {
		switch err {
		case datastore.ErrNoSuchEntity:
			return nil, cerrors.NotFoundError(errorTitleGetExample, resourceNotFoundErrMsg)
		default:
			return nil, cerrors.InternalError(errorTitleGetExample, userCloudConnectionErrMsg)
		}
	}
	return &pb.GetPrecompiledObjectResponse{PrecompiledObject: precompiledObject}, nil
}

// GetPrecompiledObjectCode returns the code of the specific example
func (controller *playgroundController) GetPrecompiledObjectCode(ctx context.Context, info *pb.GetPrecompiledObjectCodeRequest) (*pb.GetPrecompiledObjectCodeResponse, error) {
	if err := controller.verifyRouter(); err != nil {
		logger.Errorf("GetPrecompiledObjectCode() error: %s", err.Error())
		return nil, err
	}
	exampleId := info.GetCloudPath()
	files, err := controller.db.GetExampleCode(ctx, exampleId)
	if err != nil {
		switch err {
		case datastore.ErrNoSuchEntity:
			return nil, cerrors.NotFoundError(errorTitleGetExampleCode, resourceNotFoundErrMsg)
		default:
			return nil, cerrors.InternalError(errorTitleGetExampleCode, userCloudConnectionErrMsg)
		}
	}
	if len(files) == 0 {
		return nil, cerrors.NotFoundError(errorTitleGetExampleCode, resourceNotFoundErrMsg)
	}
	response := pb.GetPrecompiledObjectCodeResponse{}
	for _, file := range files {
		response.Files = append(response.Files, &pb.SnippetFile{
			Name:    file.Name,
			Content: file.Content,
			IsMain:  file.IsMain,
		})
		if file.IsMain {
			response.Code = file.Content
		}
	}
	if len(response.Files) == 0 || response.Code == "" {
		return nil, cerrors.InternalError(errorTitleGetExampleCode, resourceInconsistentErrMsg)
	}
	return &response, nil
}

// GetPrecompiledObjectOutput returns the output of the compiled and run example
func (controller *playgroundController) GetPrecompiledObjectOutput(ctx context.Context, info *pb.GetPrecompiledObjectOutputRequest) (*pb.GetPrecompiledObjectOutputResponse, error) {
	if err := controller.verifyRouter(); err != nil {
		logger.Errorf("GetPrecompiledObjectOutput() error: %s", err.Error())
		return nil, err
	}
	exampleId := info.GetCloudPath()
	output, err := controller.db.GetExampleOutput(ctx, exampleId)
	if err != nil {
		switch err {
		case datastore.ErrNoSuchEntity:
			return nil, cerrors.NotFoundError(errorTitleGetExampleOutput, resourceNotFoundErrMsg)
		default:
			return nil, cerrors.InternalError(errorTitleGetExampleOutput, userCloudConnectionErrMsg)
		}
	}
	response := pb.GetPrecompiledObjectOutputResponse{Output: output}
	return &response, nil
}

// GetPrecompiledObjectLogs returns the logs of the compiled and run example
func (controller *playgroundController) GetPrecompiledObjectLogs(ctx context.Context, info *pb.GetPrecompiledObjectLogsRequest) (*pb.GetPrecompiledObjectLogsResponse, error) {
	if err := controller.verifyRouter(); err != nil {
		logger.Errorf("GetPrecompiledObjectLogs() error: %s", err.Error())
		return nil, err
	}
	exampleId := info.GetCloudPath()
	logs, err := controller.db.GetExampleLogs(ctx, exampleId)
	if err != nil {
		switch err {
		case datastore.ErrNoSuchEntity:
			return nil, cerrors.NotFoundError(errorTitleGetExampleLogs, resourceNotFoundErrMsg)
		default:
			return nil, cerrors.InternalError(errorTitleGetExampleLogs, userCloudConnectionErrMsg)
		}
	}
	response := pb.GetPrecompiledObjectLogsResponse{Output: logs}
	return &response, nil
}

// GetPrecompiledObjectGraph returns the graph of the compiled and run example
func (controller *playgroundController) GetPrecompiledObjectGraph(ctx context.Context, info *pb.GetPrecompiledObjectGraphRequest) (*pb.GetPrecompiledObjectGraphResponse, error) {
	if err := controller.verifyRouter(); err != nil {
		logger.Errorf("GetPrecompiledObjectGraph() error: %s", err.Error())
		return nil, err
	}
	exampleId := info.GetCloudPath()
	graph, err := controller.db.GetExampleGraph(ctx, exampleId)
	if err != nil {
		switch err {
		case datastore.ErrNoSuchEntity:
			return nil, cerrors.NotFoundError(errorTitleGetExampleGraph, resourceNotFoundErrMsg)
		default:
			return nil, cerrors.InternalError(errorTitleGetExampleGraph, userCloudConnectionErrMsg)
		}
	}
	response := pb.GetPrecompiledObjectGraphResponse{Graph: graph}
	return &response, nil
}

// GetDefaultPrecompiledObject returns the default precompile object for sdk.
func (controller *playgroundController) GetDefaultPrecompiledObject(ctx context.Context, info *pb.GetDefaultPrecompiledObjectRequest) (*pb.GetDefaultPrecompiledObjectResponse, error) {
	if err := controller.verifyRouter(); err != nil {
		logger.Errorf("GetDefaultPrecompiledObject() error: %s", err.Error())
		return nil, err
	}
	switch info.Sdk {
	case pb.Sdk_SDK_UNSPECIFIED:
		logger.Errorf("GetDefaultPrecompiledObject(): unimplemented sdk: %s\n", info.Sdk)
		return nil, cerrors.InvalidArgumentError(errorTitleGetDefaultExample, "Sdk is not implemented yet: %s", info.Sdk.String())
	}
	precompiledObject, err := controller.cacheComponent.GetDefaultPrecompiledObjectFromCacheOrDatastore(ctx, info.Sdk, controller.env.ApplicationEnvs.CacheRequestTimeout())
	if err != nil {
		logger.Errorf("GetDefaultPrecompiledObject(): error during getting catalog: %s", err.Error())
		return nil, cerrors.InternalError(errorTitleGetDefaultExample, userCloudConnectionErrMsg)
	}
	response := pb.GetDefaultPrecompiledObjectResponse{PrecompiledObject: precompiledObject}
	return &response, nil
}

// SaveSnippet returns the generated ID
func (controller *playgroundController) SaveSnippet(ctx context.Context, req *pb.SaveSnippetRequest) (*pb.SaveSnippetResponse, error) {
	if err := controller.verifyRouter(); err != nil {
		logger.Errorf("SaveSnippet() error: %s", err.Error())
		return nil, err
	}
	if req.Sdk == pb.Sdk_SDK_UNSPECIFIED {
		logger.Errorf("SaveSnippet(): unimplemented sdk: %s\n", req.Sdk)
		return nil, cerrors.InvalidArgumentError(errorTitleSaveSnippet, "Sdk is not implemented yet: %s", req.Sdk.String())
	}
	if controller.db == nil {
		logger.Error("SaveSnippet(): the runner is trying to save the snippet")
		return nil, cerrors.InvalidArgumentError(errorTitleSaveSnippet, "The runner doesn't support snippets")
	}
	if req.Files == nil || len(req.Files) == 0 {
		logger.Error("SaveSnippet(): files are empty")
		return nil, cerrors.InvalidArgumentError(errorTitleSaveSnippet, "Snippet must have files")
	}
	if req.PersistenceKey > "" {
		logger.Debugf("saving snippet by persistence_key: %v", req.PersistenceKey)
	}

	snippet := controller.entityMapper.ToSnippet(req)

	for _, file := range req.Files {
		if file.Content == "" {
			logger.Error("SaveSnippet(): entity is empty")
			return nil, cerrors.InvalidArgumentError(errorTitleSaveSnippet, "Snippet must have some content")
		}
		maxSnippetSize := controller.props.MaxSnippetSize
		if len(file.Content) > int(maxSnippetSize) {
			logger.Errorf("SaveSnippet(): entity is too large. Max entity size: %d symbols", maxSnippetSize)
			return nil, cerrors.InvalidArgumentError(errorTitleSaveSnippet, "Snippet size is more than %d symbols", maxSnippetSize)
		}
		fileEntity, err := controller.entityMapper.ToFileEntity(req, file)
		if err != nil {
			logger.Errorf("SaveSnippet(): file has wrong properties, err: %s", err.Error())
			return nil, cerrors.InvalidArgumentError(errorTitleSaveSnippet, "File content is invalid")
		}
		snippet.Files = append(snippet.Files, fileEntity)
	}

	id, err := snippet.ID()
	if err != nil {
		logger.Errorf("SaveSnippet(): ID(): error during ID generation: %s", err.Error())
		return nil, cerrors.InternalError(errorTitleSaveSnippet, "Failed to generate ID")
	}
	if err = controller.db.PutSnippet(ctx, id, snippet); err != nil {
		logger.Errorf("SaveSnippet(): PutSnippet(): error during entity saving: %s", err.Error())
		return nil, cerrors.InternalError(errorTitleSaveSnippet, "Failed to save a snippet entity")
	}

	response := pb.SaveSnippetResponse{Id: id}
	return &response, nil
}

// GetSnippet returns the snippet entity
func (controller *playgroundController) GetSnippet(ctx context.Context, info *pb.GetSnippetRequest) (*pb.GetSnippetResponse, error) {
	if err := controller.verifyRouter(); err != nil {
		logger.Errorf("GetSnippet() error: %s", err.Error())
		return nil, err
	}
	if controller.db == nil {
		logger.Error("GetSnippet(): the runner is trying to read the snippet")
		return nil, cerrors.InvalidArgumentError(errorTitleGetSnippet, "The runner doesn't support snippets")
	}
	snippet, err := controller.db.GetSnippet(ctx, info.GetId())
	if err != nil {
		logger.Errorf("GetSnippet(): error during getting the snippet: %s", err.Error())
		if errors.Is(err, datastore.ErrNoSuchEntity) {
			return nil, cerrors.NotFoundError(errorTitleGetSnippet, resourceNotFoundErrMsg)
		}
		return nil, cerrors.InternalError(errorTitleGetSnippet, "Failed to retrieve the snippet")
	}

	response := pb.GetSnippetResponse{
		Sdk:             pb.Sdk(pb.Sdk_value[snippet.Sdk.Name]),
		PipelineOptions: snippet.PipeOpts,
		Complexity:      pb.Complexity(pb.Complexity_value[snippet.Complexity]),
	}
	files, err := controller.db.GetFiles(ctx, info.GetId(), snippet.NumberOfFiles)
	if err != nil {
		logger.Errorf("GetSnippet(): GetFiles(): error during getting files: %s", err.Error())
		return nil, cerrors.InternalError(errorTitleGetSnippet, "Failed to retrieve files")
	}
	for _, file := range files {
		response.Files = append(response.Files, &pb.SnippetFile{
			Name:    file.Name,
			Content: file.Content,
			IsMain:  file.IsMain,
		})
	}
	return &response, nil
}

// GetMetadata returns runner metadata
func (controller *playgroundController) GetMetadata(_ context.Context, _ *pb.GetMetadataRequest) (*pb.GetMetadataResponse, error) {
	commitTimestampInteger, err := strconv.ParseInt(BuildCommitTimestamp, 10, 64)
	if err != nil {
		logger.Warnf("GetMetadata(): failed to parse BuildCommitTimestamp (\"%s\"): %s", BuildCommitTimestamp, err.Error())
		commitTimestampInteger = 0
	}

	response := pb.GetMetadataResponse{
		RunnerSdk:                             controller.env.BeamSdkEnvs.ApacheBeamSdk.String(),
		BuildCommitHash:                       BuildCommitHash,
		BuildCommitTimestampSecondsSinceEpoch: commitTimestampInteger,
		BeamSdkVersion:                        controller.env.BeamSdkEnvs.BeamVersion,
	}

	return &response, nil
}

// verifyRouter verifies that controller is configured to work in router mode
func (controller *playgroundController) verifyRouter() error {
	if controller.env.BeamSdkEnvs.ApacheBeamSdk != pb.Sdk_SDK_UNSPECIFIED {
		return errors.New("server is in runner mode")
	}
	if controller.db == nil {
		return errors.New("no database service")
	}
	if controller.props == nil {
		return errors.New("no properties")
	}
	if controller.entityMapper == nil {
		return errors.New("no entity mapper")
	}
	if controller.cacheComponent == nil {
		return errors.New("no cache component")
	}
	return nil
}
