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

// Package fhirio provides an API for reading and writing resources to Google
// Cloud Healthcare Fhir stores.
package fhirio

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/gcs"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/google/uuid"
)

func init() {
	register.DoFn3x0[context.Context, string, func(string)]((*importFn)(nil))
	register.DoFn4x0[context.Context, string, func(string), func(string)]((*createBatchFilesFn)(nil))
	register.Emitter1[string]()
}

// ContentStructure representation as per:
// https://cloud.google.com/healthcare-api/docs/reference/rest/v1/projects.locations.datasets.fhirStores/import#contentstructure
type ContentStructure int

const (
	// ContentStructureUnspecified is to be used as argument to Import if the content
	// structure is not specified, the default value BUNDLE is used.
	ContentStructureUnspecified ContentStructure = iota

	// ContentStructureBundle is to be used as argument to Import if the source file
	// contains one or more lines of newline-delimited JSON (ndjson). Each line is a
	// bundle that contains one or more resources.
	ContentStructureBundle

	// ContentStructureResource is to be used as argument to Import if the source
	// file contains one or more lines of newline-delimited JSON (ndjson). Each line
	// is a single resource.
	ContentStructureResource
)

func (cs ContentStructure) String() string {
	switch cs {
	case ContentStructureBundle:
		return "BUNDLE"
	case ContentStructureResource:
		return "RESOURCE"
	case ContentStructureUnspecified:
		fallthrough
	default:
		return "CONTENT_STRUCTURE_UNSPECIFIED"
	}
}

type createBatchFilesFn struct {
	fs              filesystem.Interface
	batchFileWriter io.WriteCloser
	batchFilePath   string
	TempLocation    string
}

func (fn *createBatchFilesFn) StartBundle(ctx context.Context, _, _ func(string)) error {
	fs, err := filesystem.New(ctx, fn.TempLocation)
	if err != nil {
		return err
	}
	fn.fs = fs
	fn.batchFilePath = fmt.Sprintf("%s/fhirImportBatch-%v.ndjson", fn.TempLocation, uuid.New())
	log.Infof(ctx, "Opening to write batch file: %v", fn.batchFilePath)
	fn.batchFileWriter, err = fn.fs.OpenWrite(ctx, fn.batchFilePath)
	if err != nil {
		return err
	}
	return nil
}

func (fn *createBatchFilesFn) ProcessElement(ctx context.Context, resource string, _, emitFailedResource func(string)) {
	_, err := fn.batchFileWriter.Write([]byte(resource + "\n"))
	if err != nil {
		log.Warnf(ctx, "Failed to write resource to batch file. Reason: %v", err)
		emitFailedResource(resource)
	}
}

func (fn *createBatchFilesFn) FinishBundle(ctx context.Context, emitBatchFilePath, _ func(string)) {
	fn.batchFileWriter.Close()
	fn.batchFileWriter = nil
	fn.fs.Close()
	fn.fs = nil
	emitBatchFilePath(fn.batchFilePath)
	log.Infof(ctx, "Batch file created: %v", fn.batchFilePath)
}

type importFn struct {
	fnCommonVariables
	operationCounters
	fs                               filesystem.Interface
	batchFilesPath                   []string
	tempBatchDir                     string
	FhirStorePath                    string
	TempLocation, DeadLetterLocation string
	ContentStructure                 ContentStructure
}

func (fn importFn) String() string {
	return "importFn"
}

func (fn *importFn) Setup() {
	fn.fnCommonVariables.setup(fn.String())
	fn.operationCounters.setup(fn.String())
}

func (fn *importFn) StartBundle(ctx context.Context, _ func(string)) error {
	fs, err := filesystem.New(ctx, fn.TempLocation)
	if err != nil {
		return err
	}
	fn.fs = fs
	fn.batchFilesPath = make([]string, 0)
	fn.tempBatchDir = fmt.Sprintf("%s/tmp-%v", fn.TempLocation, uuid.New())
	return nil
}

func (fn *importFn) ProcessElement(ctx context.Context, batchFilePath string, _ func(string)) {
	updatedBatchFilePath := fmt.Sprintf("%s/%s", fn.tempBatchDir, filepath.Base(batchFilePath))
	err := filesystem.Rename(ctx, fn.fs, batchFilePath, updatedBatchFilePath)
	if err != nil {
		updatedBatchFilePath = batchFilePath
		log.Warnf(ctx, "Failed to move %v to temp location. Reason: %v", batchFilePath, err)
	}
	fn.batchFilesPath = append(fn.batchFilesPath, updatedBatchFilePath)
}

func (fn *importFn) FinishBundle(ctx context.Context, emitDeadLetter func(string)) {
	defer func() {
		fn.fs.Close()
		fn.fs = nil
		fn.batchFilesPath = nil
	}()

	importURI := fn.tempBatchDir + "/*.ndjson"
	log.Infof(ctx, "About to begin import operation with importURI: %v", importURI)
	result, err := executeAndRecordLatency(ctx, &fn.latencyMs, func() (operationResults, error) {
		return fn.client.importResources(fn.FhirStorePath, importURI, fn.ContentStructure)
	})
	if err != nil {
		fn.moveToDeadLetterOrRemoveFailedImportBatchFiles(ctx)
		fn.operationCounters.errorCount.Inc(ctx, 1)
		deadLetterMessage := fmt.Sprintf("Failed to import [%v]. Reason: %v", importURI, err)
		log.Warn(ctx, deadLetterMessage)
		emitDeadLetter(deadLetterMessage)
		return
	}

	log.Infof(ctx, "Imported %v. Results: %v", importURI, result)
	fn.operationCounters.successCount.Inc(ctx, 1)
	fn.resourcesSuccessCount.Inc(ctx, result.Successes)
	fn.resourcesErrorCount.Inc(ctx, result.Failures)
	fn.removeTempBatchFiles(ctx)
}

func (fn *importFn) moveToDeadLetterOrRemoveFailedImportBatchFiles(ctx context.Context) {
	if fn.DeadLetterLocation == "" {
		log.Info(ctx, "Deadletter path not provided. Remove failed import batch files instead.")
		fn.removeTempBatchFiles(ctx)
		return
	}

	log.Infof(ctx, "Moving failed import files to Deadletter path: [%v]", fn.DeadLetterLocation)
	for _, p := range fn.batchFilesPath {
		err := filesystem.Rename(ctx, fn.fs, p, fmt.Sprintf("%s/%s", fn.DeadLetterLocation, filepath.Base(p)))
		if err != nil {
			log.Warnf(ctx, "Failed to move failed imported file %v to %v. Reason: %v", p, fn.DeadLetterLocation, err)
		}
	}
}

func (fn *importFn) removeTempBatchFiles(ctx context.Context) {
	for _, p := range fn.batchFilesPath {
		err := fn.fs.(filesystem.Remover).Remove(ctx, p)
		if err != nil {
			log.Warnf(ctx, "Failed to delete temp batch file [%v]. Reason: %v", p, err)
		}
	}
}

// Import consumes FHIR resources as input PCollection<string> and imports them
// into a given Google Cloud Healthcare FHIR store. It does so by creating batch
// files in the provided Google Cloud Storage `tempDir` and importing those files
// to the store through FHIR import API method: https://cloud.google.com/healthcare-api/docs/concepts/fhir-import.
// If `tempDir` is not provided, it falls back to the dataflow temp_location flag.
// Resources that fail to be included in the batch files are included as the
// first output PCollection. In case a batch file fails to be imported, it will
// be moved to the `deadLetterDir` and an error message will be provided in the
// second output PCollection. If `deadLetterDir` is not provided, the failed
// import files will be deleted and be irretrievable, but the error message will
// still be provided.
func Import(s beam.Scope, fhirStorePath, tempDir, deadLetterDir string, contentStructure ContentStructure, resources beam.PCollection) (beam.PCollection, beam.PCollection) {
	s = s.Scope("fhirio.Import")

	if tempDir == "" {
		tempDir = tryFallbackToDataflowTempDirOrPanic()
	}
	tempDir = strings.TrimSuffix(tempDir, "/")
	deadLetterDir = strings.TrimSuffix(deadLetterDir, "/")

	return importResourcesInBatches(s, fhirStorePath, tempDir, deadLetterDir, contentStructure, resources, nil)
}

// This is useful as an entry point for testing because we can provide a fake FHIR store client.
func importResourcesInBatches(s beam.Scope, fhirStorePath, tempDir, deadLetterDir string, contentStructure ContentStructure, resources beam.PCollection, client fhirStoreClient) (beam.PCollection, beam.PCollection) {
	batchFiles, failedResources := beam.ParDo2(s, &createBatchFilesFn{TempLocation: tempDir}, resources)
	failedImportsDeadLetter := beam.ParDo(
		s,
		&importFn{
			fnCommonVariables:  fnCommonVariables{client: client},
			FhirStorePath:      fhirStorePath,
			TempLocation:       tempDir,
			DeadLetterLocation: deadLetterDir,
			ContentStructure:   contentStructure,
		},
		batchFiles,
	)
	return failedResources, failedImportsDeadLetter
}

func tryFallbackToDataflowTempDirOrPanic() string {
	beam.PipelineOptions.LoadOptionsFromFlags(nil)
	if f := beam.PipelineOptions.Get("temp_location"); f != "" {
		return f
	}

	// temp_location is optional, so fallback to staging_location.
	if f := beam.PipelineOptions.Get("staging_location"); f != "" {
		return f
	}
	panic("could not resolve to a temp directory for import batch files")
}
