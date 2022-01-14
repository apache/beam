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

package cloud_bucket

import (
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/logger"
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	BucketName       = "playground-precompiled-objects"
	OutputExtension  = "output"
	LogsExtension    = "log"
	MetaInfoName     = "meta.info"
	Timeout          = time.Second * 10
	javaExtension    = "java"
	goExtension      = "go"
	pyExtension      = "py"
	scioExtension    = "scala"
	separatorsNumber = 2
)

type ObjectInfo struct {
	Name            string
	CloudPath       string
	Description     string                   `protobuf:"bytes,3,opt,name=description,proto3" json:"description,omitempty"`
	Type            pb.PrecompiledObjectType `protobuf:"varint,4,opt,name=type,proto3,enum=api.v1.PrecompiledObjectType" json:"type,omitempty"`
	Categories      []string                 `json:"categories,omitempty"`
	PipelineOptions string                   `protobuf:"bytes,3,opt,name=pipeline_options,proto3" json:"pipeline_options,omitempty"`
}

type PrecompiledObjects []ObjectInfo
type CategoryToPrecompiledObjects map[string]PrecompiledObjects
type SdkToCategories map[string]CategoryToPrecompiledObjects

// CloudStorage represents working tools for getting compiled and
// run beam examples from Google Cloud Storage. It is required that
// the bucket where examples are stored would be public,
// and it has a specific structure of files, namely:
// SDK_JAVA/
// --------MinimalWordCount/
// ----------- MinimalWordCount.java
// ----------- MinimalWordCount.output
// ----------- meta.info
// --------JoinExamples/
// ----------- JoinExamples.java
// ----------- JoinExamples.output
// ----------- meta.info
// ----  ...
// SDK_GO/
// --------MinimalWordCount/
// ----------- MinimalWordCount.go
// ----------- MinimalWordCount.output
// ----------- meta.info
// --------PingPong/
// ----  ...
// ...
// meta.info is a json file that has the following fields:
// {
//	"description": "Description of an example",
//	"type": 1, ## 1 - Example, 2 - Kata, 3 - Unit-test
//	"categories": ["Common", "IO"]
// }
//
type CloudStorage struct {
}

func New() *CloudStorage {
	return &CloudStorage{}
}

// GetPrecompiledObjectCode returns the source code of the example
func (cd *CloudStorage) GetPrecompiledObjectCode(ctx context.Context, precompiledObjectPath string) (string, error) {
	extension, err := getFileExtensionBySdk(precompiledObjectPath)
	if err != nil {
		return "", err
	}
	data, err := cd.getFileFromBucket(ctx, precompiledObjectPath, extension)
	if err != nil {
		return "", err
	}
	result := string(data)
	return result, nil
}

// GetPrecompiledObjectOutput returns the run output of the example
func (cd *CloudStorage) GetPrecompiledObjectOutput(ctx context.Context, precompiledObjectPath string) (string, error) {
	data, err := cd.getFileFromBucket(ctx, precompiledObjectPath, OutputExtension)
	if err != nil {
		return "", err
	}
	result := string(data)
	return result, nil
}

// GetPrecompiledObjectLogs returns the logs of the example
func (cd *CloudStorage) GetPrecompiledObjectLogs(ctx context.Context, precompiledObjectPath string) (string, error) {
	data, err := cd.getFileFromBucket(ctx, precompiledObjectPath, LogsExtension)
	if err != nil {
		return "", err
	}
	result := string(data)
	return result, nil
}

// GetPrecompiledObjects returns stored at the cloud storage bucket precompiled objects for the target category
func (cd *CloudStorage) GetPrecompiledObjects(ctx context.Context, targetSdk pb.Sdk, targetCategory string) (*SdkToCategories, error) {
	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		return nil, fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()

	precompiledObjects := make(SdkToCategories, 0)
	bucket := client.Bucket(BucketName)

	dirs, err := cd.getPrecompiledObjectsDirs(ctx, targetSdk, bucket)
	if err != nil {
		return nil, err
	}
	for objectDir := range dirs {
		infoPath := filepath.Join(objectDir, MetaInfoName) // helping file with information about this object
		rc, err := bucket.Object(infoPath).NewReader(ctx)
		if err != nil {
			logger.Errorf("Object(%q).NewReader: %v", infoPath, err.Error())
			continue
		}
		data, err := ioutil.ReadAll(rc)
		if err != nil {
			logger.Errorf("ioutil.ReadAll: %v", err.Error())
			continue
		}
		precompiledObject := ObjectInfo{}
		err = json.Unmarshal(data, &precompiledObject)
		if err != nil {
			logger.Errorf("json.Unmarshal: %v", err.Error())
			continue
		}
		for _, objectCategory := range precompiledObject.Categories {
			if targetCategory == "" || targetCategory == objectCategory { //take only requested categories
				appendPrecompiledObject(precompiledObject, &precompiledObjects, objectDir, objectCategory)
			}
		}
		rc.Close()
	}
	return &precompiledObjects, nil
}

// GetDefaultPrecompiledObject returns the default precompiled object for the sdk
func (cd *CloudStorage) GetDefaultPrecompiledObject(ctx context.Context, defaultExamplePath string) (*ObjectInfo, error) {
	infoPath := filepath.Join(defaultExamplePath, MetaInfoName)
	metaInfo, err := cd.getFileFromBucket(ctx, infoPath, "")
	if err != nil {
		return nil, err
	}

	precompiledObject := ObjectInfo{}
	err = json.Unmarshal(metaInfo, &precompiledObject)
	if err != nil {
		logger.Errorf("json.Unmarshal: %v", err.Error())
		return nil, err
	}

	precompiledObject.CloudPath = filepath.Dir(infoPath)

	return &precompiledObject, nil
}

// getPrecompiledObjectsDirs finds directories with precompiled objects
// Since there is no notion of directory at cloud storage, then
// to avoid duplicates of a base path (directory) need to store it in a set/map.
func (cd *CloudStorage) getPrecompiledObjectsDirs(ctx context.Context, targetSdk pb.Sdk, bucket *storage.BucketHandle) (map[string]bool, error) {
	prefix := targetSdk.String()
	if targetSdk == pb.Sdk_SDK_UNSPECIFIED {
		prefix = ""
	}
	it := bucket.Objects(ctx, &storage.Query{
		Prefix: prefix,
	})
	objectDirs := make(map[string]bool, 0)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("Bucket(%q).Objects: %v", BucketName, err)
		}
		path := attrs.Name
		if isPathToPrecompiledObjectFile(path) {
			objectDirs[filepath.Dir(path)] = true //save base path (directory) of a file
		}
	}
	return objectDirs, nil
}

// appendPrecompiledObject add precompiled object to the common structure of precompiled objects
func appendPrecompiledObject(objectInfo ObjectInfo, sdkToCategories *SdkToCategories, pathToObject string, categoryName string) {
	sdkName := getSdkName(pathToObject)
	categoryToPrecompiledObjects, ok := (*sdkToCategories)[sdkName]
	if !ok {
		(*sdkToCategories)[sdkName] = make(CategoryToPrecompiledObjects, 0)
		categoryToPrecompiledObjects = (*sdkToCategories)[sdkName]
	}
	objects, ok := categoryToPrecompiledObjects[categoryName]
	if !ok {
		categoryToPrecompiledObjects[categoryName] = make(PrecompiledObjects, 0)
		objects = categoryToPrecompiledObjects[categoryName]
	}
	objectInfo.CloudPath = pathToObject
	objectInfo.Name = filepath.Base(pathToObject)
	categoryToPrecompiledObjects[categoryName] = append(objects, objectInfo)
}

// getFileFromBucket receives the file from the bucket by its name
func (cd *CloudStorage) getFileFromBucket(ctx context.Context, pathToObject string, extension string) ([]byte, error) {
	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		return nil, fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()

	bucket := client.Bucket(BucketName)

	filePath := getFullFilePath(pathToObject, extension)
	rc, err := bucket.Object(filePath).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("Object(%q).NewReader: %v", filePath, err)
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("ioutil.ReadAll: %v", err)
	}
	return data, nil
}

// getFileExtensionBySdk get extension of the file with code by the sdk name
func getFileExtensionBySdk(precompiledObjectPath string) (string, error) {
	sdk := strings.Split(precompiledObjectPath, string(os.PathSeparator))[0]
	var extension string
	switch sdk {
	case pb.Sdk_SDK_JAVA.String():
		extension = javaExtension
	case pb.Sdk_SDK_PYTHON.String():
		extension = pyExtension
	case pb.Sdk_SDK_GO.String():
		extension = goExtension
	case pb.Sdk_SDK_SCIO.String():
		extension = scioExtension
	default:
		return "", fmt.Errorf("")
	}
	return extension, nil
}

// getFullFilePath get full path to the precompiled object file
func getFullFilePath(objectDir string, extension string) string {
	if extension == "" {
		return objectDir
	}
	precompiledObjectName := filepath.Base(objectDir) //the base of the object's directory matches the name of the file
	fileName := strings.Join([]string{precompiledObjectName, extension}, ".")
	filePath := filepath.Join(objectDir, fileName)
	return filePath
}

// isPathToPrecompiledObjectFile is it a path where precompiled object is stored (i.e. SDK/ObjectName/ObjectCode.sdkExtension)
func isPathToPrecompiledObjectFile(path string) bool {
	return strings.Count(path, string(os.PathSeparator)) == separatorsNumber && !isDir(path)
}

// isDir checks whether the path imitates directory
func isDir(path string) bool {
	return path[len(path)-1] == os.PathSeparator
}

// getSdkName gets category and sdk from the filepath
func getSdkName(path string) string {
	sdkName := strings.Split(path, string(os.PathSeparator))[0] // the path of the form "sdkName/example/", where the first part is sdkName
	return sdkName
}
