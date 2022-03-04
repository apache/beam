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
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	OutputExtension              = "output"
	LogsExtension                = "log"
	GraphExtension               = "graph"
	defaultPrecompiledObjectInfo = "defaultPrecompiledObject.info"
	MetaInfoName                 = "meta.info"
	Timeout                      = time.Minute
	javaExtension                = "java"
	goExtension                  = "go"
	pyExtension                  = "py"
	scioExtension                = "scala"
	separatorsNumber             = 3
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

// CloudStorage represents working tools for getting compiled and
// run beam examples from Google Cloud Storage. It is required that
// the bucket where examples are stored would be public,
// and it has a specific structure of files, namely:
// SDK_JAVA/
// ----defaultPrecompiledObject.info
// ----PRECOMPILED_OBJECT_TYPE_EXAMPLE/
// --------MinimalWordCount/
// ----------- MinimalWordCount.java
// ----------- MinimalWordCount.output
// ----------- MinimalWordCount.log
// ----------- MinimalWordCount.graph
// ----------- meta.info
// --------JoinExamples/
// ----------- JoinExamples.java
// ----------- JoinExamples.output
// ----------- JoinExamples.log
// ----------- JoinExamples.graph
// ----------- meta.info
// ----PRECOMPILED_OBJECT_TYPE_KATA/
// --------...
// ----...
// SDK_GO/
// ----defaultPrecompiledObject.info
// ----PRECOMPILED_OBJECT_TYPE_EXAMPLE/
// --------MinimalWordCount/
// ----------- MinimalWordCount.go
// ----------- MinimalWordCount.output
// ----------- MinimalWordCount.log
// ----------- MinimalWordCount.graph
// ----------- meta.info
// --------PingPong/
// ----PRECOMPILED_OBJECT_TYPE_KATA/
// --------...
// ----...
//
// defaultPrecompiledObject.info is a file that contains path to the default example:
// {
//   "SDK_JAVA": "SDK_JAVA/PRECOMPILED_OBJECT_TYPE_EXAMPLE/MinimalWordCount"
// }
//
// meta.info is a json file that has the following fields:
// {
//  "name": "name of the example",
//	"description": "Description of an example",
//  "multifile": false
//	"categories": ["Common", "IO"]
//  "pipeline_options": "--key1 value1",
//  "default_example": false,
//  "context_line": 1,
//  "link": "https://github.com/apache/beam/blob/master/path/to/example"
// }
//
type CloudStorage struct {
}

func New() *CloudStorage {
	return &CloudStorage{}
}

// GetPrecompiledObject returns the precompiled example
func (cd *CloudStorage) GetPrecompiledObject(ctx context.Context, precompiledObjectPath, bucketName string) (*pb.PrecompiledObject, error) {
	cloudPath := filepath.Join(precompiledObjectPath, MetaInfoName)
	data, err := cd.getFileFromBucket(ctx, cloudPath, "", bucketName)
	if err != nil {
		return nil, err
	}
	precompiledObject := &pb.PrecompiledObject{}
	err = json.Unmarshal(data, precompiledObject)
	if err != nil {
		logger.Errorf("json.Unmarshal: %v", err.Error())
		return nil, err
	}
	precompiledObject.CloudPath = precompiledObjectPath
	return precompiledObject, nil
}

// GetPrecompiledObjectCode returns the source code of the example
func (cd *CloudStorage) GetPrecompiledObjectCode(ctx context.Context, precompiledObjectPath, bucketName string) (string, error) {
	extension, err := getFileExtensionBySdk(precompiledObjectPath)
	if err != nil {
		return "", err
	}
	data, err := cd.getFileFromBucket(ctx, precompiledObjectPath, extension, bucketName)
	if err != nil {
		return "", err
	}
	result := string(data)
	return result, nil
}

// GetPrecompiledObjectOutput returns the run output of the example
func (cd *CloudStorage) GetPrecompiledObjectOutput(ctx context.Context, precompiledObjectPath, bucketName string) (string, error) {
	data, err := cd.getFileFromBucket(ctx, precompiledObjectPath, OutputExtension, bucketName)
	if err != nil {
		return "", err
	}
	result := string(data)
	return result, nil
}

// GetPrecompiledObjectLogs returns the logs of the example
func (cd *CloudStorage) GetPrecompiledObjectLogs(ctx context.Context, precompiledObjectPath, bucketName string) (string, error) {
	data, err := cd.getFileFromBucket(ctx, precompiledObjectPath, LogsExtension, bucketName)
	if err != nil {
		return "", err
	}
	result := string(data)
	return result, nil
}

// GetPrecompiledObjectGraph returns the graph of the example
func (cd *CloudStorage) GetPrecompiledObjectGraph(ctx context.Context, precompiledObjectPath, bucketName string) (string, error) {
	data, err := cd.getFileFromBucket(ctx, precompiledObjectPath, GraphExtension, bucketName)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// GetPrecompiledObjects returns stored at the cloud storage bucket precompiled objects for the target category
func (cd *CloudStorage) GetPrecompiledObjects(ctx context.Context, targetSdk pb.Sdk, targetCategory, bucketName string) (*SdkToCategories, error) {
	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		return nil, fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()

	precompiledObjects := make(SdkToCategories, 0)
	bucket := client.Bucket(bucketName)

	dirs, err := cd.getPrecompiledObjectsDirs(ctx, targetSdk, bucket)
	if err != nil {
		return nil, err
	}
	metaFiles := make(map[string][]byte, 0)
	for objectDir := range dirs {
		infoPath := filepath.Join(objectDir, MetaInfoName) // helping file with information about this object
		rc, err := bucket.Object(infoPath).NewReader(ctx)
		if err != nil {
			logger.Errorf("Object(%q).NewReader: %v", infoPath, err.Error())
			continue
		}
		metaFile, err := ioutil.ReadAll(rc)
		if err != nil {
			logger.Errorf("ioutil.ReadAll: %v", err.Error())
			continue
		}
		metaFiles[objectDir] = metaFile
		rc.Close()
	}

	for objectDir, metaFile := range metaFiles {
		precompiledObject := ObjectInfo{}
		err = json.Unmarshal(metaFile, &precompiledObject)
		if err != nil {
			logger.Errorf("json.Unmarshal: %v", err.Error())
			continue
		}

		folderName := strings.Split(objectDir, string(os.PathSeparator))[1]
		precompiledObject.Type = pb.PrecompiledObjectType(pb.PrecompiledObjectType_value[folderName])

		for _, objectCategory := range precompiledObject.Categories {
			if targetCategory == "" || targetCategory == objectCategory { //take only requested categories
				appendPrecompiledObject(precompiledObject, &precompiledObjects, objectDir, objectCategory)
			}
		}
	}
	return &precompiledObjects, nil
}

// GetDefaultPrecompiledObjects returns the default precompiled objects
func (cd *CloudStorage) GetDefaultPrecompiledObjects(ctx context.Context, bucketName string) (map[pb.Sdk]*pb.PrecompiledObject, error) {
	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		return nil, fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()
	bucket := client.Bucket(bucketName)

	paths := make(map[pb.Sdk]string, 0)
	for _, sdkName := range pb.Sdk_name {
		sdk := pb.Sdk(pb.Sdk_value[sdkName])
		if sdk == pb.Sdk_SDK_UNSPECIFIED {
			continue
		}
		path, err := cd.getDefaultPrecompiledObjectsPath(ctx, bucket, sdk)
		if err != nil {
			return nil, err
		}
		paths[sdk] = path
	}

	defaultPrecompiledObjects := make(map[pb.Sdk]*pb.PrecompiledObject, 0)
	for sdk, path := range paths {
		infoPath := filepath.Join(path, MetaInfoName)
		rc, err := bucket.Object(infoPath).NewReader(ctx)
		if err != nil {
			logger.Errorf("Object(%q).NewReader: %v", infoPath, err.Error())
			continue
		}
		metaFile, err := ioutil.ReadAll(rc)
		if err != nil {
			logger.Errorf("ioutil.ReadAll: %v", err.Error())
			continue
		}
		rc.Close()

		precompiledObject := &pb.PrecompiledObject{}
		err = json.Unmarshal(metaFile, &precompiledObject)
		if err != nil {
			logger.Errorf("json.Unmarshal: %v", err.Error())
			return nil, err
		}
		precompiledObject.CloudPath = path
		defaultPrecompiledObjects[sdk] = precompiledObject
	}
	return defaultPrecompiledObjects, nil
}

// getDefaultPrecompiledObjectsPath returns path for SDK to the default precompiled object
func (cd *CloudStorage) getDefaultPrecompiledObjectsPath(ctx context.Context, bucket *storage.BucketHandle, sdk pb.Sdk) (string, error) {
	pathToFile := fmt.Sprintf("%s/%s", sdk.String(), defaultPrecompiledObjectInfo)
	rc, err := bucket.Object(pathToFile).NewReader(ctx)
	if err != nil {
		logger.Errorf("Object(%q).NewReader: %v", pathToFile, err.Error())
		return "", err
	}

	data, err := io.ReadAll(rc)
	if err != nil {
		logger.Errorf("ioutil.ReadAll: %v", err.Error())
		return "", err
	}

	path := make(map[string]string, 0)
	if err := json.Unmarshal(data, &path); err != nil {
		return "", err
	}
	return path[sdk.String()], nil
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
			bucketAttrs, errWithAttrs := bucket.Attrs(ctx)
			if errWithAttrs != nil {
				return nil, fmt.Errorf("error during receiving bucket's attributes: %s", err)
			}
			return nil, fmt.Errorf("Bucket(%q).Objects: %v", bucketAttrs.Name, err)
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
func (cd *CloudStorage) getFileFromBucket(ctx context.Context, pathToObject string, extension, bucketName string) ([]byte, error) {
	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		return nil, fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()

	bucket := client.Bucket(bucketName)

	filePath := pathToObject
	if extension != "" {
		filePath = getFullFilePath(pathToObject, extension)
	}
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
	precompiledObjectName := filepath.Base(objectDir) //the base of the object's directory matches the name of the file
	fileName := strings.Join([]string{precompiledObjectName, extension}, ".")
	filePath := filepath.Join(objectDir, fileName)
	return filePath
}

// isPathToPrecompiledObjectFile is it a path where precompiled object is stored (i.e. SDK/ObjectType/ObjectName/ObjectCode.sdkExtension)
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
