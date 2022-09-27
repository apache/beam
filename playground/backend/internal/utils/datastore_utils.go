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

package utils

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"cloud.google.com/go/datastore"

	"beam.apache.org/playground/backend/internal/constants"
	"beam.apache.org/playground/backend/internal/errors"
	"beam.apache.org/playground/backend/internal/logger"
)

func ID(salt, content string, length int8) (string, error) {
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

func GetExampleKey(ctx context.Context, values ...interface{}) *datastore.Key {
	id := GetIDWithDelimiter(values...)
	return getNameKey(ctx, constants.ExampleKind, id, nil)
}

func GetSdkKey(ctx context.Context, values ...interface{}) *datastore.Key {
	id := GetIDWithDelimiter(values...)
	return getNameKey(ctx, constants.SdkKind, id, nil)
}

func GetFileKey(ctx context.Context, values ...interface{}) *datastore.Key {
	id := GetIDWithDelimiter(values...)
	return getNameKey(ctx, constants.FileKind, id, nil)
}

func GetSchemaVerKey(ctx context.Context, values ...interface{}) *datastore.Key {
	id := GetIDWithDelimiter(values...)
	return getNameKey(ctx, constants.SchemaKind, id, nil)
}

func GetSnippetKey(ctx context.Context, values ...interface{}) *datastore.Key {
	id := GetIDWithDelimiter(values...)
	return getNameKey(ctx, constants.SnippetKind, id, nil)
}

func GetPCObjectKey(ctx context.Context, values ...interface{}) *datastore.Key {
	id := GetIDWithDelimiter(values...)
	return getNameKey(ctx, constants.PCObjectKind, id, nil)
}

func GetExampleID(cloudPath string) (string, error) {
	cloudPathParams := strings.Split(cloudPath, constants.CloudPathDelimiter)
	if len(cloudPathParams) < 3 {
		logger.Error("the wrong cloud path from a client")
		return "", fmt.Errorf("cloud path doesn't have all options. The minimum size must be 3")
	}
	return GetIDWithDelimiter(cloudPathParams[0], cloudPathParams[2]), nil
}

func GetIDWithDelimiter(values ...interface{}) string {
	valuesAsStr := make([]string, 0)
	for _, value := range values {
		switch value.(type) {
		case int:
			valuesAsStr = append(valuesAsStr, strconv.Itoa(value.(int)))
		case int8:
			valuesAsStr = append(valuesAsStr, strconv.Itoa(int(value.(int8))))
		case int16:
			valuesAsStr = append(valuesAsStr, strconv.Itoa(int(value.(int16))))
		case int32:
			valuesAsStr = append(valuesAsStr, strconv.Itoa(int(value.(int32))))
		case int64:
			valuesAsStr = append(valuesAsStr, strconv.Itoa(int(value.(int64))))
		default:
			valuesAsStr = append(valuesAsStr, value.(string))
		}
	}
	return strings.Join(valuesAsStr, constants.IDDelimiter)
}

// getNameKey returns the datastore key
func getNameKey(ctx context.Context, kind, id string, parentId *datastore.Key) *datastore.Key {
	key := datastore.NameKey(kind, id, nil)
	if parentId != nil {
		key.Parent = parentId
	}
	key.Namespace = GetNamespace(ctx)
	return key
}

func GetNamespace(ctx context.Context) string {
	namespace, ok := ctx.Value(constants.DatastoreNamespaceKey).(string)
	if !ok {
		namespace, ok = os.LookupEnv(constants.DatastoreNamespaceKey)
		if !ok {
			return constants.Namespace
		}
		return namespace
	}
	return namespace
}
