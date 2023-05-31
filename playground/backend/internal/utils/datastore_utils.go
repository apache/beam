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
	"beam.apache.org/playground/backend/internal/logger"
	"context"
	"os"
	"strconv"
	"strings"

	"cloud.google.com/go/datastore"

	"beam.apache.org/playground/backend/internal/constants"
)

func GetExampleKey(ctx context.Context, values ...interface{}) *datastore.Key {
	id := GetIDWithDelimiter(values...)
	return getNameKey(ctx, constants.ExampleKind, id, nil)
}

func GetSdkKey(ctx context.Context, values ...interface{}) *datastore.Key {
	id := GetIDWithDelimiter(values...)
	return getNameKey(ctx, constants.SdkKind, id, nil)
}

func GetDatasetKey(ctx context.Context, values ...interface{}) *datastore.Key {
	id := GetIDWithDelimiter(values...)
	return getNameKey(ctx, constants.DatasetKind, id, nil)
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
	namespaceValue := ctx.Value(constants.DatastoreNamespaceKey)
	namespace, ok := namespaceValue.(string)
	if namespaceValue != nil && !ok {
		logger.Warnf("GetNamespace(): %s value is set in context, but is not a string", constants.DatastoreNamespaceKey)
	}
	if !ok {
		namespace, ok = os.LookupEnv(constants.DatastoreNamespaceKey)
		if !ok {
			return constants.Namespace
		}
		return namespace
	}
	return namespace
}
