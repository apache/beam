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

package udf

import (
	"context"
	"encoding/base64"
	"fmt"
	udf_v1 "github.com/apache/beam/sdks/v2/go/cmd/wasmx/internal/proto/udf/v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/protox"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

const (
	urnDelim    = ":"
	urnPrefix   = "beam:transform"
	udfFileName = "udf.dat"
)

type Registry interface {
	Get(ctx context.Context, urn string) (*udf_v1.UserDefinedFunction, error)
	Set(ctx context.Context, urn string, fn *udf_v1.UserDefinedFunction) error
	Delete(ctx context.Context, urn string) error
}

func NewRegistry(ctx context.Context, location *url.URL) (Registry, error) {
	if location.Scheme != "file" {
		return nil, fmt.Errorf("no valid registry matches scheme for url: %s", location.String())
	}
	dir, err := filepath.Abs(filepath.Join(location.Host, location.Path))
	log.Printf("using registry at %s", dir)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, err
	}

	reg := &fileBasedRegistry{
		dir: dir,
	}

	fns := map[string]WasmFn{
		urn("add", "1.0"):       addWasm,
		urn("wordcount", "1.0"): wordCountWasm,
	}

	for urn, fn := range fns {
		data, err := protox.EncodeBase64(fn.FunctionSpec())
		if err != nil {
			return nil, err
		}
		b, err := base64.StdEncoding.DecodeString(data)
		if err != nil {
			return nil, err
		}

		now := timestamppb.Now()

		if err := reg.Set(ctx, urn, &udf_v1.UserDefinedFunction{
			Urn:      urn,
			Bytes:    b,
			Language: udf_v1.UserDefinedFunction_Language_Go,
			Created:  now,
			Updated:  now,
		}); err != nil {
			return nil, err
		}
	}

	return reg, nil
}

var _ Registry = &fileBasedRegistry{}

type fileBasedRegistry struct {
	dir string
}

func (reg *fileBasedRegistry) path(urn string) string {
	segs := strings.Split(urn, urnDelim)
	segs = append([]string{reg.dir}, segs...)
	return filepath.Join(segs...)
}

func (reg *fileBasedRegistry) Get(_ context.Context, urn string) (*udf_v1.UserDefinedFunction, error) {
	var result udf_v1.UserDefinedFunction
	parent := reg.path(urn)
	f, err := os.Open(filepath.Join(parent, udfFileName))
	if os.IsNotExist(err) {
		return nil, err
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	if err := protox.DecodeBase64(string(b), &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (reg *fileBasedRegistry) Set(ctx context.Context, urn string, fn *udf_v1.UserDefinedFunction) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	parent := reg.path(urn)
	if err := os.MkdirAll(parent, 0750); err != nil {
		return err
	}

	data, err := protox.EncodeBase64(fn)
	if err != nil {
		return err
	}

	f, err := os.Create(filepath.Join(parent, udfFileName))
	if err != nil {
		return err
	}

	_, err = io.WriteString(f, data)
	return err
}

func (reg *fileBasedRegistry) Delete(_ context.Context, urn string) error {
	dir := reg.path(urn)

	return os.RemoveAll(dir)
}

func urn(name string, version string) string {
	return strings.Join([]string{urnPrefix, name, version}, urnDelim)
}
