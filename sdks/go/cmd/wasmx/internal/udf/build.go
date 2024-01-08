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
	"fmt"
	"github.com/apache/beam/sdks/v2/go/cmd/wasmx/internal/environment"
	"io"
	"os"
	"path/filepath"
)

const (
	extismPkg   = "github.com/extism/go-pdk"
	goSrcName   = "fn.go"
	wasmDstName = "fn.wasm"
)

var (
	tinyGoGenerate = fmt.Sprintf(`//go:generate tinygo build -o %s -target=wasi %s`, wasmDstName, goSrcName)
)

func goBuilder() *execBasedBuilder {
	return &execBasedBuilder{
		codeSrcName: goSrcName,
		setup: []*execSet{
			{
				executable: "go",
				args:       []string{"mod", "init", "wasmx"},
			},
			{
				executable: "go",
				args:       []string{"get", extismPkg},
			},
		},
		build: &execSet{
			executable: "go",
			args:       []string{"generate", goSrcName},
		},
		extraFileHeaders: []string{tinyGoGenerate},
	}
}

type builder interface {
	Setup(ctx context.Context) error
	Build(ctx context.Context, dst io.Writer, src io.Reader) error
	Teardown(ctx context.Context) error
}

type execBasedBuilder struct {
	tempDir          string
	codeSrcPath      string
	codeSrcName      string
	codeSrc          io.Writer
	extraFileHeaders []string
	setup            []*execSet
	build            *execSet
}

func (e *execBasedBuilder) Setup(ctx context.Context) error {
	if e.setup == nil || e.build == nil {
		return fmt.Errorf("executables for setup and build not fully configured")
	}
	if err := e.setupDir(); err != nil {
		return err
	}
	if err := e.setupSrcFile(); err != nil {
		return err
	}

	for _, exec := range e.setup {
		if err := exec.executable.Execute(ctx, e.tempDir, nil, nil, exec.args...); err != nil {
			return err
		}
	}
	return nil
}

func (e *execBasedBuilder) setupDir() error {
	dir, err := os.MkdirTemp("", "")
	e.tempDir = dir
	return err
}

func (e *execBasedBuilder) setupSrcFile() error {
	if e.tempDir == "" {
		return fmt.Errorf("temporary directory has not been setup")
	}
	if e.codeSrcName == "" {
		return fmt.Errorf("temporary name is empty")
	}
	e.codeSrcPath = filepath.Join(e.tempDir, e.codeSrcName)

	f, err := os.Create(e.codeSrcPath)
	if err != nil {
		return err
	}
	e.codeSrc = f
	for _, header := range e.extraFileHeaders {
		if _, err = fmt.Fprintln(e.codeSrc, header); err != nil {
			return err
		}
	}

	return nil
}

func (e *execBasedBuilder) Build(ctx context.Context, dst io.Writer, src io.Reader) error {
	defer e.Teardown(ctx)
	if err := e.Setup(ctx); err != nil {
		return err
	}
	if _, err := io.Copy(e.codeSrc, src); err != nil {
		return err
	}
	if err := e.build.executable.Execute(ctx, e.tempDir, nil, nil, e.build.args...); err != nil {
		return err
	}
	path := filepath.Join(e.tempDir, wasmDstName)
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	_, err = io.Copy(dst, f)
	return err
}

func (e *execBasedBuilder) Teardown(ctx context.Context) error {
	if e.tempDir != "" {
		return os.RemoveAll(e.tempDir)
	}
	return nil
}

type execSet struct {
	executable environment.Executable
	args       []string
}
