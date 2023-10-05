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

package logger

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

var preparedHandler testHandler

const (
	codeFatal     = "package main\n\nimport (\n\t\"beam.apache.org/playground/backend/internal/logger\"\n)\n\nfunc main() {\n\tlogger.AddHandler(logger.NewStdHandler())\n\tlogger.Fatal(\"%v\")\n}"
	codeFatalf    = "package main\n\nimport (\n\t\"beam.apache.org/playground/backend/internal/logger\"\n)\n\nfunc main() {\n\tlogger.AddHandler(logger.NewStdHandler())\n\tlogger.Fatalf(\"%v\",\"%s\")\n}"
	testLoggerDir = "testLogger"
	testFatalDir  = "testFatal"
	testFatalfDir = "testFatalf"
)

var testFatalPath, testFatalfPath string

type testHandler struct {
	logs []string
}

func (t *testHandler) Info(args ...interface{}) {
	t.logMessage(INFO, args...)
}

func (t *testHandler) Infof(format string, args ...interface{}) {
	t.logMessage(INFO, fmt.Sprintf(format, args...))
}

func (t *testHandler) Warn(args ...interface{}) {
	t.logMessage(WARN, args...)
}

func (t *testHandler) Warnf(format string, args ...interface{}) {
	t.logMessage(WARN, fmt.Sprintf(format, args...))
}

func (t *testHandler) Error(args ...interface{}) {
	t.logMessage(ERROR, args...)
}

func (t *testHandler) Errorf(format string, args ...interface{}) {
	t.logMessage(ERROR, fmt.Sprintf(format, args...))
}

func (t *testHandler) Debug(args ...interface{}) {
	t.logMessage(DEBUG, args...)
}

func (t *testHandler) Debugf(format string, args ...interface{}) {
	t.logMessage(DEBUG, fmt.Sprintf(format, args...))
}

func (t *testHandler) Fatal(args ...interface{}) {
	t.logMessage(FATAL, args...)
}

func (t *testHandler) Fatalf(format string, args ...interface{}) {
	t.logMessage(FATAL, fmt.Sprintf(format, args...))
}

func (t *testHandler) logMessage(severity Severity, args ...interface{}) {
	args = append([]interface{}{severity}, args...)
	t.logs = append(t.logs, fmt.Sprint(args...))
}

func TestMain(m *testing.M) {
	preparedHandler = testHandler{logs: []string{}}
	handlersTest := []Handler{&preparedHandler}
	SetHandlers(handlersTest)
	err := preparedFolder()
	if err != nil {
		log.Fatal(err)
	}
	defer teardown()
	m.Run()
}

func preparedFolder() error {
	err := os.MkdirAll(filepath.Join(testLoggerDir, testFatalDir), 0755)
	if err != nil {
		return err
	}
	err = os.MkdirAll(filepath.Join(testLoggerDir, testFatalfDir), 0755)
	if err != nil {
		return err
	}
	testFatalPath = filepath.Join(testLoggerDir, testFatalDir, "test.go")
	testFatalfPath = filepath.Join(testLoggerDir, testFatalfDir, "test.go")
	_, err = os.Create(testFatalPath)
	if err != nil {
		return err
	}
	_, err = os.Create(testFatalfPath)
	if err != nil {
		return err
	}
	return nil
}

func teardown() {
	err := os.RemoveAll(testLoggerDir)
	if err != nil {
		log.Fatal(err)
	}
}

func TestInfo(t *testing.T) {
	type args struct {
		args []interface{}
	}
	var tests = []struct {
		name string
		args args
	}{
		{
			name: "Info",
			args: args{args: []interface{}{"TEST_VALUE"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Info(tt.args.args...)
			value := append([]interface{}{INFO}, tt.args.args...)
			expectedValue := preparedHandler.logs[len(preparedHandler.logs)-1]
			if expectedValue != fmt.Sprint(value...) {
				t.Errorf("Value %v not added in the logs", expectedValue)
			}
		})
	}
}

func TestInfof(t *testing.T) {
	type args struct {
		format string
		args   []interface{}
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Infof",
			args: args{
				format: "TEST FORMAT %s",
				args:   []interface{}{"TEST_VALUE"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Infof(tt.args.format, tt.args.args...)
			value := append([]interface{}{INFO}, fmt.Sprintf(tt.args.format, tt.args.args...))
			expectedValue := preparedHandler.logs[len(preparedHandler.logs)-1]
			if expectedValue != fmt.Sprint(value...) {
				t.Errorf("Value %v not added in the logs", expectedValue)
			}
		})
	}
}

func TestWarn(t *testing.T) {
	type args struct {
		args []interface{}
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Warn",
			args: args{args: []interface{}{"TEST_VALUE"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Warn(tt.args.args...)
			value := append([]interface{}{WARN}, tt.args.args...)
			expectedValue := preparedHandler.logs[len(preparedHandler.logs)-1]
			if expectedValue != fmt.Sprint(value...) {
				t.Errorf("Value %v not added in the logs", expectedValue)
			}
		})
	}
}

func TestWarnf(t *testing.T) {
	type args struct {
		format string
		args   []interface{}
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Warnf",
			args: args{
				format: "TEST FORMAT %s",
				args:   []interface{}{"TEST_VALUE"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Warnf(tt.args.format, tt.args.args...)
			value := append([]interface{}{WARN}, fmt.Sprintf(tt.args.format, tt.args.args...))
			expectedValue := preparedHandler.logs[len(preparedHandler.logs)-1]
			if expectedValue != fmt.Sprint(value...) {
				t.Errorf("Value %v not added in the logs", expectedValue)
			}
		})
	}
}

func TestError(t *testing.T) {
	type args struct {
		args []interface{}
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Error",
			args: args{args: []interface{}{"TEST_VALUE"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Error(tt.args.args...)
			value := append([]interface{}{ERROR}, tt.args.args...)
			expectedValue := preparedHandler.logs[len(preparedHandler.logs)-1]
			if expectedValue != fmt.Sprint(value...) {
				t.Errorf("Value %v not added in the logs", expectedValue)
			}
		})
	}
}

func TestErrorf(t *testing.T) {
	type args struct {
		format string
		args   []interface{}
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Errorf",
			args: args{
				format: "TEST FORMAT %s",
				args:   []interface{}{"TEST_VALUE"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Errorf(tt.args.format, tt.args.args...)
			value := append([]interface{}{ERROR}, fmt.Sprintf(tt.args.format, tt.args.args...))
			expectedValue := preparedHandler.logs[len(preparedHandler.logs)-1]
			if expectedValue != fmt.Sprint(value...) {
				t.Errorf("Value %v not added in the logs", expectedValue)
			}
		})
	}
}

func TestDebug(t *testing.T) {
	type args struct {
		args []interface{}
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Debug",
			args: args{args: []interface{}{"TEST_VALUE"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Debug(tt.args.args...)
			value := append([]interface{}{DEBUG}, tt.args.args...)
			expectedValue := preparedHandler.logs[len(preparedHandler.logs)-1]
			if expectedValue != fmt.Sprint(value...) {
				t.Errorf("Value %v not added in the logs", expectedValue)
			}
		})
	}
}

func TestDebugf(t *testing.T) {
	type args struct {
		format string
		args   []interface{}
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Debugf",
			args: args{
				format: "TEST FORMAT %s",
				args:   []interface{}{"TEST_VALUE"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Debugf(tt.args.format, tt.args.args...)
			value := append([]interface{}{DEBUG}, fmt.Sprintf(tt.args.format, tt.args.args...))
			expectedValue := preparedHandler.logs[len(preparedHandler.logs)-1]
			if expectedValue != fmt.Sprint(value...) {
				t.Errorf("Value %v not added in the logs", expectedValue)
			}
		})
	}
}

func TestFatal(t *testing.T) {
	type args struct {
		args []interface{}
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Fatal",
			args: args{args: []interface{}{"TEST_VALUE"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := os.WriteFile(testFatalPath, []byte(fmt.Sprintf(codeFatal, tt.args.args[0])), 0755)
			if err != nil {
				t.Error(err)
			}
			cmd := exec.Command("go", "run", testFatalPath)
			stdout, _ := cmd.StderrPipe()
			if err := cmd.Start(); err != nil {
				t.Error(err)
			}
			gotBytes, _ := ioutil.ReadAll(stdout)
			got := strings.Split(string(gotBytes), "\n")
			args := append([]interface{}{string(FATAL) + " "}, tt.args.args...)
			expectedValue := fmt.Sprint(args...)
			if !strings.HasSuffix(got[0], expectedValue) {
				t.Errorf("Unexpected log message. Got '%s' but contain '%s'", got[:len(got)-1], expectedValue)
			}
			err = cmd.Wait()
			if e, ok := err.(*exec.ExitError); !ok || e.Success() {
				t.Errorf("Process ran with err %v, want exit status 1", err)
			}
		})
	}
}

func TestFatalf(t *testing.T) {
	type args struct {
		format string
		args   []interface{}
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Fatalf",
			args: args{
				format: "TEST FORMAT %s",
				args:   []interface{}{"TEST_VALUE"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := os.WriteFile(testFatalfPath, []byte(fmt.Sprintf(codeFatalf, tt.args.format, tt.args.args[0])), 0755)
			if err != nil {
				t.Error(err)
			}
			cmd := exec.Command("go", "run", testFatalfPath)
			stdout, _ := cmd.StderrPipe()
			if err := cmd.Start(); err != nil {
				t.Error(err)
			}
			gotBytes, _ := ioutil.ReadAll(stdout)
			got := strings.Split(string(gotBytes), "\n")
			args := append([]interface{}{string(FATAL) + " "}, fmt.Sprintf(tt.args.format, tt.args.args...))
			expectedValue := fmt.Sprint(args...)
			if !strings.HasSuffix(got[0], expectedValue) {
				t.Errorf("Unexpected log message. Got '%s' but contain '%s'", got[:len(got)-1], expectedValue)
			}
			err = cmd.Wait()
			if e, ok := err.(*exec.ExitError); !ok || e.Success() {
				t.Errorf("Process ran with err %v, want exit status 1", err)
			}
		})
	}
}
