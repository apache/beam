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
	"cloud.google.com/go/logging"
	"fmt"
	"time"
)

const logId = "playground-log"

// CloudLoggingHandler represents 'Cloud Logging' package that logs to Google Cloud Logging service.
type CloudLoggingHandler struct {
	logger *logging.Logger
	client *logging.Client
}

// NewCloudLoggingHandler creates CloudLoggingHandler
func NewCloudLoggingHandler(client *logging.Client) *CloudLoggingHandler {
	return &CloudLoggingHandler{client: client, logger: client.Logger(logId)}
}

func (c CloudLoggingHandler) Info(args ...interface{}) {
	c.logMessage(logging.Info, args...)
}

func (c CloudLoggingHandler) Infof(format string, args ...interface{}) {
	c.logMessage(logging.Info, fmt.Sprintf(format, args...))
}

func (c CloudLoggingHandler) Warn(args ...interface{}) {
	c.logMessage(logging.Warning, args...)
}

func (c CloudLoggingHandler) Warnf(format string, args ...interface{}) {
	c.logMessage(logging.Warning, fmt.Sprintf(format, args...))
}

func (c CloudLoggingHandler) Error(args ...interface{}) {
	c.logMessage(logging.Error, args...)
}

func (c CloudLoggingHandler) Errorf(format string, args ...interface{}) {
	c.logMessage(logging.Error, fmt.Sprintf(format, args...))
}

func (c CloudLoggingHandler) Debug(args ...interface{}) {
	c.logMessage(logging.Debug, args...)
}

func (c CloudLoggingHandler) Debugf(format string, args ...interface{}) {
	c.logMessage(logging.Debug, fmt.Sprintf(format, args...))
}

func (c CloudLoggingHandler) Fatal(args ...interface{}) {
	c.logMessage(logging.Critical, args...)
}

func (c CloudLoggingHandler) Fatalf(format string, args ...interface{}) {
	c.logMessage(logging.Critical, fmt.Sprintf(format, args...))
}

// logMessage buffers the Entry for output to the logging service.
func (c CloudLoggingHandler) logMessage(severity logging.Severity, args ...interface{}) {
	c.logger.Log(logging.Entry{
		Timestamp: time.Now(),
		Severity:  severity,
		Payload:   fmt.Sprint(args...),
	})
}

// CloseConn waits for all opened loggers to be flushed and closes the client.
func (c CloudLoggingHandler) CloseConn() error {
	return c.client.Close()
}
