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

package fhirio

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"net/http"
	"testing"
)

type fakeFhirStoreClient struct {
	fakeReadResources func(string) (*http.Response, error)
}

func (c *fakeFhirStoreClient) readResource(resourcePath string) (*http.Response, error) {
	return c.fakeReadResources(resourcePath)
}

// Useful to fake the Body of a http.Response.
type fakeReaderCloser struct {
	fakeRead func([]byte) (int, error)
}

func (*fakeReaderCloser) Close() error {
	return nil
}

func (m *fakeReaderCloser) Read(b []byte) (int, error) {
	return m.fakeRead(b)
}

func validateCounters(t *testing.T, counterResults []metrics.CounterResult, expected []struct {
	string
	int64
}) {
	if len(counterResults) != len(expected) {
		t.Fatalf("counterResults got length %v, expected %v", len(counterResults), len(expected))
	}
	for i := 0; i < len(counterResults); i++ {
		if counterResults[i].Name() != expected[i].string {
			t.Fatalf("counterResults[i].Name() is '%v', expected '%v'", counterResults[i].Name(), expected[i].string)
		}
		if counterResults[i].Result() != expected[i].int64 {
			t.Fatalf("counterResults[i].Result() is %v, expected %v", counterResults[i].Result(), expected[i].int64)
		}
	}
}
