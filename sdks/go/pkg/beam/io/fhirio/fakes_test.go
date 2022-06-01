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

import "net/http"

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
