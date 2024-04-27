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

/*
Package webapi supports reading from or writing to Web APIs.

Its design goals are to reduce the boilerplate of building Beam I/O connectors. See tracking issue:
https://github.com/apache/beam/issues/30423 and visit the Beam website
(https://beam.apache.org/documentation/io/built-in/webapis/) for details and examples.

# Basic usage

Basic usage requires providing a Caller to the Call func.

	var _ webapi.Caller = &myCaller{}
	type myCaller struct {
		// Make configuration details public and tag with: `beam:"endpoint"` so that they are encoded/decoded by Beam.
		Endpoint string `beam:"endpoint"`
	}

	// Call posts webapi.Request's JSON Payload in this example to myCaller's Endpoint.
	// Returns a webapi.Response containing the HTTP response body.
	func (caller *myCaller) Call(ctx context.Context, request *webapi.Request) (*webapi.Response, error) {
		resp, err := http.Post(caller.Endpoint, "application/json", bytes.NewBuffer(request.Payload))
		if err != nil {
			return nil, err
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return &webapi.Response{
			Payload: body,
		}, nil
	}

To use the Caller in a Beam PTransform, simply provide it to the Call func which returns a tuple of PCollections,
on for successful responses and another for any errors.

	requests := // PCollection of *webapi.Request.
	responses, failures := Call(&myCaller{})
*/
package webapi
