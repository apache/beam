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

package entity

import "cloud.google.com/go/datastore"

type ExampleEntity struct {
	Name        string         `datastore:"name"`
	Sdk         *datastore.Key `datastore:"sdk"`
	Descr       string         `datastore:"descr"`
	Tags        []string       `datastore:"tags"`
	Cats        []string       `datastore:"cats"`
	Path        string         `datastore:"path"` // TODO remove after #24402 update sequence is done
	Type        string         `datastore:"type"`
	Origin      string         `datastore:"origin"`
	SchVer      *datastore.Key `datastore:"schVer"`
	UrlVCS      string         `datastore:"urlVCS"`
	UrlNotebook string         `datastore:"urlNotebook"`
	AlwaysRun   bool           `datastore:"alwaysRun"`
	NeverRun    bool           `datastore:"neverRun"`
}

type PrecompiledObjectEntity struct {
	Content string `datastore:"content,noindex"`
}
