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

package common

import (
	"beam.apache.org/learning/katas/windowing/adding_timestamp/pardo/pkg/task"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"time"
)

var (
	commits = []task.Commit{
		{
			Datetime: time.Date(2020, 7, 31, 15, 52, 5, 0, time.UTC),
			Message:  "3c6c45924a Remove trailing whitespace from README",
		},
		{
			Datetime: time.Date(2020, 7, 31, 15, 59, 40, 0, time.UTC),
			Message:  "a52be99b62 Merge pull request #12443 from KevinGG/whitespace",
		},
		{
			Datetime: time.Date(2020, 7, 31, 16, 7, 36, 0, time.UTC),
			Message:  "7c1772d13f Merge pull request #12439 from ibzib/beam-9199-1",
		},
		{
			Datetime: time.Date(2020, 7, 31, 16, 35, 41, 0, time.UTC),
			Message:  "d971ba13b8 Widen ranges for GCP libraries (#12198)",
		},
		{
			Datetime: time.Date(2020, 8, 1, 0, 7, 25, 0, time.UTC),
			Message:  "875620111b Enable all Jenkins jobs triggering for committers (#12407)",
		},
	}
)

func CreateCommits(s beam.Scope) beam.PCollection {
	return beam.CreateList(s, commits)
}
