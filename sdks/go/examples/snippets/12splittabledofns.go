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

package snippets

import (
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
)

type Record struct{}

type SomeService struct {
	ThrottlingErr error
}

func (s *SomeService) readNextRecords(position any) ([]Record, error) {
	return []Record{}, nil
}

type checkpointingSplittableDoFn struct {
	ExternalService SomeService
}

// [START self_checkpoint]
func (fn *checkpointingSplittableDoFn) ProcessElement(rt *sdf.LockRTracker, emit func(Record)) (sdf.ProcessContinuation, error) {
	position := rt.GetRestriction().(offsetrange.Restriction).Start
	for {
		records, err := fn.ExternalService.readNextRecords(position)

		if err != nil {
			if err == fn.ExternalService.ThrottlingErr {
				// Resume at a later time to avoid throttling.
				return sdf.ResumeProcessingIn(60 * time.Second), nil
			}
			return sdf.StopProcessing(), err
		}

		if len(records) == 0 {
			// Wait for data to be available.
			return sdf.ResumeProcessingIn(10 * time.Second), nil
		}
		for _, record := range records {
			if !rt.TryClaim(position) {
				// Records have been claimed, finish processing.
				return sdf.StopProcessing(), nil
			}
			position += 1

			emit(record)
		}
	}
}

// [END self_checkpoint]
