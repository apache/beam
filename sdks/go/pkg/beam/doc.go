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

// Package beam is an implementation of the Apache Beam (https://beam.apache.org)
// programming model in Go.  Beam provides a simple, powerful model for
// building both batch and streaming parallel data processing pipelines.
//
// In order to start creating the pipeline for execution, a Pipeline object is needed.
//
// 	 p, s := beam.NewPipeline()
//
// The pipeline object encapsulates all the data and steps in your processing task.
// It is the basis for creating the pipeline's data sets as PCollections and its operations
// as transforms.
//
// The PCollection abstraction represents a potentially distributed,
// multi-element data set. You can think of a PCollection as “pipeline” data;
// Beam transforms use PCollection objects as inputs and outputs. As such, if
// you want to work with data in your pipeline, it must be in the form of a
// PCollection.
//
//   // Start by reading text from an input files.
//   lines := textio.Read(s, "protocol://path/file*.txt")
//
// Transforms are added to the pipeline so they are part of the work to be
// executed.  Since this transform has no PCollection as an input, it is
// considered a 'root transform'
//
//    // A pipeline can have multiple root transforms
//    moreLines :=  textio.Read(s, "protocol://other/path/file*.txt")
//
// Further transforms can be applied, creating an arbitrary, acyclic graph.
// Subsequent transforms (and the intermediate PCollections they produce) are
// attached to the same pipeline.
//    all := beam.Flatten(s, lines, moreLines)
//    wordRegexp := regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)
//    words := beam.ParDo(s, func (line string, emit func(string))) {
//         for _, word := range wordRegexp.FindAllString(line, -1) {
//             emit(word)
//         }
//    }, all)
//    formatted := beam.ParDo(s, string.ToUpper, words)
//    textio.Write(s, "protocol://output/path", formatted)
//
// Applying a transform adds it to the pipeline, rather than executing it
// immediately.  Once the whole pipeline of transforms is constructed, the
// pipeline can be executed by a PipelineRunner.  The direct runner executes the
// transforms directly, sequentially, in this one process, which is useful for
// unit tests and simple experiments:
//    if err := direct.Run(context.Background(), p); err != nil {
//        log.Fatalf("Pipeline failed: %v", err)
//    }
//
// See: https://beam.apache.org/documentation/programming-guide
package beam
