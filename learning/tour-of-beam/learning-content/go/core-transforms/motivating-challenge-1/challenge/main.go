/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// beam-playground:
//   name: CoreTransformsChallenge1
//   description: Core Transforms first motivating challenge.
//   multifile: false
//   context_line: 44
//   categories:
//     - Quickstart
//   complexity: BASIC
//   tags:
//     - hellobeam

package main

import (
	"context"
    _ "strings"
	"regexp"
    _ "github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/filter"
    "github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
    _ "github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
)

var (
    wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)
)

func main() {
	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

    file := textio.Read(s, "gs://apache-beam-samples/shakespeare/kinglear.txt")

    lines := getLines(s, file)

    words := getWords(s,lines)

    allCaseWords := partitionPCollectionByCase(s,words)

    upperCaseWords := countPerElement(s, allCaseWords[0])
    capitalCaseWords := countPerElement(s, allCaseWords[1])
    lowerCaseWords := countPerElement(s, allCaseWords[2])

    newFirstPartPCollection:=convertPCollectionToLowerCase(s,upperCaseWords)
    newSecondPartPCollection:=convertPCollectionToLowerCase(s,capitalCaseWords)

    flattenPCollection := mergePCollections(s,newFirstPartPCollection,newSecondPartPCollection,lowerCaseWords)

    groupByKeyPCollection := groupByKey(s,flattenPCollection)

    debug.Print(s, groupByKeyPCollection)

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

func getLines(s beam.Scope, input beam.PCollection) beam.PCollection {
    return input
}

func getWords(s beam.Scope, input beam.PCollection) beam.PCollection {
    return input
}


func partitionPCollectionByCase(s beam.Scope, input beam.PCollection) []beam.PCollection {
   result := []beam.PCollection{input,input,input}

   return result
   }

func countPerElement(s beam.Scope, input beam.PCollection) beam.PCollection{
        return input
}

func convertPCollectionToLowerCase(s beam.Scope, input beam.PCollection) beam.PCollection{
        return input
}

func mergePCollections(s beam.Scope, aInput beam.PCollection,bInput beam.PCollection,cInput beam.PCollection) beam.PCollection{
        return aInput
}

func groupByKey(s beam.Scope, input beam.PCollection) beam.PCollection {
	return input
}