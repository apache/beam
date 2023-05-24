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
//   name: FinalChallenge1
//   description: Final challenge 1.
//   multifile: false
//   context_line: 54
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

package main

import (
	"context"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
	"strconv"
	"strings"
)

type Review struct {
	ReviewId   int
	MovieId    int
	UserId     int
	Rating     int
	ReviewText string
}

func main() {
	ctx := context.Background()

	// Initialize the pipeline
	p, s := beam.NewPipelineWithRoot()

	// Read the CSV lines
	lines := textio.Read(s, "input.csv")

	debug.Printf(s, "Log: %s", lines)

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

func parseLine(line string) Review {
	fields := strings.Split(line, ",")
	reviewId, _ := strconv.Atoi(fields[0])
	movieId, _ := strconv.Atoi(fields[1])
	userId, _ := strconv.Atoi(fields[2])
	rating, _ := strconv.Atoi(fields[3])
	reviewText := fields[4]

	return Review{ReviewId: reviewId, MovieId: movieId, UserId: userId, Rating: rating, ReviewText: reviewText}
}
