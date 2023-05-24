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
//   name: FinalSolution1
//   description: Final challenge solution 1.
//   multifile: false
//   context_line: 54
//   categories:
//     - Quickstart
//   complexity: BASIC
//   tags:
//     - hellobeam

package main

import (
	"context"
	"fmt"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/filter"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
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

	// Transform each line into a Review
	reviews := beam.ParDo(s, parseLine, lines)

	// Compute average ratings for each movie
	averageRatings := stats.MeanPerKey(s, beam.ParDo(s, func(r Review) (int, int) {
		return r.MovieId, r.Rating
	}, reviews))

	// Save the average ratings to a text file
	textio.Write(s, "average_ratings.txt", formatMeanResult(averageRatings))

	// Compute review counts for each movie
	reviewCounts := beam.ParDo(s, func(r Review) (int, int) {
		return r.MovieId, 1
	}, reviews)
	textio.Write(s, "review_counts.txt", formatCounts(reviewCounts))

	// Find the top 10 movies with the highest average rating and at least 2 reviews
	topMovies := beam.ParDo(s, func(r Review) (int, int) {
		return r.MovieId, r.Rating
	}, reviews)
	topMovies = filter.Include(s, topMovies, func(id int, rating int) bool {
		return rating > 2
	})
	topMovies = stats.MeanPerKey(s, topMovies)
	textio.Write(s, "top_movies.txt", formatMeanResult(topMovies))

	// ...and so on for the other tasks

	// Execute the pipeline
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

func formatMeanResult(id int, mean float64) string {
	return fmt.Sprintf("Movie %d: Average rating %.2f", id, mean)
}

func formatCounts(id int, count int) string {
	return fmt.Sprintf("Movie %d: Review count %d", id, count)
}
