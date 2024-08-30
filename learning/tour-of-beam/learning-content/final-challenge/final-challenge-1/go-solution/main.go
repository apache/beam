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
//   multifile: true
//   files:
//     - name: input.csv
//   context_line: 54
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

package main

import (
	"context"
	"fmt"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window/trigger"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/filter"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"log"
	"strconv"
	"strings"
	"time"
)

type Transaction struct {
	ID          int64
	Date        string
	ProductID   string
	ProductName string
	Price       float64
	Quantity    int64
	CustomerID  int64
	Country     string
}

func main() {
	ctx := context.Background()

	beam.Init()
	p := beam.NewPipeline()
	s := p.Root()

	file := textio.Read(s, "input.csv")

	transactions := getTransactions(s, file)

	trigger := trigger.AfterEndOfWindow().
		EarlyFiring(trigger.AfterProcessingTime().
			PlusDelay(5 * time.Second)).
		LateFiring(trigger.Repeat(trigger.AfterCount(1)))

	fixedWindowedItems := beam.WindowInto(s, window.NewFixedWindows(30*time.Second), transactions,
		beam.Trigger(trigger),
		beam.AllowedLateness(30*time.Minute),
		beam.PanesDiscard(),
	)

	filtered := filtering(s, fixedWindowedItems)

	result := getPartition(s, filtered)

	biggerThan10 := sumCombine(s, mapIdWithPrice(s, result[0]))
	textio.Write(s, "price_more_than_10.txt", convertToString(s, biggerThan10))

	smallerThan10 := sumCombine(s, mapIdWithPrice(s, result[1]))
	textio.Write(s, "price_less_than_10.txt", convertToString(s, smallerThan10))

	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}

func getTransactions(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, func(line string, emit func(transaction Transaction)) {
		csv := strings.Split(line, ",")

		if csv[0] != "TransactionNo" {
			id, _ := strconv.ParseInt(csv[0], 10, 64)
			price, _ := strconv.ParseFloat(csv[4], 64)
			quantity, _ := strconv.ParseInt(csv[5], 10, 64)
			customerID, _ := strconv.ParseInt(csv[6], 10, 64)
			emit(Transaction{
				ID:          id,
				Date:        csv[1],
				ProductID:   csv[2],
				ProductName: csv[3],
				Price:       price,
				Quantity:    quantity,
				CustomerID:  customerID,
				Country:     csv[7],
			})
		}
	}, input)
}

func getPartition(s beam.Scope, input beam.PCollection) []beam.PCollection {
	return beam.Partition(s, 2, func(element Transaction) int {
		if element.Price >= 10 {
			return 0
		}
		return 1
	}, input)
}

func convertToString(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, func(product string, sum float64, emit func(string)) {
		emit(fmt.Sprint("product: ", product, " , sum: ", sum))
	}, input)
}

func filtering(s beam.Scope, input beam.PCollection) beam.PCollection {
	return filter.Include(s, input, func(element Transaction) bool {
		return element.Quantity >= 20
	})
}

func mapIdWithPrice(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, func(element Transaction, emit func(string, float64)) {
		emit(element.ProductID, element.Price)
	}, input)
}

func sumCombine(s beam.Scope, input beam.PCollection) beam.PCollection {
	return stats.SumPerKey(s, input)
}
