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
//   name: FinalSolution2
//   description: Final challenge solution 2.
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
	"log"
	"reflect"
	"strconv"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
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
	beam.Init()
	p := beam.NewPipeline()
	s := p.Root()

	t := beam.TypeOf(reflect.TypeOf((*Transaction)(nil)).Elem())
	file := textio.Read(s, "input.csv")

	data := beam.ParDo(s, extractDataFn, file, beam.TypeDefinition{Var: t.Var, T: t})

	biggerThan10 := beam.ParDo(s, partitionBiggerThan10Fn, data, beam.TypeDefinition{Var: t.Var, T: t})
	smallerThan10 := beam.ParDo(s, partitionSmallerThan10Fn, data, beam.TypeDefinition{Var: t.Var, T: t})

	textio.Write(s, "biggerThan10.txt", biggerThan10)
	textio.Write(s, "smallerThan10.txt", smallerThan10)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}

func extractDataFn(ctx context.Context, line string, emit func(Transaction)) {
	csv := strings.Split(line, ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
	if len(csv) < 8 {
		return
	}
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

func partitionBiggerThan10Fn(ctx context.Context, transaction Transaction, emit func(Transaction)) {
	if transaction.Price > 10 {
		emit(transaction)
	}
}

func partitionSmallerThan10Fn(ctx context.Context, transaction Transaction, emit func(Transaction)) {
	if transaction.Price <= 10 {
		emit(transaction)
	}
}
