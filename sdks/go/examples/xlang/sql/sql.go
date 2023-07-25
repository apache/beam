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

// The sql example demonstrates how to run a cross-language SQL transform
// in a Beam Go Pipeline.

// Prerequisites to run sql:
// –> [Required] Job needs to be submitted to a portable runner (--runner=universal)
// –> [Required] Endpoint of job service needs to be passed (--endpoint=<ip:port>)
// –> [Required] Endpoint of expansion service needs to be passed (--expansion_addr=<ip:port>)
// –> [Optional] Environment type can be LOOPBACK. Defaults to DOCKER. (--environment_type=LOOPBACK|DOCKER)
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/sql"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	expansionAddr = flag.String("expansion_addr", "", "Address of Expansion Service")
)

// Move is a struct wrapping information about a character's moves in a video
// game. Each field is exported so the type has a schema inferred for it that
// is passed to the expansion service when the cross-language transform is called.
type Move struct {
	Character string
	MoveName  string
}

func (m *Move) String() string {
	return fmt.Sprintf("Character: %v, Move %v", m.Character, m.MoveName)
}

func blankaMoves() []Move {
	var moves []Move
	moves = append(moves, Move{Character: "Blanka", MoveName: "Vertical Rolling"})
	moves = append(moves, Move{Character: "Blanka", MoveName: "Rolling Attack"})
	moves = append(moves, Move{Character: "Blanka", MoveName: "Electric Thunder"})
	return moves
}

func sagatMoves() []Move {
	var moves []Move
	moves = append(moves, Move{Character: "Sagat", MoveName: "Tiger Shot"})
	moves = append(moves, Move{Character: "Sagat", MoveName: "Tiger Knee Crush"})
	moves = append(moves, Move{Character: "Sagat", MoveName: "Tiget Uppercut"})
	return moves
}

func ryuMoves() []Move {
	var moves []Move
	moves = append(moves, Move{Character: "Ryu", MoveName: "Hadouken"})
	moves = append(moves, Move{Character: "Ryu", MoveName: "Shoryuken"})
	moves = append(moves, Move{Character: "Ryu", MoveName: "Tatsumaki Senpuukyaku"})
	return moves
}

func makeDemoMoves() []Move {
	blanka := blankaMoves()
	sagat := sagatMoves()
	ryu := ryuMoves()
	var moves []Move
	moves = append(moves, blanka...)
	moves = append(moves, sagat...)
	moves = append(moves, ryu...)
	return moves
}

func main() {
	flag.Parse()
	beam.Init()

	p, s := beam.NewPipelineWithRoot()

	moves := makeDemoMoves()
	pcol := beam.CreateList(s, moves)

	// Options for the sql transform have to be defined before the Transform call.
	var opts []sql.Option
	// Dialect options are "calcite" and "zetasql"
	opts = append(opts, sql.Dialect("calcite"))
	// The expansion address can be specified per-call here or overwritten for all
	// calls using xlangx.RegisterOverrideForUrn(sqlx.Urn, *expansionAddr).
	opts = append(opts, sql.ExpansionAddr(*expansionAddr))
	// The PCollection input name currently does not matter, as it will be referred
	// to as "PCOLLECTION" in the query
	opts = append(opts, sql.Input("moves", pcol))
	// Specify the expected output type of the returned PCollection. This is type
	// checked at pipeline construction time, not run time.
	opts = append(opts, sql.OutputType(reflect.TypeOf(Move{})))

	// The query should be provded as a string in the SQL dialect specified in the options
	// above. When specifying fields from the struct, wrap the names in ``
	query := "SELECT * FROM PCOLLECTION WHERE `Character` = 'Blanka'"

	if *expansionAddr != "" {
		// The expansion address can be specified per-call here or overwritten for all
		// calls using xlangx.RegisterOverrideForUrn(sqlx.Urn, *expansionAddr).
		opts = append(opts, sql.ExpansionAddr(*expansionAddr))
	} else {
		log.Print("no expansion service address provided, will start automated service")
	}

	outputCollection := sql.Transform(s, query, opts...)

	// We aren't saving our output, so we will just check that the query returned the correct
	// output (in this case, only Blanka's move list.)
	passert.EqualsList(s, outputCollection, blankaMoves())

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
