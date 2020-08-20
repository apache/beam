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

// xlang_wordcount exemplifies using a cross language transform from Python to count words
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"

	// Imports to enable correct filesystem access and runner setup in LOOPBACK mode
	_ "github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/gcs"
	_ "github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/local"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/universal"
)

var (
	// Set this required option to specify where to write the output.
	output = flag.String("output", "./output", "Output file (required).")

	expansionAddr = flag.String("expansion-addr", "", "Address of Expansion Service")
)

var (
	wordRE  = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)
	empty   = beam.NewCounter("extract", "emptyLines")
	lineLen = beam.NewDistribution("extract", "lineLenDistro")
)

// extractFn is a DoFn that emits the words in a given line.
func extractFn(ctx context.Context, line string, emit func(string)) {
	lineLen.Update(ctx, int64(len(line)))
	if len(strings.TrimSpace(line)) == 0 {
		empty.Inc(ctx, 1)
	}
	for _, word := range wordRE.FindAllString(line, -1) {
		emit(word)
	}
}

// formatFn is a DoFn that formats a word and its count as a string.
func formatFn(w string, c int64) string {
	return fmt.Sprintf("%s: %v", w, c)
}

func init() {
	beam.RegisterFunction(extractFn)
	beam.RegisterFunction(formatFn)
}

func main() {
	flag.Parse()
	beam.Init()

	if *output == "" {
		log.Fatal("No output provided")
	}

	if *expansionAddr == "" {
		log.Fatal("No expansion address provided")
	}

	p := beam.NewPipeline()
	s := p.Root()

	lines := beam.CreateList(s, strings.Split(lorem, "\n"))
	col := beam.ParDo(s, extractFn, lines)

	// Using Cross-language Count from Python's test expansion service
	outputType := typex.NewKV(typex.New(reflectx.String), typex.New(reflectx.Int64))
	counted := beam.CrossLanguage(s,
		"beam:transforms:xlang:count",
		nil,
		*expansionAddr,
		map[string]beam.PCollection{"xlang-in": col},
		map[string]typex.FullType{"output": outputType},
	)

	formatted := beam.ParDo(s, formatFn, counted["output"])
	textio.Write(s, *output, formatted)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}

var lorem = `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Mauris id tellus vehicula, rutrum turpis quis, suscipit est. Quisque vehicula nec ex a interdum. Phasellus vulputate nunc sit amet nisl dapibus tincidunt ut ullamcorper nisi. Mauris gravida porta leo vel congue. Duis sit amet arcu eu nisl pharetra interdum a eget enim. Nulla facilisis massa ut egestas interdum. Nunc elit dui, hendrerit at pharetra a, pellentesque non turpis. Integer auctor vulputate congue. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Ut sagittis convallis lorem non semper. Ut ultrices elit a enim pulvinar fermentum.
In rhoncus, diam sit amet laoreet ullamcorper, turpis lorem ornare tortor, eget eleifend purus risus id justo. Ut eget tortor vel elit aliquet egestas. Integer iaculis ipsum at nunc condimentum accumsan. Ut tristique felis ut metus tincidunt, quis ullamcorper diam convallis. Donec mattis ultrices lorem, id placerat tellus venenatis at. Donec quis nulla dui. Pellentesque at semper nunc. Aenean orci dui, dictum id urna a, luctus consequat augue. Nulla hendrerit mi ut quam iaculis euismod. Praesent eu velit fermentum, luctus velit id, condimentum velit.
Suspendisse tempus vestibulum magna ac sollicitudin. Pellentesque id consequat lorem. Curabitur laoreet at velit a laoreet. Donec justo lectus, elementum eu elit a, placerat elementum turpis. Aliquam pretium ipsum eros, quis ultricies metus lobortis ut. Cras porta congue luctus. Curabitur vestibulum lacus est, quis eleifend metus euismod id. Duis vel ante ipsum. Proin sed posuere nulla. Sed nisi mauris, consequat ut eros vel, mollis bibendum dolor. Aenean laoreet lacus a eros iaculis eleifend.
Fusce tempor tortor vel eleifend ornare. Maecenas euismod vitae nunc vel congue. Sed in tristique felis, at venenatis arcu. Etiam egestas sem quis accumsan aliquet. Ut arcu lorem, auctor et metus venenatis, ullamcorper pharetra turpis. Ut gravida, eros ac tristique faucibus, lorem quam sollicitudin lacus, tempus porta tortor risus non lectus. Nunc consequat in magna sed tincidunt. Nullam lacus mi, vulputate et erat eu, rutrum fermentum leo. Sed blandit lobortis nisl et auctor. Curabitur rutrum semper justo, quis commodo lacus tincidunt eget. Sed tempus velit ac malesuada finibus. Integer feugiat quam in metus elementum varius. Nam odio elit, tempus scelerisque est sed, tincidunt consequat ligula. Proin ligula neque, ornare et turpis at, hendrerit volutpat urna. Vivamus vel eleifend urna, quis fermentum dolor.
Mauris felis urna, tincidunt quis fermentum ut, consequat eu mauris. Nulla placerat venenatis molestie. Suspendisse vitae bibendum ante. Nulla lacinia hendrerit diam non feugiat. Curabitur efficitur risus in porttitor condimentum. Pellentesque tincidunt tincidunt diam, et mollis nibh consequat id. Nulla ultrices, ligula interdum convallis varius, mi odio posuere metus, id congue risus nisl at erat. Sed rhoncus, eros eget ullamcorper interdum, leo nibh condimentum neque, eu eleifend metus odio sed justo. Etiam ultricies suscipit sapien ut ornare. Praesent ultricies fringilla nisl ac fringilla. In mollis commodo massa ac bibendum. Vestibulum interdum massa ut urna ornare iaculis. Nullam lacus eros, vehicula id hendrerit sit amet, porta a nisl. Curabitur ac leo orci. In id arcu tristique, vestibulum felis non, lobortis orci. Cras ut nunc eros.
`
