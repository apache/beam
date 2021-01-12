---
title: "Try Apache Beam"
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Try Apache Beam

You can try Apache Beam using our interactive notebooks, which are hosted in [Colab](https://colab.research.google.com). The notebooks allow you to interactively play with the code and see how your changes affect the pipeline. You don't need to install anything or modify your computer in any way to use these notebooks.

{{< language-switcher java py go >}}

## Interactive WordCount in Colab

This interactive notebook shows you what a simple, minimal version of WordCount looks like.

{{< highlight java >}}
package samples.quickstart;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

public class WordCount {
  public static void main(String[] args) {
    String inputsDir = "data/*";
    String outputsPrefix = "outputs/part";

    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply("Read lines", TextIO.read().from(inputsDir))
        .apply("Find words", FlatMapElements.into(TypeDescriptors.strings())
            .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
        .apply("Filter empty words", Filter.by((String word) -> !word.isEmpty()))
        .apply("Count words", Count.perElement())
        .apply("Write results", MapElements.into(TypeDescriptors.strings())
            .via((KV<String, Long> wordCount) ->
                  wordCount.getKey() + ": " + wordCount.getValue()))
        .apply(TextIO.write().to(outputsPrefix));
    pipeline.run();
  }
}
{{< /highlight >}}

{{< paragraph class="language-java" >}}
<a class="button button--primary" target="_blank"
  href="https://colab.sandbox.google.com/github/{{< param branch_repo >}}/examples/notebooks/get-started/try-apache-beam-java.ipynb">
  Run in Colab
</a>
<a class="button button--primary" target="_blank"
  href="https://github.com/{{< param branch_repo >}}/examples/notebooks/get-started/try-apache-beam-java.ipynb">
  View on GitHub
</a>
{{< /paragraph >}}

{{< paragraph class="language-java" >}}
To learn how to install and run the Apache Beam Java SDK on your own computer, follow the instructions in the <a href="/get-started/quickstart-java">Java Quickstart</a>.
{{< /paragraph >}}

{{< highlight py >}}
import apache_beam as beam
import re

inputs_pattern = 'data/*'
outputs_prefix = 'outputs/part'

with beam.Pipeline() as pipeline:
  (
      pipeline
      | 'Read lines' >> beam.io.ReadFromText(inputs_pattern)
      | 'Find words' >> beam.FlatMap(lambda line: re.findall(r"[a-zA-Z']+", line))
      | 'Pair words with 1' >> beam.Map(lambda word: (word, 1))
      | 'Group and sum' >> beam.CombinePerKey(sum)
      | 'Format results' >> beam.Map(lambda word_count: str(word_count))
      | 'Write results' >> beam.io.WriteToText(outputs_prefix)
  )
{{< /highlight >}}

{{< paragraph class="language-py" >}}
<a class="button button--primary" target="_blank"
  href="https://colab.sandbox.google.com/github/{{< param branch_repo >}}/examples/notebooks/get-started/try-apache-beam-py.ipynb">
  Run in Colab
</a>
<a class="button button--primary" target="_blank"
  href="https://github.com/{{< param branch_repo >}}/examples/notebooks/get-started/try-apache-beam-py.ipynb">
  View on GitHub
</a>
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
To learn how to install and run the Apache Beam Python SDK on your own computer, follow the instructions in the <a href="/get-started/quickstart-py">Python Quickstart</a>.
{{< /paragraph >}}

{{< highlight go >}}
package main

import (
	"context"
	"flag"
	"fmt"
	"regexp"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/direct"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"

	_ "github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/local"
)

var (
	input = flag.String("input", "data/*", "File(s) to read.")
	output = flag.String("output", "outputs/wordcounts.txt", "Output filename.")
)

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

func main() {
  flag.Parse()

	beam.Init()

	pipeline := beam.NewPipeline()
	root := pipeline.Root()

	lines := textio.Read(root, *input)
	words := beam.ParDo(root, func(line string, emit func(string)) {
		for _, word := range wordRE.FindAllString(line, -1) {
			emit(word)
		}
	}, lines)
	counted := stats.Count(root, words)
	formatted := beam.ParDo(root, func(word string, count int) string {
		return fmt.Sprintf("%s: %v", word, count)
	}, counted)
	textio.Write(root, *output, formatted)

	direct.Execute(context.Background(), pipeline)
}
{{< /highlight >}}

{{< paragraph class="language-go" >}}
<a class="button button--primary" target="_blank"
  href="https://colab.sandbox.google.com/github/{{< param branch_repo >}}/examples/notebooks/get-started/try-apache-beam-go.ipynb">
  Run in Colab
</a>
<a class="button button--primary" target="_blank"
  href="https://github.com/{{< param branch_repo >}}/examples/notebooks/get-started/try-apache-beam-go.ipynb">
  View on GitHub
</a>
{{< /paragraph >}}

{{< paragraph class="language-go" >}}
To learn how to install and run the Apache Beam Go SDK on your own computer, follow the instructions in the <a href="/get-started/quickstart-go">Go Quickstart</a>.
{{< /paragraph >}}

For a more detailed explanation about how WordCount works, see the [WordCount Example Walkthrough](/get-started/wordcount-example).

## Next Steps

* Walk through additional WordCount examples in the [WordCount Example Walkthrough](/get-started/wordcount-example).
* Take a self-paced tour through our [Learning Resources](/documentation/resources/learning-resources).
* Dive in to some of our favorite [Videos and Podcasts](/documentation/resources/videos-and-podcasts).
* Join the Beam [users@](/community/contact-us) mailing list.
* If you're interested in contributing to the Apache Beam codebase, see the [Contribution Guide](/contribute).

Please don't hesitate to [reach out](/community/contact-us) if you encounter any issues!
