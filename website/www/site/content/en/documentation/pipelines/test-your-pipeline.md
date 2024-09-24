---
title: "Test Your Pipeline"
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
# Test Your Pipeline

{{< toc >}}

Testing your pipeline is a particularly important step in developing an effective data processing solution. The indirect nature of the Beam model, in which your user code constructs a pipeline graph to be executed remotely, can make debugging failed runs a non-trivial task. Often it is faster and simpler to perform local unit testing on your pipeline code than to debug a pipeline's remote execution.

Before running your pipeline on the runner of your choice, unit testing your pipeline code locally is often the best way to identify and fix bugs in your pipeline code. Unit testing your pipeline locally also allows you to use your familiar/favorite local debugging tools.

You can use [DirectRunner](/documentation/runners/direct), a local runner helpful for testing and local development.

After you test your pipeline using the `DirectRunner`, you can use the runner of your choice to test on a small scale. For example, use the Flink runner with a local or remote Flink cluster.

The Beam SDKs provide a number of ways to unit test your pipeline code, from the lowest to the highest levels. From the lowest to the highest level, these are:

*   You can test the individual functions used in your pipeline.
*   You can test an entire [Transform](/documentation/programming-guide/#composite-transforms) as a unit.
*   You can perform an end-to-end test for an entire pipeline.

To support unit testing, the Beam SDK for Java provides a number of test classes in the [testing package](https://github.com/apache/beam/tree/master/sdks/java/core/src/test/java/org/apache/beam/sdk). You can use these tests as references and guides.

## Testing Transforms

To test a transform you've created, you can use the following pattern:

*   Create a `TestPipeline`.
*   Create some static, known test input data.
*   Use the `Create` transform to create a `PCollection` of your input data.
*   `Apply` your transform to the input `PCollection` and save the resulting output `PCollection`.
*   Use `PAssert` and its subclasses to verify that the output `PCollection` contains the elements that you expect.

### TestPipeline

{{< paragraph class="language-java" >}}
[TestPipeline](https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/testing/TestPipeline.java) is a class included in the Beam Java SDK specifically for testing transforms.
{{< /paragraph >}}
{{< paragraph class="language-py" >}}
[TestPipeline](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/testing/test_pipeline.py) is a class included in the Beam Python SDK specifically for testing transforms.
{{< /paragraph >}}
For tests, use `TestPipeline` in place of `Pipeline` when you create the pipeline object. Unlike `Pipeline.create`, `TestPipeline.create` handles setting `PipelineOptions` internally.

You create a `TestPipeline` as follows:

{{< highlight java >}}
Pipeline p = TestPipeline.create();
{{< /highlight >}}

{{< highlight py >}}
with TestPipeline as p:
    ...
{{< /highlight >}}

{{< highlight go >}}
import "github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"

// Override TestMain with ptest.Main,
// once per package. 
func TestMain(m *testing.M) {
	ptest.Main(m)
}

func TestPipeline(t *testing.T) {
	...
	// The Go SDK doesn't use a TestPipeline concept, and recommends using the ptest
	// harness to wrap pipeline construction.
	pr := ptest.BuildAndRun(t, func(s beam.Scope) {
		...
	})
	...
}
{{< /highlight >}}

> **Note:** Read about testing unbounded pipelines in Beam in [this blog post](/blog/2016/10/20/test-stream.html).

### Using the Create Transform

You can use the `Create` transform to create a `PCollection` out of a standard in-memory collection class, such as Java or Python `List`. See [Creating a PCollection](/documentation/programming-guide/#creating-a-pcollection) for more information.

### PAssert

[PAssert](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/testing/PAssert.html) is a class included in the Beam Java SDK  that is an assertion on the contents of a `PCollection`. You can use `PAssert`to verify that a `PCollection` contains a specific set of expected elements.

For a given `PCollection`, you can use `PAssert` to verify the contents as follows:

{{< highlight java >}}
PCollection<String> output = ...;

// Check whether a PCollection contains some elements in any order.
PAssert.that(output)
.containsInAnyOrder(
  "elem1",
  "elem3",
  "elem2");
{{< /highlight >}}

{{< highlight py >}}
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

output = ...

# Check whether a PCollection contains some elements in any order.
assert_that(
    output,
    equal_to(["elem1", "elem3", "elem2"]))
{{< /highlight >}}

{{< highlight go >}}
import "github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"

output := ... // beam.PCollection

// Check whether a PCollection contains some elements in any order.
passert.EqualsList(s, output, ["elem1", "elem3", "elem2"])
{{< /highlight >}}


{{< paragraph class="language-java" >}}
Any Java code that uses `PAssert` must link in `JUnit` and `Hamcrest`. If you're using Maven, you can link in `Hamcrest` by adding the following dependency to your project's `pom.xml` file:
{{< /paragraph >}}

{{< highlight java >}}
<dependency>
    <groupId>org.hamcrest</groupId>
    <artifactId>hamcrest</artifactId>
    <version>2.2</version>
    <scope>test</scope>
</dependency>
{{< /highlight >}}

For more information on how these classes work, see the [org.apache.beam.sdk.testing](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/testing/package-summary.html) package documentation.

### An Example Test for a Composite Transform

The following code shows a complete test for a composite transform. The test applies the `Count` transform to an input `PCollection` of `String` elements. The test uses the `Create` transform to create the input `PCollection` from a `List<String>`.

{{< highlight java >}}
public class CountTest {

  // Our static input data, which will make up the initial PCollection.
  static final String[] WORDS_ARRAY = new String[] {
  "hi", "there", "hi", "hi", "sue", "bob",
  "hi", "sue", "", "", "ZOW", "bob", ""};

  static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

  public void testCount() {
    // Create a test pipeline.
    Pipeline p = TestPipeline.create();

    // Create an input PCollection.
    PCollection<String> input = p.apply(Create.of(WORDS));

    // Apply the Count transform under test.
    PCollection<KV<String, Long>> output =
      input.apply(Count.<String>perElement());

    // Assert on the results.
    PAssert.that(output)
      .containsInAnyOrder(
          KV.of("hi", 4L),
          KV.of("there", 1L),
          KV.of("sue", 2L),
          KV.of("bob", 2L),
          KV.of("", 3L),
          KV.of("ZOW", 1L));

    // Run the pipeline.
    p.run();
  }
}
{{< /highlight >}}

{{< highlight py >}}
import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

class CountTest(unittest.TestCase):

  def test_count(self):
    # Our static input data, which will make up the initial PCollection.
    WORDS = [
      "hi", "there", "hi", "hi", "sue", "bob",
      "hi", "sue", "", "", "ZOW", "bob", ""
    ]
    # Create a test pipeline.
    with TestPipeline() as p:

      # Create an input PCollection.
      input = p | beam.Create(WORDS)

      # Apply the Count transform under test.
      output = input | beam.combiners.Count.PerElement()

      # Assert on the results.
      assert_that(
        output,
        equal_to([
            ("hi", 4),
            ("there", 1),
            ("sue", 2),
            ("bob", 2),
            ("", 3),
            ("ZOW", 1)]))

      # The pipeline will run and verify the results.
{{< /highlight >}}

{{< highlight go >}}
import (
  "testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
)

// formatFn takes a key value pair and puts them
// into a single string for comparison.
func formatFn(w string, c int) string { 
   return fmt.Sprintf("%s: %d", w, c)
}

// Register the functional DoFn to ensure execution on workers.
func init() {
  register.Function2x1(formatFn)
}

func TestCountWords(t *testing.T) {
  // The pipeline will run and verify the results.
  ptest.BuildAndRun(t, func(s beam.Scope) {
    words := []string{"hi", "there", "hi", "hi", "sue", "bob",
      "hi", "sue", "", "", "ZOW", "bob", ""}

    wantCounts := []string{"hi: 5", "there: 1", "sue: 2", "bob: 2"}

    // Create a PCollection from the words static input data.
    input := beam.CreateList(s, words)

    // Apply the Count transform under test.
    output := stats.Count(s, col)
    formatted := beam.ParDo(s, formatFn, output)

    // Assert that the output PCollection matches the wantCounts data.
    passert.Equals(s, formatted, wantCounts...)
  })
}
{{< /highlight >}}
## Testing a Pipeline End-to-End

You can use the test classes in the Beam SDKs (such as `TestPipeline` and `PAssert` in the Beam SDK for Java) to test an entire pipeline end-to-end. Typically, to test an entire pipeline, you do the following:

*   For every source of input data to your pipeline, create some known static test input data.
*   Create some static test output data that matches what you expect in your pipeline's final output `PCollection`(s).
*   Create a `TestPipeline` in place of the standard `Pipeline.create`.
*   In place of your pipeline's `Read` transform(s), use the `Create` transform to create one or more `PCollection`s from your static input data.
*   Apply your pipeline's transforms.
*   In place of your pipeline's `Write` transform(s), use `PAssert` to verify that the contents of the final `PCollection`s your pipeline produces match the expected values in your static output data.

### Testing the WordCount Pipeline

The following example code shows how one might test the [WordCount example pipeline](/get-started/wordcount-example/). `WordCount` usually reads lines from a text file for input data; instead, the test creates a `List<String>` containing some text lines and uses a `Create` transform to create an initial `PCollection`.

`WordCount`'s final transform (from the composite transform `CountWords`) produces a `PCollection<String>` of formatted word counts suitable for printing. Rather than write that `PCollection` to an output text file, our test pipeline uses `PAssert` to verify that the elements of the `PCollection` match those of a static `String` array containing our expected output data.

{{< highlight java >}}
public class WordCountTest {

    // Our static input data, which will comprise the initial PCollection.
    static final String[] WORDS_ARRAY = new String[] {
      "hi there", "hi", "hi sue bob",
      "hi sue", "", "bob hi"};

    static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

    // Our static output data, which is the expected data that the final PCollection must match.
    static final String[] COUNTS_ARRAY = new String[] {
        "hi: 5", "there: 1", "sue: 2", "bob: 2"};

    // Example test that tests the pipeline's transforms.

    public void testCountWords() throws Exception {
      Pipeline p = TestPipeline.create();

      // Create a PCollection from the WORDS static input data.
      PCollection<String> input = p.apply(Create.of(WORDS));

      // Run ALL the pipeline's transforms (in this case, the CountWords composite transform).
      PCollection<String> output = input.apply(new CountWords());

      // Assert that the output PCollection matches the COUNTS_ARRAY known static output data.
      PAssert.that(output).containsInAnyOrder(COUNTS_ARRAY);

      // Run the pipeline.
      p.run();
    }
}
{{< /highlight >}}

{{< highlight py >}}
import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

class CountWords(beam.PTransform):
    # CountWords transform omitted for conciseness.
    # Full transform can be found here - https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount_debugging.py

class WordCountTest(unittest.TestCase):

  # Our input data, which will make up the initial PCollection.
  WORDS = [
      "hi", "there", "hi", "hi", "sue", "bob",
      "hi", "sue", "", "", "ZOW", "bob", ""
  ]

  # Our output data, which is the expected data that the final PCollection must match.
  EXPECTED_COUNTS = ["hi: 5", "there: 1", "sue: 2", "bob: 2"]

  # Example test that tests the pipeline's transforms.

  def test_count_words(self):
    with TestPipeline() as p:

      # Create a PCollection from the WORDS static input data.
      input = p | beam.Create(WORDS)

      # Run ALL the pipeline's transforms (in this case, the CountWords composite transform).
      output = input | CountWords()

      # Assert that the output PCollection matches the EXPECTED_COUNTS data.
      assert_that(output, equal_to(EXPECTED_COUNTS), label='CheckOutput')

    # The pipeline will run and verify the results.
{{< /highlight >}}

{{< highlight go >}}
package wordcount

import (
  "testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

// CountWords and formatFn are omitted for conciseness.
// Code for the Full transforms can be found here:
// https://github.com/apache/beam/blob/master/sdks/go/examples/debugging_wordcount/debugging_wordcount.go

func CountWords(s beam.Scope, lines beam.PCollection) beam.PCollection { ... }

func formatFn(w string, c int) string { ... }

func TestCountWords(t *testing.T) {
  // The pipeline will run and verify the results.
  ptest.BuildAndRun(t, func(s beam.Scope) {
    words := []string{"hi", "there", "hi", "hi", "sue", "bob",
      "hi", "sue", "", "", "ZOW", "bob", ""}

    wantCounts := []string{"hi: 5", "there: 1", "sue: 2", "bob: 2"}

    // Create a PCollection from the words static input data.
    input := beam.CreateList(s, words)

    // Run ALL the pipeline's transforms (in this case, the CountWords composite transform).
    output := CountWords(s, input)
    formatted := beam.ParDo(s, formatFn, output)

    // Assert that the output PCollection matches the wantCounts data.
    passert.Equals(s, formatted, wantCounts...)
  })
}
{{< /highlight >}}