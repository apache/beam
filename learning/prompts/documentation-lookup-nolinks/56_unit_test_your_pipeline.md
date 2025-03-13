Prompt:
How can I unit-test my Apache Beam pipeline?

Response:
Before deploying your Apache Beam pipeline on your chosen runner, it's crucial to conduct thorough unit tests locally. This approach is highly effective for identifying and rectifying bugs within your pipeline code, all while allowing you to leverage your preferred debugging tools.

The Apache Beam Direct Runner is particularly valuable for local testing and development. Once you've tested your pipeline with the Direct Runner, you can seamlessly transition to testing it on a small scale using your preferred runner, such as the Flink runner with either a local or remote Flink cluster.

The Apache Beam SDKs offer three levels of unit testing for pipeline code:
* ***Testing individual functions***: you can test each function used in your pipeline independently.
* ***Testing an entire transform***: this involves testing a transform as a cohesive unit.
* ***Performing an end-to-end test***: this comprehensive test evaluates the entire pipeline.

Both the Java and Python SDKs provide numerous test classes within their testing packages. The following examples demonstrate the most commonly used test classes in Java and Python.

***Testing a Transform***

1. Create a `TestPipeline`. The `TestPipeline` class, included in the Beam SDKs, is specifically designed for testing transforms. Replace `Pipeline` with `TestPipeline` when creating the pipeline object for tests. Unlike `Pipeline.create`, `TestPipeline.create` handles the setting of `PipelineOptions` internally.

Java:

```java
Pipeline p = TestPipeline.create();
```

Python:

```python
with TestPipeline as p:
    …
```

2. Create static test input data.
3. Use the `Create` transform. You can use this transform to create a `PCollection` of your input data from a standard in-memory collection class, such as Java or Python `List`.
4. Apply your transform. Apply your transform to the input `PCollection` and store the resulting output `PCollection`.
5. Verify output using `PAssert` (Java) or `assert_that` (Python). These assertion classes ensure that the output `PCollection` contains the expected elements.

Java:

```java
PCollection<String> output = ...;

// Check whether a PCollection contains some elements in any order.
PAssert.that(output)
.containsInAnyOrder(
  "elem1",
  "elem3",
  "elem2");
```

Python:

```python
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

output = ...

# Check whether a PCollection contains some elements in any order.
assert_that(
    output,
    equal_to(["elem1", "elem3", "elem2"]))
```

***Testing a Pipeline End-to-End***

To test an entire pipeline end-to-end:
* Create static test input data for each source of input data.
* Prepare static test output data matching the expected final output `PCollection`.
* Use `TestPipeline` instead of `Pipeline.create`.
* Replace the pipeline’s `Read` transforms with the `Create` transform to generate `PCollection` objects from static input data.
* Apply the pipeline’s transforms.
* Replace the pipeline’s `Write` transforms with `PAssert` (Java) or `assert_that` (Python) to verify that the final `PCollection` objects match the expected values in static output data.

The following examples demonstrate how to test the WordCount example pipeline in Java and Python using these steps.

Java:

```java
public class WordCountTest {
    // Static input data for the initial PCollection.
    static final String[] WORDS_ARRAY = new String[] {
      "hi there", "hi", "hi sue bob",
      "hi sue", "", "bob hi"};

    static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

    // Static output data, expected to match the final PCollection.
    static final String[] COUNTS_ARRAY = new String[] {
        "hi: 5", "there: 1", "sue: 2", "bob: 2"};

    // Example test for pipeline's transforms.
    public void testCountWords() throws Exception {
      Pipeline p = TestPipeline.create();

      // Create a PCollection from the static input data.
      PCollection<String> input = p.apply(Create.of(WORDS));

      // Run ALL the pipeline's transforms.
      PCollection<String> output = input.apply(new CountWords());

      // Assert that the output matches the known static output data.
      PAssert.that(output).containsInAnyOrder(COUNTS_ARRAY);

      // Execute the pipeline.
      p.run();
    }
}
```

Python:

```python
import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

class CountWords(beam.PTransform):
    # CountWords transform omitted for conciseness.

class WordCountTest(unittest.TestCase):
  # Input data for the initial PCollection.
  WORDS = [
      "hi", "there", "hi", "hi", "sue", "bob",
      "hi", "sue", "", "", "ZOW", "bob", ""
  ]

  # Expected output data to match the final PCollection.
  EXPECTED_COUNTS = ["hi: 5", "there: 1", "sue: 2", "bob: 2"]

  # Example test for pipeline's transforms.
  def test_count_words(self):
    with TestPipeline() as p:
      # Create a PCollection from the static input data.
      input = p | beam.Create(WORDS)

      # Run ALL the pipeline's transforms.
      output = input | CountWords()

      # Assert that the output matches the expected data.
      assert_that(output, equal_to(EXPECTED_COUNTS), label='CheckOutput')

    # The pipeline runs and verifies the results.
```

Typically, WordCount reads lines from a text file for input data. However, the provided sample tests create static input data containing text lines and use the `Create` transform to create an initial `PCollection`. Instead of writing that `PCollection` to an output text file, the test pipelines use `PAssert` (Java) or `assert_that` (Python) to verify that the `PCollection` elements match a static string containing expected output data.
