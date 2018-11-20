---
layout: section
title: "Test Your Pipeline"
section_menu: section-menu/documentation.html
permalink: /documentation/pipelines/test-your-pipeline/
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

* TOC
{:toc}

Testing your pipeline is a particularly important step in developing an effective data processing solution. The indirect nature of the Beam model, in which your user code constructs a pipeline graph to be executed remotely, can make debugging-failed runs a non-trivial task. Often it is faster and simpler to perform local unit testing on your pipeline code than to debug a pipeline's remote execution.

Before running your pipeline on the runner of your choice, unit testing your pipeline code locally is often the best way to identify and fix bugs in your pipeline code. Unit testing your pipeline locally also allows you to use your familiar/favorite local debugging tools.

You can use [DirectRunner]({{ site.baseurl }}/documentation/runners/direct), a local runner helpful for testing and local development.

After you test your pipeline using the `DirectRunner`, you can use the runner of your choice to test on a small scale. For example, use the Flink runner with a local or remote Flink cluster.






The Beam SDKs provide a number of ways to unit test your pipeline code, from the lowest to the highest levels. From the lowest to the highest level, these are:

*   You can test the individual function objects, such as [DoFn]({{ site.baseurl }}/documentation/programming-guide/#pardo)s, inside your pipeline's core transforms.
*   You can test an entire [Composite Transform]({{ site.baseurl }}/documentation/programming-guide/#composite-transforms) as a unit.
*   You can perform an end-to-end test for an entire pipeline.

To support unit testing, the Beam SDK for Java provides a number of test classes in the [testing package](https://github.com/apache/beam/tree/master/sdks/java/core/src/test/java/org/apache/beam/sdk). You can use these tests as references and guides.

## Testing Individual DoFn Objects

The code in your pipeline's `DoFn` functions runs often, and often across multiple Compute Engine instances. Unit-testing your `DoFn` objects before running them using a runner service can save a great deal of debugging time and energy.

The Beam SDK for Java provides a convenient way to test an individual `DoFn` called [DoFnTester](https://github.com/apache/beam/blob/master/sdks/java/core/src/test/java/org/apache/beam/sdk/transforms/DoFnTesterTest.java), which is included in the SDK `Transforms` package.

`DoFnTester`uses the [JUnit](http://junit.org) framework. To use `DoFnTester`, you'll need to do the following:

1.  Create a `DoFnTester`. You'll need to pass an instance of the `DoFn` you want to test to the static factory method for `DoFnTester`.
2.  Create one or more main test inputs of the appropriate type for your `DoFn`. If your `DoFn` takes side inputs and/or produces [multiple outputs]({{ site.baseurl }}/documentation/programming-guide#additional-outputs), you should also create the side inputs and the output tags.
3.  Call `DoFnTester.processBundle` to process the main inputs.
4.  Use JUnit's `Assert.assertThat` method to ensure the test outputs returned from `processBundle` match your expected values.

### Creating a DoFnTester

To create a `DoFnTester`, first create an instance of the `DoFn` you want to test. You then use that instance when you create a `DoFnTester` using the `.of()` static factory method:

```java
static class MyDoFn extends DoFn<String, Integer> { ... }
  MyDoFn myDoFn = ...;

  DoFnTester<String, Integer> fnTester = DoFnTester.of(myDoFn);
```

### Creating Test Inputs

You'll need to create one or more test inputs for `DoFnTester` to send to your `DoFn`. To create test inputs, simply create one or more input variables of the same input type that your `DoFn` accepts. In the case above:

```java
static class MyDoFn extends DoFn<String, Integer> { ... }
MyDoFn myDoFn = ...;
DoFnTester<String, Integer> fnTester = DoFnTester.of(myDoFn);

String testInput = "test1";
```

#### Side Inputs

If your `DoFn` accepts side inputs, you can create those side inputs by using the method `DoFnTester.setSideInputs`.

```java
static class MyDoFn extends DoFn<String, Integer> { ... }
MyDoFn myDoFn = ...;
DoFnTester<String, Integer> fnTester = DoFnTester.of(myDoFn);

PCollectionView<List<Integer>> sideInput = ...;
Iterable<Integer> value = ...;
fnTester.setSideInputInGlobalWindow(sideInput, value);
```

See the `ParDo` documentation on [side inputs]({{ site.baseurl }}/documentation/programming-guide/#side-inputs) for more information.

#### Additional Outputs

If your `DoFn` produces multiple output `PCollection`s, you'll need to set the
appropriate `TupleTag` objects that you'll use to access each output. A `DoFn`
with multiple outputs produces a `PCollectionTuple` for each output; you'll need
to provide a `TupleTagList` that corresponds to each output in that tuple.

Suppose your `DoFn` produces outputs of type `String` and `Integer`. You create
`TupleTag` objects for each, and bundle them into a `TupleTagList`, then set it
for the `DoFnTester` as follows:

```java
static class MyDoFn extends DoFn<String, Integer> { ... }
MyDoFn myDoFn = ...;
DoFnTester<String, Integer> fnTester = DoFnTester.of(myDoFn);

TupleTag<String> tag1 = ...;
TupleTag<Integer> tag2 = ...;
TupleTagList tags = TupleTagList.of(tag1).and(tag2);

fnTester.setOutputTags(tags);
```

See the `ParDo` documentation on [additional outputs]({{ site.baseurl }}/documentation/programming-guide/#additional-outputs) for more information.

### Processing Test Inputs and Checking Results

To process the inputs (and thus run the test on your `DoFn`), you call the method `DoFnTester.processBundle`. When you call `processBundle`, you pass one or more main test input values for your `DoFn`. If you set side inputs, the side inputs are available to each batch of main inputs that you provide.

`DoFnTester.processBundle` returns a `List` of outputs—that is, objects of the same type as the `DoFn`'s specified output type. For a `DoFn<String, Integer>`, `processBundle` returns a `List<Integer>`:

```java  
static class MyDoFn extends DoFn<String, Integer> { ... }
MyDoFn myDoFn = ...;
DoFnTester<String, Integer> fnTester = DoFnTester.of(myDoFn);

String testInput = "test1";
List<Integer> testOutputs = fnTester.processBundle(testInput);
```

To check the results of `processBundle`, you use JUnit's `Assert.assertThat` method to test if the `List` of outputs contains the values you expect:

```java  
String testInput = "test1";
List<Integer> testOutputs = fnTester.processBundle(testInput);

Assert.assertThat(testOutputs, Matchers.hasItems(...));

// Process a larger batch in a single step.
Assert.assertThat(fnTester.processBundle("input1", "input2", "input3"), Matchers.hasItems(...));
```

## Testing Composite Transforms

To test a composite transform you've created, you can use the following pattern:

*   Create a `TestPipeline`.
*   Create some static, known test input data.
*   Use the `Create` transform to create a `PCollection` of your input data.
*   `Apply` your composite transform to the input `PCollection` and save the resulting output `PCollection`.
*   Use `PAssert` and its subclasses to verify that the output `PCollection` contains the elements that you expect.

### TestPipeline

[TestPipeline](https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/testing/TestPipeline.java) is a class included in the Beam Java SDK specifically for testing transforms. For tests, use `TestPipeline` in place of `Pipeline` when you create the pipeline object. Unlike `Pipeline.create`, `TestPipeline.create` handles setting `PipelineOptions` interally.

You create a `TestPipeline` as follows:

```java
Pipeline p = TestPipeline.create();
```

> **Note:** Read about testing unbounded pipelines in Beam in [this blog post]({{ site.baseurl }}/blog/2016/10/20/test-stream.html).

### Using the Create Transform

You can use the `Create` transform to create a `PCollection` out of a standard in-memory collection class, such as Java `List`. See [Creating a PCollection]({{ site.baseurl }}/documentation/programming-guide/#creating-a-pcollection) for more information.

### PAssert
[PAssert](https://beam.apache.org/releases/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/testing/PAssert.html) is a class included in the Beam Java SDK  that is an assertion on the contents of a `PCollection`. You can use `PAssert`to verify that a `PCollection` contains a specific set of expected elements.

For a given `PCollection`, you can use `PAssert` to verify the contents as follows:

```java  
PCollection<String> output = ...;

// Check whether a PCollection contains some elements in any order.
PAssert.that(output)
.containsInAnyOrder(
  "elem1",
  "elem3",
  "elem2");
```

Any code that uses `PAssert` must link in `JUnit` and `Hamcrest`. If you're using Maven, you can link in `Hamcrest` by adding the following dependency to your project's `pom.xml` file:

```java
<dependency>
    <groupId>org.hamcrest</groupId>
    <artifactId>hamcrest-all</artifactId>
    <version>1.3</version>
    <scope>test</scope>
</dependency>
```

For more information on how these classes work, see the [org.apache.beam.sdk.testing](https://beam.apache.org/releases/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/testing/package-summary.html) package documentation.

### An Example Test for a Composite Transform

The following code shows a complete test for a composite transform. The test applies the `Count` transform to an input `PCollection` of `String` elements. The test uses the `Create` transform to create the input `PCollection` from a Java `List<String>`.

```java  
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
  PCollection<String> input = p.apply(Create.of(WORDS)).setCoder(StringUtf8Coder.of());

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
```

## Testing a Pipeline End-to-End

You can use the test classes in the Beam SDKs (such as `TestPipeline` and `PAssert` in the Beam SDK for Java) to test an entire pipeline end-to-end. Typically, to test an entire pipeline, you do the following:

*   For every source of input data to your pipeline, create some known static test input data.
*   Create some static test output data that matches what you expect in your pipeline's final output `PCollection`(s).
*   Create a `TestPipeline` in place of the standard `Pipeline.create`.
*   In place of your pipeline's `Read` transform(s), use the `Create` transform to create one or more `PCollection`s from your static input data.
*   Apply your pipeline's transforms.
*   In place of your pipeline's `Write` transform(s), use `PAssert` to verify that the contents of the final `PCollection`s your pipeline produces match the expected values in your static output data.

### Testing the WordCount Pipeline

The following example code shows how one might test the [WordCount example pipeline]({{ site.baseurl }}/get-started/wordcount-example/). `WordCount` usually reads lines from a text file for input data; instead, the test creates a Java `List<String>` containing some text lines and uses a `Create` transform to create an initial `PCollection`.

`WordCount`'s final transform (from the composite transform `CountWords`) produces a `PCollection<String>` of formatted word counts suitable for printing. Rather than write that `PCollection` to an output text file, our test pipeline uses `PAssert` to verify that the elements of the `PCollection` match those of a static `String` array containing our expected output data.

```java
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
      PCollection<String> input = p.apply(Create.of(WORDS)).setCoder(StringUtf8Coder.of());

      // Run ALL the pipeline's transforms (in this case, the CountWords composite transform).
      PCollection<String> output = input.apply(new CountWords());

      // Assert that the output PCollection matches the COUNTS_ARRAY known static output data.
      PAssert.that(output).containsInAnyOrder(COUNTS_ARRAY);

      // Run the pipeline.
      p.run();
    }
}
```
