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
# ParDo one-to-one

`ParDo` is a Beam transform for generic parallel processing. The `ParDo` processing paradigm is similar to the “Map” phase of a Map/Shuffle/Reduce-style algorithm: a `ParDo` transform considers each element in the input PCollection, performs some processing function (your user code) on that element, and emits zero, one, or multiple elements to an output `PCollection`.

`ParDo` is useful for a variety of common data processing operations, including:

&#8594; Filtering a data set. You can use `ParDo` to consider each element in a `PCollection` and either output that element to a new collection or discard it.

&#8594; Formatting or type-converting each element in a data set. If your input `PCollection` contains elements that are of a different type or format than you want, you can use `ParDo` to perform a conversion on each element and output the result to a new `PCollection`.

&#8594; Extracting parts of each element in a data set. If you have a PCollection of records with multiple fields, for example, you can use a ParDo to parse out just the fields you want to consider into a new `PCollection`.

&#8594; Performing computations on each element in a data set. You can use `ParDo` to perform simple or complex computations on every element, or certain elements, of a PCollection and output the results as a new `PCollection`.

In such roles, `ParDo` is a common intermediate step in a pipeline. You might use it to extract certain fields from a set of raw input records, or convert raw input into a different format; you might also use `ParDo` to convert processed data into a format suitable for output, like database table rows or printable strings.

When you apply a ParDo transform, you’ll need to provide user code in the form of a `DoFn` object. `DoFn` is a Beam SDK class that defines a distributed processing function.

When you create a subclass of `DoFn`, note that your subclass should adhere to the Requirements for writing user code for Beam transforms.


### Applying ParDo

Like all Beam transforms, you apply `ParDo` by calling the apply method on the input `PCollection` and passing `ParDo` as an argument, as shown in the following example code:
```
// The input PCollection of Strings.
PCollection<String> words = ...;

// The DoFn to perform on each element in the input PCollection.
static class ComputeWordLengthFn extends DoFn<String, Integer> { ... }

// Apply a ParDo to the PCollection "words" to compute lengths for each word.
PCollection<Integer> wordLengths = words.apply(
    ParDo
    .of(new ComputeWordLengthFn()));        // The DoFn to perform on each element, which
                                            // we define above.
```

In the example, our input `PCollection` contains String values. We apply a `ParDo` transform that specifies a function (ComputeWordLengthFn) to compute the length of each string, and outputs the result to a new `PCollection` of Integer values that stores the length of each word.


### Creating a DoFn

The `DoFn` object that you pass to `ParDo` contains the processing logic that gets applied to the elements in the input collection. When you use Beam, often the most important pieces of code you’ll write are these DoFns - they’re what define your pipeline’s exact data processing tasks.


> Note: When you create your `DoFn`, be mindful of the Requirements for writing user code for Beam transforms and ensure that your code follows them.


A `DoFn` processes one element at a time from the input `PCollection`. When you create a subclass of `DoFn`, you’ll need to provide type parameters that match the types of the input and output elements. If your `DoFn` processes incoming `String` elements and produces `Integer` elements for the output collection (like our previous example, ComputeWordLengthFn), your class declaration would look like this:

```
static class ComputeWordLengthFn extends DoFn<String, Integer> { ... }
```

Inside your `DoFn` subclass, you’ll write a method annotated with `@ProcessElement` where you provide the actual processing logic. You don’t need to manually extract the elements from the input collection; the Beam SDKs handle that for you. Your `@ProcessElement` method should accept a parameter tagged with `@Element`, which will be populated with the input element. In order to output elements, the method can also take a parameter of type `OutputReceiver` which provides a method for emitting elements. The parameter types must match the input and output types of your `DoFn` or the framework will raise an error.
Note: `@Element` and `OutputReceiver` were introduced in Beam 2.5.0; if using an earlier release of Beam, a `ProcessContext` parameter should be used instead.

```
static class ComputeWordLengthFn extends DoFn<String, Integer> {
  @ProcessElement
  public void processElement(@Element String word, OutputReceiver<Integer> out) {
    // Use OutputReceiver.output to emit the output element.
    out.output(word.length());
  }
}
```

> Note: Whether using a structural DoFn type or a functional `DoFn`, they should be registered with beam in an init block. Otherwise, they may not execute on distributed runners.

> Note: If the elements in your input `PCollection` are key/value pairs, you can access the key or value by using `element.getKey()` or `element.getValue()`, respectively.


A given `DoFn` instance generally gets invoked one or more times to process some arbitrary bundle of elements. However, Beam doesn’t guarantee an exact number of invocations; it may be invoked multiple times on a given worker node to account for failures and retries. As such, you can cache information across multiple calls to your processing method, but if you do so, make sure the implementation does not depend on the number of invocations.

In your processing method, you’ll also need to meet some immutability requirements to ensure that Beam and the processing back-end can safely serialize and cache the values in your pipeline. Your method should meet the following requirements:

&#8594; You should not in any way modify an element returned by the `@Element` annotation or `ProcessContext.sideInput()` (the incoming elements from the input collection).
&#8594; Once you output a value using `OutputReceiver.output()` you should not modify that value in any way.


### Accessing additional parameters in your DoFn

In addition to the element and the OutputReceiver, Beam will populate other parameters to your DoFn’s @ProcessElement method. Any combination of these parameters can be added to your process method in any order.

**Timestamp**: To access the timestamp of an input element, add a parameter annotated with @Timestamp of type Instant. For example:

```
.of(new DoFn<String, String>() {
     public void processElement(@Element String word, @Timestamp Instant timestamp) {
  }})
```

**Window**: To access the window an input element falls into, add a parameter of the type of the window used for the input `PCollection`. If the parameter is a window type (a subclass of BoundedWindow) that does not match the input `PCollection`, then an error will be raised. If an element falls in multiple windows (for example, this will happen when using `SlidingWindows`), then the `@ProcessElement` method will be invoked multiple time for the element, once for each window. For example, when fixed windows are being used, the window is of type `IntervalWindow`.

```
.of(new DoFn<String, String>() {
     public void processElement(@Element String word, IntervalWindow window) {
  }})
```

**PaneInfo**: When triggers are used, Beam provides a `PaneInfo` object that contains information about the current firing. Using `PaneInfo` you can determine whether this is an early or a late firing, and how many times this window has already fired for this key.

```
.of(new DoFn<String, String>() {
     public void processElement(@Element String word, PaneInfo paneInfo) {
  }})
```

**PipelineOptions**: The `PipelineOptions` for the current pipeline can always be accessed in a process method by adding it as a parameter:

```
.of(new DoFn<String, String>() {
     public void processElement(@Element String word, PipelineOptions options) {
  }})
```

`@OnTimer` methods can also access many of these parameters. Timestamp, Window, key, `PipelineOptions`, `OutputReceiver`, and `MultiOutputReceiver` parameters can all be accessed in an @OnTimer method. In addition, an `@OnTimer` method can take a parameter of type `TimeDomain` which tells whether the timer is based on event time or processing time. Timers are explained in more detail in the Timely (and Stateful) Processing with Apache Beam blog post.



### Description for example

There are `PCollection` elements at the input. the `applyTransform()` function will return the elements multiplied by 10.