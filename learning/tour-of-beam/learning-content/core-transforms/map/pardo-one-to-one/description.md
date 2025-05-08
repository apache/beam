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

`ParDo` is a Beam transform for generic parallel processing. The `ParDo` processing paradigm is similar to the “Map” phase of a `Map/Shuffle/Reduce`-style algorithm: a `ParDo` transform considers each element in the input `PCollection`, performs some processing function (your user code) on that element, and emits zero, one, or multiple elements to an output `PCollection`.

`ParDo` is useful for a variety of common data processing operations, including:

→ Filtering a data set. You can use `ParDo` to consider each element in a `PCollection` and either output that element to a new collection or discard it.

→ Formatting or type-converting each element in a data set. If your input `PCollection` contains elements that are of a different type or format than you want, you can use `ParDo` to perform a conversion on each element and output the result to a new `PCollection`.

→ Extracting parts of each element in a data set. If you have a `PCollection` of records with multiple fields, for example, you can use a `ParDo` to parse out just the fields you want to consider into a new `PCollection`.

→ Performing computations on each element in a data set. You can use `ParDo` to perform simple or complex computations on every element, or certain elements, of a PCollection and output the results as a new `PCollection`.

In such roles, `ParDo` is a common intermediate step in a pipeline. You might use it to extract certain fields from a set of raw input records, or convert raw input into a different format; you might also use `ParDo` to convert processed data into a format suitable for output, like database table rows or printable strings.

When you apply a `ParDo` transform, you’ll need to provide user code in the form of a DoFn object. `DoFn` is a Beam SDK class that defines a distributed processing function.
{{if (eq .Sdk "go")}}
All `DoFns` should be registered using a generic register.DoFnXxY[...] function. This allows the Go SDK to infer an encoding from any inputs/outputs, registers the `DoFn` for execution on remote runners, and optimizes the runtime execution of the `DoFns` via reflection.

```
// ComputeWordLengthFn is a DoFn that computes the word length of string elements.
type ComputeWordLengthFn struct{}

// ProcessElement computes the length of word and emits the result.
// When creating structs as a DoFn, the ProcessElement method performs the
// work of this step in the pipeline.
func (fn *ComputeWordLengthFn) ProcessElement(ctx context.Context, word string) int {
   ...
}

func init() {
  // 2 inputs and 1 output => DoFn2x1
  // Input/output types are included in order in the brackets
	register.DoFn2x1[context.Context, string, int](&ComputeWordLengthFn{})
}
```
{{end}}

### Applying ParDo

`beam.ParDo` applies the passed in `DoFn` argument to the input `PCollection`, as shown in the following example code:

{{if (eq .Sdk "go")}}
```
// ComputeWordLengthFn is the DoFn to perform on each element in the input PCollection.
type ComputeWordLengthFn struct{}

// ProcessElement is the method to execute for each element.
func (fn *ComputeWordLengthFn) ProcessElement(word string, emit func(int)) {
	emit(len(word))
}

// DoFns must be registered with beam.
func init() {
	beam.RegisterType(reflect.TypeOf((*ComputeWordLengthFn)(nil)))
	// 2 inputs and 0 outputs => DoFn2x0
	// 1 input => Emitter1
	// Input/output types are included in order in the brackets
	register.DoFn2x0[string, func(int)](&ComputeWordLengthFn{})
	register.Emitter1[int]()
}


// [input] is an input PCollection of strings
var input beam.PCollection = ...

wordLengths := beam.ParDo(s, &ComputeWordLengthFn{}, input)
```
In the example, our input `PCollection` contains string values. We apply a `ParDo` transform that specifies a function (ComputeWordLengthFn) to compute the length of each string, and outputs the result to a new `PCollection` of int values that stores the length of each word.
{{end}}

{{if (eq .Sdk "java")}}

```
// The input PCollection of Strings.
PCollection<String> input = ...;

// The DoFn to perform on each element in the input PCollection.
static class ComputeWordLengthFn extends DoFn<String, Integer> { ... }

// Apply a ParDo to the PCollection "input" to compute lengths for each word.
PCollection<Integer> wordLengths = input.apply(
    ParDo
    .of(new ComputeWordLengthFn()));        // The DoFn to perform on each element, which
                                            // we define above.
```
{{end}}

{{if (eq .Sdk "python")}}
```
# The input PCollection of Strings.
input = ...

# The DoFn to perform on each element in the input PCollection.

class ComputeWordLengthFn(beam.DoFn):
  def process(self, element):
    return [len(element)]



# Apply a ParDo to the PCollection [input] to compute lengths for each word.
word_lengths = input | beam.ParDo(ComputeWordLengthFn())
```
{{end}}
### Creating a DoFn

The `DoFn` object that you pass to `ParDo` contains the processing logic that gets applied to the elements in the input collection. When you use Beam, often the most important pieces of code you’ll write are these `DoFn`s - they’re what define your pipeline’s exact data processing tasks.

> Note: When you create your `DoFn`, be mindful of the Requirements for writing user code for Beam transforms and ensure that your code follows them.

{{if (eq .Sdk "go")}}
A `DoFn` processes one element at a time from the input `PCollection`. When you create a `DoFn` struct, you’ll need to provide type parameters that match the types of the input and output elements in a ProcessElement method. If your DoFn processes incoming string elements and produces int elements for the output collection (like our previous example, ComputeWordLengthFn), your dofn could look like this:

```
// ComputeWordLengthFn is a DoFn that computes the word length of string elements.
type ComputeWordLengthFn struct{}

// ProcessElement computes the length of word and emits the result.
// When creating structs as a DoFn, the ProcessElement method performs the
// work of this step in the pipeline.
func (fn *ComputeWordLengthFn) ProcessElement(word string, emit func(int)) {
   ...
}

func init() {
  // 2 inputs and 0 outputs => DoFn2x0
  // 1 input => Emitter1
  // Input/output types are included in order in the brackets
	register.Function2x0(&ComputeWordLengthFn{})
	register.Emitter1[int]()
}
```

For your `DoFn` type, you’ll write a method `ProcessElement` where you provide the actual processing logic. You don’t need to manually extract the elements from the input collection; the Beam SDKs handle that for you. Your `ProcessElement` method should accept a parameter element, which is the input element. In order to output elements, the method can also take a function parameter, which can be called to emit elements. The parameter types must match the input and output types of your `DoFn` or the framework will raise an error.

```
// ComputeWordLengthFn is the DoFn to perform on each element in the input PCollection.
type ComputeWordLengthFn struct{}

// ProcessElement is the method to execute for each element.
func (fn *ComputeWordLengthFn) ProcessElement(word string, emit func(int)) {
	emit(len(word))
}

// DoFns must be registered with beam.
func init() {
	beam.RegisterType(reflect.TypeOf((*ComputeWordLengthFn)(nil)))
	// 2 inputs and 0 outputs => DoFn2x0
	// 1 input => Emitter1
	// Input/output types are included in order in the brackets
	register.DoFn2x0[string, func(int)](&ComputeWordLengthFn{})
	register.Emitter1[int]()
}
```

Simple DoFns can also be written as functions.

```
func ComputeWordLengthFn(word string, emit func(int)) { ... }

func init() {
  // 2 inputs and 0 outputs => DoFn2x0
  // 1 input => Emitter1
  // Input/output types are included in order in the brackets
  register.DoFn2x0[string, func(int)](&ComputeWordLengthFn{})
  register.Emitter1[int]()
}
```
{{end}}

{{if (eq .Sdk "java")}}
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
{{end}}

{{if (eq .Sdk "python")}}
Inside your `DoFn` subclass, you’ll write a method process where you provide the actual processing logic. You don’t need to manually extract the elements from the input collection; the Beam SDKs handle that for you. Your process method should accept an argument element, which is the input element, and return an iterable with its output values. You can accomplish this by emitting individual elements with yield statements. You can also use a return statement with an iterable, like a list or a generator.

```
class ComputeWordLengthFn(beam.DoFn):
  def process(self, element):
    return [len(element)]
```

{{end}}

> Note: Whether using a structural `DoFn` type or a functional `DoFn`, they should be registered with beam in an init block. Otherwise they may not execute on distributed runners.

> Note: If the elements in your input PCollection are key/value pairs, your process element method must have two parameters, for each of the key and value, respectively. Similarly, key/value pairs are also output as separate parameters to a single emitter function.

A given `DoFn` instance generally gets invoked one or more times to process some arbitrary bundle of elements. However, Beam doesn’t guarantee an exact number of invocations; it may be invoked multiple times on a given worker node to account for failures and retries. As such, you can cache information across multiple calls to your processing method, but if you do so, make sure the implementation does not depend on the number of invocations.

In your processing method, you’ll also need to meet some immutability requirements to ensure that Beam and the processing back-end can safely serialize and cache the values in your pipeline. Your method should meet the following requirements:

→ You should not in any way modify the parameters provided to the `ProcessElement` method, or any side inputs.
→ Once you output a value using an emitter function, you should not modify that value in any way.

{{if (eq .Sdk "go")}}
### Lightweight DoFns and other abstractions

If your function is relatively straightforward, you can simplify your use of `ParDo` by providing a lightweight `DoFn` in-line, as an anonymous function .

Here’s the previous example, `ParDo` with `ComputeLengthWordsFn`, with the DoFn specified as an anonymous function :

```
// [input] is the input PCollection of strings
var input beam.PCollection = ...

lengths := beam.ParDo(s, func (word string, emit func(int)) {
      emit(len(word))
}, input)
```

If your ParDo performs a one-to-one mapping of input elements to output elements–that is, for each input element, it applies a function that produces exactly one output element, you can return that element directly.
Here’s the previous example using a direct return:

```
// [input] is the input PCollection of strings
var input beam.PCollection = ...


// Apply an anonymous function as a DoFn PCollection [input].
// Save the result as the PCollection wordLengths.
wordLengths := beam.ParDo(s, func(word string) int {
	return len(word)
}, input)
```

> Note: You can use Java 8 lambda functions with several other Beam transforms, including Filter, FlatMapElements, and Partition.

> Note: Anonymous function DoFns may not work on distributed runners. It’s recommended to use named functions and register them with register.FunctionXxY in an init() block.

{{end}}

{{if (eq .Sdk "java")}}
### Accessing additional parameters in your DoFn

In addition to the element and the `OutputReceiver`, Beam will populate other parameters to your DoFn’s `@ProcessElement` method. Any combination of these parameters can be added to your process method in any order.

**Timestamp**: To access the timestamp of an input element, add a parameter annotated with `@Timestamp` of type `Instant`. For example:

```
.of(new DoFn<String, String>() {
     public void processElement(@Element String word, @Timestamp Instant timestamp) {
  }})
```

**Window**: To access the window an input element falls into, add a parameter of the type of the window used for the input `PCollection`. If the parameter is a window type (a subclass of `BoundedWindow`) that does not match the input `PCollection`, then an error will be raised. If an element falls in multiple windows (for example, this will happen when using `SlidingWindows`), then the `@ProcessElement` method will be invoked multiple time for the element, once for each window. For example, when fixed windows are being used, the window is of type `IntervalWindow`.

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

`@OnTimer` methods can also access many of these parameters. Timestamp, Window, key, `PipelineOptions`, `OutputReceiver`, and `MultiOutputReceiver` parameters can all be accessed in an `@OnTimer` method. In addition, an `@OnTimer` method can take a parameter of type `TimeDomain` which tells whether the timer is based on event time or processing time. Timers are explained in more detail in the [Timely (and Stateful) Processing with Apache Beam blog post](https://beam.apache.org/blog/timely-processing/).


{{end}}
{{if (eq .Sdk "python")}}
### Accessing additional parameters in your DoFn

In addition to the element, Beam will populate other parameters to your DoFn’s process method. Any combination of these parameters can be added to your process method in any order.

**Timestamp**: To access the timestamp of an input element, add a keyword parameter default to DoFn.TimestampParam. For example:

```
import apache_beam as beam

class ProcessRecord(beam.DoFn):
  def process(self, element, timestamp=beam.DoFn.TimestampParam):
     # access timestamp of element.
     pass
```

**Window**: To access the window an input element falls into, add a keyword parameter default to `DoFn.WindowParam`. If an element falls in multiple windows (for example, this will happen when using `SlidingWindows`), then the process method will be invoked multiple time for the element, once for each window.

```
import apache_beam as beam

class ProcessRecord(beam.DoFn):
  def process(self, element, window=beam.DoFn.WindowParam):
     # access window e.g. window.end.micros
     pass
```

**PaneInfo**: When triggers are used, Beam provides a `DoFn.PaneInfoParam` object that contains information about the current firing. Using `DoFn.PaneInfoParam` you can determine whether this is an early or a late firing, and how many times this window has already fired for this key.

```
import apache_beam as beam

class ProcessRecord(beam.DoFn):
  def process(self, element, pane_info=beam.DoFn.PaneInfoParam):
     # access pane info, e.g. pane_info.is_first, pane_info.is_last, pane_info.timing
     pass
```

**Timer and State**: In addition to aforementioned parameters, user defined Timer and State parameters can be used in a stateful `DoFn`. Timers and States are explained in more detail in the Timely (and Stateful) Processing with Apache Beam blog post.

```
class StatefulDoFn(beam.DoFn):
  """An example stateful DoFn with state and timer"""
  BUFFER_STATE_1 = BagStateSpec('buffer1', beam.BytesCoder())
  BUFFER_STATE_2 = BagStateSpec('buffer2', beam.VarIntCoder())
  WATERMARK_TIMER = TimerSpec('watermark_timer', TimeDomain.WATERMARK)

  def process(self,
              element,
              timestamp=beam.DoFn.TimestampParam,
              window=beam.DoFn.WindowParam,
              buffer_1=beam.DoFn.StateParam(BUFFER_STATE_1),
              buffer_2=beam.DoFn.StateParam(BUFFER_STATE_2),
              watermark_timer=beam.DoFn.TimerParam(WATERMARK_TIMER)):

    # Do your processing here
    key, value = element
    # Read all the data from buffer1
    all_values_in_buffer_1 = [x for x in buffer_1.read()]

    if StatefulDoFn._is_clear_buffer_1_required(all_values_in_buffer_1):
        # clear the buffer data if required conditions are met.
        buffer_1.clear()

    # add the value to buffer 2
    buffer_2.add(value)

    if StatefulDoFn._all_condition_met():
      # Clear the timer if certain condition met and you don't want to trigger
      # the callback method.
      watermark_timer.clear()

    yield element

  @on_timer(WATERMARK_TIMER)
  def on_expiry_1(self,
                  timestamp=beam.DoFn.TimestampParam,
                  window=beam.DoFn.WindowParam,
                  key=beam.DoFn.KeyParam,
                  buffer_1=beam.DoFn.StateParam(BUFFER_STATE_1),
                  buffer_2=beam.DoFn.StateParam(BUFFER_STATE_2)):
    # Window and key parameters are really useful especially for debugging issues.
    yield 'expired1'

  @staticmethod
  def _all_condition_met():
      # some logic
      return True

  @staticmethod
  def _is_clear_buffer_1_required(buffer_1_data):
      # Some business logic
      return True
```
{{end}}
### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

You can work with any type of object. For example `String`:

{{if (eq .Sdk "go")}}
```
input := beam.Create(s, "Hello World! How are you?")
output := ApplyTransform(s, input)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	split := beam.ParDo(s, SplitFn, input)
	return stats.Count(s, split)
}
```
{{end}}

{{if (eq .Sdk "java")}}
```
input.apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String element = c.element();
                for (String word : element.split(" ")) {
                    c.output(word);
                }
            }
 }));
```
{{end}}
{{if (eq .Sdk "python")}}
```
class SplitFn(DoFn):
    def process(self, element):
        return element.split()

def ApplyTransform(s, input_pcoll):
    split_pcoll = input_pcoll | beam.ParDo(SplitFn())
    return split_pcoll | stats.Count()

input = beam.Create(s, ["Hello World! How are you?"])
output = ApplyTransform(s, input)
```
{{end}}