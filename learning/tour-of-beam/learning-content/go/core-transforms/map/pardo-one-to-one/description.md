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

&#8594; Filtering a data set. You can use ParDo to consider each element in a `PCollection` and either output that element to a new collection or discard it.

&#8594; Formatting or type-converting each element in a data set. If your input `PCollection` contains elements that are of a different type or format than you want, you can use `ParDo` to perform a conversion on each element and output the result to a new `PCollection`.

&#8594; Extracting parts of each element in a data set. If you have a `PCollection` of records with multiple fields, for example, you can use a ParDo to parse out just the fields you want to consider into a new `PCollection`.

&#8594; Performing computations on each element in a data set. You can use ParDo to perform simple or complex computations on every element, or certain elements, of a PCollection and output the results as a new `PCollection`.

In such roles, `ParDo` is a common intermediate step in a pipeline. You might use it to extract certain fields from a set of raw input records, or convert raw input into a different format; you might also use `ParDo` to convert processed data into a format suitable for output, like database table rows or printable strings.

When you apply a `ParDo` transform, you’ll need to provide user code in the form of a DoFn object. DoFn is a Beam SDK class that defines a distributed processing function.

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

### Applying ParDo

`beam.ParDo` applies the passed in `DoFn` argument to the input `PCollection`, as shown in the following example code:

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


// words is an input PCollection of strings
var words beam.PCollection = ...

wordLengths := beam.ParDo(s, &ComputeWordLengthFn{}, words)
```

In the example, our input `PCollection` contains string values. We apply a `ParDo` transform that specifies a function (ComputeWordLengthFn) to compute the length of each string, and outputs the result to a new `PCollection` of int values that stores the length of each word.

### Creating a DoFn

The `DoFn` object that you pass to `ParDo` contains the processing logic that gets applied to the elements in the input collection. When you use Beam, often the most important pieces of code you’ll write are these `DoFn`s - they’re what define your pipeline’s exact data processing tasks.

> Note: When you create your `DoFn`, be mindful of the Requirements for writing user code for Beam transforms and ensure that your code follows them.

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

> Note: Whether using a structural `DoFn` type or a functional `DoFn`, they should be registered with beam in an init block. Otherwise they may not execute on distributed runners.

> Note: If the elements in your input PCollection are key/value pairs, your process element method must have two parameters, for each of the key and value, respectively. Similarly, key/value pairs are also output as separate parameters to a single emitter function.

A given `DoFn` instance generally gets invoked one or more times to process some arbitrary bundle of elements. However, Beam doesn’t guarantee an exact number of invocations; it may be invoked multiple times on a given worker node to account for failures and retries. As such, you can cache information across multiple calls to your processing method, but if you do so, make sure the implementation does not depend on the number of invocations.

In your processing method, you’ll also need to meet some immutability requirements to ensure that Beam and the processing back-end can safely serialize and cache the values in your pipeline. Your method should meet the following requirements:

&#8594; You should not in any way modify the parameters provided to the `ProcessElement` method, or any side inputs.
&#8594; Once you output a value using an emitter function, you should not modify that value in any way.

### Lightweight DoFns and other abstractions

If your function is relatively straightforward, you can simplify your use of `ParDo` by providing a lightweight `DoFn` in-line, as an anonymous function .

Here’s the previous example, `ParDo` with `ComputeLengthWordsFn`, with the DoFn specified as an anonymous function :

```
// words is the input PCollection of strings
var words beam.PCollection = ...

lengths := beam.ParDo(s, func (word string, emit func(int)) {
      emit(len(word))
}, words)   
```

If your ParDo performs a one-to-one mapping of input elements to output elements–that is, for each input element, it applies a function that produces exactly one output element, you can return that element directly.
Here’s the previous example using a direct return:

```
// words is the input PCollection of strings
var words beam.PCollection = ...


// Apply an anonymous function as a DoFn PCollection words.
// Save the result as the PCollection wordLengths.
wordLengths := beam.ParDo(s, func(word string) int {
	return len(word)
}, words)
```

> Note: You can use Java 8 lambda functions with several other Beam transforms, including Filter, FlatMapElements, and Partition.


> Note: Anonymous function DoFns may not work on distributed runners. It’s recommended to use named functions and register them with register.FunctionXxY in an init() block.


### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

The `applyTransform()` function will return elements multiplied by 10. The `multiplyBy10Fn` is set explicitly, we can change the logic without complicating readability.

If the logic of the conversion is easy we can write as an anonymous function:
```
func applyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
  return beam.ParDo(s, func(element int) int{
  	return element * 10
  }, input)
}
```

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.