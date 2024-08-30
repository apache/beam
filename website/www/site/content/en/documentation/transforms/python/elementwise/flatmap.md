---
title: "FlatMap"
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

# FlatMap

{{< localstorage language language-py >}}

{{< button-pydoc path="apache_beam.transforms.core" class="FlatMap" >}}

Applies a simple 1-to-many mapping function over each element in the collection.
The many elements are flattened into the resulting collection.

## Examples

In the following examples, we create a pipeline with a `PCollection` of produce with their icon, name, and duration.
Then, we apply `FlatMap` in multiple ways to yield zero or more elements per each input element into the resulting `PCollection`.

`FlatMap` accepts a function that returns an `iterable`,
where each of the output `iterable`'s elements is an element of the resulting `PCollection`.

### Example 1: FlatMap with a predefined function

We use the function `str.split` which takes a single `str` element and outputs a `list` of `str`s.
This pipeline splits the input element using whitespaces, creating a list of zero or more elements.

{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_FlatMapSimple" show="flatmap_simple" >}}
{{< /playground >}}

### Example 2: FlatMap with a function

We define a function `split_words` which splits an input `str` element using the delimiter `','` and outputs a `list` of `str`s.

{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_FlatMapFunction" show="flatmap_function" >}}
{{< /playground >}}

### Example 3: FlatMap without a function

A common use case of `FlatMap` is to flatten a `PCollection` of iterables into a `PCollection` of elements. To do that, don't specify the function argument to `FlatMap`, which uses the identity mapping function.

{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_FlatMapNoFunction" show="flatmap_nofunction" >}}
{{< /playground >}}

### Example 4: FlatMap with a lambda function

For this example, we want to flatten a `PCollection` of lists of `str`s into a `PCollection` of `str`s.
Each input element is already an `iterable`, where each element is what we want in the resulting `PCollection`.
We use a lambda function that returns the same input element it received.

{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_FlatMapLambda" show="flatmap_lambda" >}}
{{< /playground >}}

### Example 5: FlatMap with a generator

For this example, we want to flatten a `PCollection` of lists of `str`s into a `PCollection` of `str`s.
We use a generator to iterate over the input list and yield each of the elements.
Each yielded result in the generator is an element in the resulting `PCollection`.

{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_FlatMapGenerator" show="flatmap_generator" >}}
{{< /playground >}}

### Example 6: FlatMapTuple for key-value pairs

If your `PCollection` consists of `(key, value)` pairs,
you can use `FlatMapTuple` to unpack them into different function arguments.

{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_FlatMapTuple" show="flatmap_tuple" >}}
{{< /playground >}}

### Example 7: FlatMap with multiple arguments

You can pass functions with multiple arguments to `FlatMap`.
They are passed as additional positional arguments or keyword arguments to the function.

In this example, `split_words` takes `text` and `delimiter` as arguments.

{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_FlatMapMultipleArguments" show="flatmap_multiple_arguments" >}}
{{< /playground >}}

### Example 8: FlatMap with side inputs as singletons

If the `PCollection` has a single value, such as the average from another computation,
passing the `PCollection` as a *singleton* accesses that value.

In this example, we pass a `PCollection` the value `','` as a singleton.
We then use that value as the delimiter for the `str.split` method.

{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_FlatMapSideInputSingleton" show="flatmap_side_inputs_singleton" >}}
{{< /playground >}}

### Example 9: FlatMap with side inputs as iterators

If the `PCollection` has multiple values, pass the `PCollection` as an *iterator*.
This accesses elements lazily as they are needed,
so it is possible to iterate over large `PCollection`s that won't fit into memory.

{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_FlatMapSideInputsIter" show="flatmap_side_inputs_iter" >}}
{{< /playground >}}

> **Note**: You can pass the `PCollection` as a *list* with `beam.pvalue.AsList(pcollection)`,
> but this requires that all the elements fit into memory.

### Example 10: FlatMap with side inputs as dictionaries

If a `PCollection` is small enough to fit into memory, then that `PCollection` can be passed as a *dictionary*.
Each element must be a `(key, value)` pair.
Note that all the elements of the `PCollection` must fit into memory for this.
If the `PCollection` won't fit into memory, use `beam.pvalue.AsIter(pcollection)` instead.

{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_FlatMapSideInputsDict" show="flatmap_side_inputs_dict" >}}
{{< /playground >}}

## Related transforms

* [Filter](/documentation/transforms/python/elementwise/filter) is useful if the function is just
  deciding whether to output an element or not.
* [ParDo](/documentation/transforms/python/elementwise/pardo) is the most general elementwise mapping
  operation, and includes other abilities such as multiple output collections and side-inputs.
* [Map](/documentation/transforms/python/elementwise/map) behaves the same, but produces exactly one output for each input.

{{< button-pydoc path="apache_beam.transforms.core" class="FlatMap" >}}
