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

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap.py" flatmap_simple >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap_test.py" plants >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/flatmap" >}}

### Example 2: FlatMap with a function

We define a function `split_words` which splits an input `str` element using the delimiter `','` and outputs a `list` of `str`s.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap.py" flatmap_function >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap_test.py" plants >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/flatmap" >}}

### Example 3: FlatMap with a lambda function

For this example, we want to flatten a `PCollection` of lists of `str`s into a `PCollection` of `str`s.
Each input element is already an `iterable`, where each element is what we want in the resulting `PCollection`.
We use a lambda function that returns the same input element it received.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap.py" flatmap_lambda >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap_test.py" plants >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/flatmap" >}}

### Example 4: FlatMap with a generator

For this example, we want to flatten a `PCollection` of lists of `str`s into a `PCollection` of `str`s.
We use a generator to iterate over the input list and yield each of the elements.
Each yielded result in the generator is an element in the resulting `PCollection`.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap.py" flatmap_generator >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap_test.py" plants >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/flatmap" >}}

### Example 5: FlatMapTuple for key-value pairs

If your `PCollection` consists of `(key, value)` pairs,
you can use `FlatMapTuple` to unpack them into different function arguments.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap.py" flatmap_tuple >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap_test.py" plants >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/flatmap" >}}

### Example 6: FlatMap with multiple arguments

You can pass functions with multiple arguments to `FlatMap`.
They are passed as additional positional arguments or keyword arguments to the function.

In this example, `split_words` takes `text` and `delimiter` as arguments.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap.py" flatmap_multiple_arguments >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap_test.py" plants >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/flatmap" >}}

### Example 7: FlatMap with side inputs as singletons

If the `PCollection` has a single value, such as the average from another computation,
passing the `PCollection` as a *singleton* accesses that value.

In this example, we pass a `PCollection` the value `','` as a singleton.
We then use that value as the delimiter for the `str.split` method.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap.py" flatmap_side_inputs_singleton >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap_test.py" plants >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/flatmap" >}}

### Example 8: FlatMap with side inputs as iterators

If the `PCollection` has multiple values, pass the `PCollection` as an *iterator*.
This accesses elements lazily as they are needed,
so it is possible to iterate over large `PCollection`s that won't fit into memory.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap.py" flatmap_side_inputs_iter >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap_test.py" valid_plants >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/flatmap" >}}

> **Note**: You can pass the `PCollection` as a *list* with `beam.pvalue.AsList(pcollection)`,
> but this requires that all the elements fit into memory.

### Example 9: FlatMap with side inputs as dictionaries

If a `PCollection` is small enough to fit into memory, then that `PCollection` can be passed as a *dictionary*.
Each element must be a `(key, value)` pair.
Note that all the elements of the `PCollection` must fit into memory for this.
If the `PCollection` won't fit into memory, use `beam.pvalue.AsIter(pcollection)` instead.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap.py" flatmap_side_inputs_dict >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap_test.py" valid_plants >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/flatmap.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/flatmap" >}}

## Related transforms

* [Filter](/documentation/transforms/python/elementwise/filter) is useful if the function is just
  deciding whether to output an element or not.
* [ParDo](/documentation/transforms/python/elementwise/pardo) is the most general elementwise mapping
  operation, and includes other abilities such as multiple output collections and side-inputs.
* [Map](/documentation/transforms/python/elementwise/map) behaves the same, but produces exactly one output for each input.

{{< button-pydoc path="apache_beam.transforms.core" class="FlatMap" >}}
