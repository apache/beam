---
title: "Map"
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

# Map

{{< localstorage language language-py >}}

{{< button-pydoc path="apache_beam.transforms.core" class="Map" >}}

Applies a simple 1-to-1 mapping function over each element in the collection.

## Examples

In the following examples, we create a pipeline with a `PCollection` of produce with their icon, name, and duration.
Then, we apply `Map` in multiple ways to transform every element in the `PCollection`.

`Map` accepts a function that returns a single element for every input element in the `PCollection`.

### Example 1: Map with a predefined function

We use the function `str.strip` which takes a single `str` element and outputs a `str`.
It strips the input element's whitespaces, including newlines and tabs.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/map.py" map_simple >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/map_test.py" plants >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/map.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/map" >}}

### Example 2: Map with a function

We define a function `strip_header_and_newline` which strips any `'#'`, `' '`, and `'\n'` characters from each element.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/map.py" map_function >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/map_test.py" plants >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/map.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/map" >}}

### Example 3: Map with a lambda function

We can also use lambda functions to simplify **Example 2**.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/map.py" map_lambda >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/map_test.py" plants >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/map.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/map" >}}

### Example 4: Map with multiple arguments

You can pass functions with multiple arguments to `Map`.
They are passed as additional positional arguments or keyword arguments to the function.

In this example, `strip` takes `text` and `chars` as arguments.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/map.py" map_multiple_arguments >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/map_test.py" plants >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/map.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/map" >}}

### Example 5: MapTuple for key-value pairs

If your `PCollection` consists of `(key, value)` pairs,
you can use `MapTuple` to unpack them into different function arguments.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/map.py" map_tuple >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/map_test.py" plants >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/map.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/map" >}}

### Example 6: Map with side inputs as singletons

If the `PCollection` has a single value, such as the average from another computation,
passing the `PCollection` as a *singleton* accesses that value.

In this example, we pass a `PCollection` the value `'# \n'` as a singleton.
We then use that value as the characters for the `str.strip` method.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/map.py" map_side_inputs_singleton >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/map_test.py" plants >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/map.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/map" >}}

### Example 7: Map with side inputs as iterators

If the `PCollection` has multiple values, pass the `PCollection` as an *iterator*.
This accesses elements lazily as they are needed,
so it is possible to iterate over large `PCollection`s that won't fit into memory.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/map.py" map_side_inputs_iter >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/map_test.py" plants >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/map.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/map" >}}

> **Note**: You can pass the `PCollection` as a *list* with `beam.pvalue.AsList(pcollection)`,
> but this requires that all the elements fit into memory.

### Example 8: Map with side inputs as dictionaries

If a `PCollection` is small enough to fit into memory, then that `PCollection` can be passed as a *dictionary*.
Each element must be a `(key, value)` pair.
Note that all the elements of the `PCollection` must fit into memory for this.
If the `PCollection` won't fit into memory, use `beam.pvalue.AsIter(pcollection)` instead.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/map.py" map_side_inputs_dict >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/map_test.py" plant_details >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/map.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/map" >}}

## Related transforms

* [FlatMap](/documentation/transforms/python/elementwise/flatmap) behaves the same as `Map`, but for
  each input it may produce zero or more outputs.
* [Filter](/documentation/transforms/python/elementwise/filter) is useful if the function is just
  deciding whether to output an element or not.
* [ParDo](/documentation/transforms/python/elementwise/pardo) is the most general elementwise mapping
  operation, and includes other abilities such as multiple output collections and side-inputs.

{{< button-pydoc path="apache_beam.transforms.core" class="Map" >}}
