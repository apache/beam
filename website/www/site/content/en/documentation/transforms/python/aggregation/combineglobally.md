---
title: "CombineGlobally"
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

# CombineGlobally

{{< localstorage language language-py >}}

{{< button-pydoc path="apache_beam.transforms.core" class="CombineGlobally" >}}

Combines all elements in a collection.

See more information in the [Beam Programming Guide](/documentation/programming-guide/#combine).

## Examples

In the following examples, we create a pipeline with a `PCollection` of produce.
Then, we apply `CombineGlobally` in multiple ways to combine all the elements in the `PCollection`.

`CombineGlobally` accepts a function that takes an `iterable` of elements as an input, and combines them to return a single element.

### Example 1: Combining with a function

We define a function `get_common_items` which takes an `iterable` of sets as an input, and calculates the intersection (common items) of those sets.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combineglobally.py" combineglobally_function >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output `PCollection` after `CombineGlobally`:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combineglobally_test.py" common_items >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/combineglobally.py" >}}

### Example 2: Combining with a lambda function

We can also use lambda functions to simplify **Example 1**.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combineglobally.py" combineglobally_lambda >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output `PCollection` after `CombineGlobally`:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combineglobally_test.py" common_items >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/combineglobally.py" >}}

### Example 3: Combining with multiple arguments

You can pass functions with multiple arguments to `CombineGlobally`.
They are passed as additional positional arguments or keyword arguments to the function.

In this example, the lambda function takes `sets` and `exclude` as arguments.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combineglobally.py" combineglobally_multiple_arguments >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output `PCollection` after `CombineGlobally`:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combineglobally_test.py" common_items_with_exceptions >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/combineglobally.py" >}}

### Example 4: Combining with side inputs as singletons

If the `PCollection` has a single value, such as the average from another computation,
passing the `PCollection` as a *singleton* accesses that value.

In this example, we pass a `PCollection` the value `'🥕'` as a singleton.
We then use that value to exclude specific items.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combineglobally.py" combineglobally_side_inputs_singleton >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output `PCollection` after `CombineGlobally`:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combineglobally_test.py" common_items_with_exceptions >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/combineglobally.py" >}}

### Example 5: Combining with side inputs as iterators

If the `PCollection` has multiple values, pass the `PCollection` as an *iterator*.
This accesses elements lazily as they are needed,
so it is possible to iterate over large `PCollection`s that won't fit into memory.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combineglobally.py" combineglobally_side_inputs_iter >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output `PCollection` after `CombineGlobally`:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combineglobally_test.py" common_items_with_exceptions >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/combineglobally.py" >}}

> **Note**: You can pass the `PCollection` as a *list* with `beam.pvalue.AsList(pcollection)`,
> but this requires that all the elements fit into memory.

### Example 6: Combining with side inputs as dictionaries

If a `PCollection` is small enough to fit into memory, then that `PCollection` can be passed as a *dictionary*.
Each element must be a `(key, value)` pair.
Note that all the elements of the `PCollection` must fit into memory for this.
If the `PCollection` won't fit into memory, use `beam.pvalue.AsIter(pcollection)` instead.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combineglobally.py" combineglobally_side_inputs_dict >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output `PCollection` after `CombineGlobally`:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combineglobally_test.py" custom_common_items >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/combineglobally.py" >}}

### Example 7: Combining with a `CombineFn`

The more general way to combine elements, and the most flexible, is with a class that inherits from `CombineFn`.

* [`CombineFn.create_accumulator()`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.CombineFn.create_accumulator):
  This creates an empty accumulator.
  For example, an empty accumulator for a sum would be `0`, while an empty accumulator for a product (multiplication) would be `1`.

* [`CombineFn.add_input()`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.CombineFn.add_input):
  Called *once per element*.
  Takes an accumulator and an input element, combines them and returns the updated accumulator.

* [`CombineFn.merge_accumulators()`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.CombineFn.merge_accumulators):
  Multiple accumulators could be processed in parallel, so this function helps merging them into a single accumulator.

* [`CombineFn.extract_output()`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.CombineFn.extract_output):
  It allows to do additional calculations before extracting a result.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combineglobally.py" combineglobally_combinefn >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output `PCollection` after `CombineGlobally`:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combineglobally_test.py" percentages >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/combineglobally.py" >}}

## Related transforms

You can use the following combiner transforms:

* [Mean](/documentation/transforms/python/aggregation/mean)
* [Count](/documentation/transforms/python/aggregation/count)
* [Top](/documentation/transforms/python/aggregation/top)
* [Sample](/documentation/transforms/python/aggregation/sample)

{{< button-pydoc path="apache_beam.transforms.core" class="CombineGlobally" >}}
