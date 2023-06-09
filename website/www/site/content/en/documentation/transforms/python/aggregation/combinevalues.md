---
title: "CombineValues"
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

# CombineValues

{{< localstorage language language-py >}}

{{< button-pydoc path="apache_beam.transforms.core" class="CombineValues" >}}

Combines an iterable of values in a keyed collection of elements.

See more information in the [Beam Programming Guide](/documentation/programming-guide/#combine).

## Examples

In the following examples, we create a pipeline with a `PCollection` of produce.
Then, we apply `CombineValues` in multiple ways to combine the keyed values in the `PCollection`.

`CombineValues` accepts a function that takes an `iterable` of elements as an input, and combines them to return a single element.
`CombineValues` expects a keyed `PCollection` of elements, where the value is an iterable of elements to be combined.

### Example 1: Combining with a predefined function

We use the function
[`sum`](https://docs.python.org/3/library/functions.html#sum)
which takes an `iterable` of numbers and adds them together.

{{< highlight language="py" file="sdks/python/apache_beam/examples/snippets/transforms/aggregation/combinevalues_simple.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combinevalues_simple.py" combinevalues_simple >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combinevalues_test.py" total >}}
{{< /highlight >}}

### Example 2: Combining with a function

We want the sum to be bounded up to a maximum value, so we use
[saturated arithmetic](https://en.wikipedia.org/wiki/Saturation_arithmetic).

We define a function `saturated_sum` which takes an `iterable` of numbers and adds them together, up to a predefined maximum number.

{{< highlight language="py" file="sdks/python/apache_beam/examples/snippets/transforms/aggregation/combinevalues_function.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combinevalues_function.py" combinevalues_function >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combinevalues_test.py" saturated_total >}}
{{< /highlight >}}

### Example 3: Combining with a lambda function

We can also use lambda functions to simplify **Example 2**.

{{< highlight language="py" file="sdks/python/apache_beam/examples/snippets/transforms/aggregation/combinevalues_lambda.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combinevalues_lambda.py" combinevalues_lambda >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combinevalues_test.py" saturated_total >}}
{{< /highlight >}}

### Example 4: Combining with multiple arguments

You can pass functions with multiple arguments to `CombineValues`.
They are passed as additional positional arguments or keyword arguments to the function.

In this example, the lambda function takes `values` and `max_value` as arguments.

{{< highlight language="py" file="sdks/python/apache_beam/examples/snippets/transforms/aggregation/combinevalues_multiple_arguments.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combinevalues_multiple_arguments.py" combinevalues_multiple_arguments >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combinevalues_test.py" saturated_total >}}
{{< /highlight >}}

### Example 5: Combining with a `CombineFn`

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

{{< highlight language="py" file="sdks/python/apache_beam/examples/snippets/transforms/aggregation/combinevalues_combinefn.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combinevalues_combinefn.py" combinevalues_combinefn >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/combinevalues_test.py" percentages_per_season >}}
{{< /highlight >}}

## Related transforms

You can use the following combiner transforms:

* [CombineGlobally](/documentation/transforms/python/aggregation/combineglobally)
* [CombinePerKey](/documentation/transforms/python/aggregation/combineperkey)
* [Mean](/documentation/transforms/python/aggregation/mean)
* [Count](/documentation/transforms/python/aggregation/count)
* [Top](/documentation/transforms/python/aggregation/top)
* [Sample](/documentation/transforms/python/aggregation/sample)

{{< button-pydoc path="apache_beam.transforms.core" class="CombineValues" >}}
