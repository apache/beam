---
title: "GroupBy"
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

# GroupBy

{{< localstorage language language-py >}}

{{< button-pydoc path="apache_beam.transforms.core" class="GroupBy" >}}

Takes a collection of elements and produces a collection grouped,
by properties of those elements.

Unlike `GroupByKey`, the key is dynamically created from the elements themselves.

## Grouping Examples

In the following example, we create a pipeline with a `PCollection` of fruits.

We use `GroupBy` to group all fruits by the first letter of their name.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupby_test.py" groupby_expr >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupby_test.py" groupby_expr_result >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupby_test.py" >}}


We can group by a composite key consisting of multiple properties if desired.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupby_test.py" groupby_two_exprs >}}
{{< /highlight >}}

The resulting key is a named tuple with the two requested attributes, and the
values are grouped accordingly.

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupby_test.py" groupby_two_exprs_result >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupby_test.py" >}}


In the case that the property one wishes to group by is an attribute, a string
may be passed to `GroupBy` in the place of a callable expression. For example,
suppose I have the following data

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupby_test.py" groupby_table >}}
{{< /highlight >}}

We can then do

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupby_test.py" groupby_attr >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupby_test.py" groupby_attr_result >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupby_test.py" >}}

It is possible to mix and match attributes and expressions, for example

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupby_test.py" groupby_attr_expr >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupby_test.py" groupby_attr_expr_result >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupby_test.py" >}}.

## Aggregation

Grouping is often used in conjunction with aggregation, and the
`aggregate_field` method of the `GroupBy` transform can be used to accomplish
this easily.
This method takes three parameters: the field (or expression) which to
aggregate, the `CombineFn` (or associative `callable`) with which to aggregate
by, and finally a field name in which to store the result.
For example, suppose one wanted to compute the amount of each fruit to buy.
One could write

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupby_test.py" simple_aggregate >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupby_test.py" simple_aggregate_result >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupby_test.py" >}}.

Similar to the parameters in `GroupBy`, one can also aggregate multiple fields
and by expressions.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupby_test.py" expr_aggregate >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupby_test.py" expr_aggregate_result >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupby_test.py" >}}.

One can, of course, aggregate the same field multiple times as well.
This example also illustrates a global grouping, as the grouping key is empty.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupby_test.py" global_aggregate >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupby_test.py" global_aggregate_result >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupby_test.py" >}}.


## Related transforms

* [CombinePerKey](/documentation/transforms/python/aggregation/combineperkey) for combining with a single CombineFn.
* [GroupByKey](/documentation/transforms/python/aggregation/groupbykey) for grouping with a known key.
* [CoGroupByKey](/documentation/transforms/python/aggregation/cogroupbykey) for multiple input collections.

{{< button-pydoc path="apache_beam.transforms.core" class="GroupByKey" >}}
