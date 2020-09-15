---
title: "ToString"
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

# ToString

{{< localstorage language language-py >}}

{{< button-pydoc path="apache_beam.transforms.util" class="ToString" >}}

Transforms every element in an input collection to a string.

## Examples

Any non-string element can be converted to a string using standard Python functions and methods.
Many I/O transforms, such as
[`textio.WriteToText`](https://beam.apache.org/releases/pydoc/current/apache_beam.io.textio.html#apache_beam.io.textio.WriteToText),
expect their input elements to be strings.

### Example 1: Key-value pairs to string

The following example converts a `(key, value)` pair into a string delimited by `','`.
You can specify a different delimiter using the `delimiter` argument.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/tostring.py" tostring_kvs >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/tostring_test.py" plants >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/tostring.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/tostring" >}}

### Example 2: Elements to string

The following example converts a dictionary into a string.
The string output will be equivalent to `str(element)`.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/tostring.py" tostring_element >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/tostring_test.py" plant_lists >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/tostring.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/tostring" >}}

### Example 3: Iterables to string

The following example converts an iterable, in this case a list of strings,
into a string delimited by `','`.
You can specify a different delimiter using the `delimiter` argument.
The string output will be equivalent to `iterable.join(delimiter)`.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/tostring.py" tostring_iterables >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/tostring_test.py" plants_csv >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/tostring.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/tostring" >}}

## Related transforms

* [Map](/documentation/transforms/python/elementwise/map) applies a simple 1-to-1 mapping function over each element in the collection

{{< button-pydoc path="apache_beam.transforms.util" class="ToString" >}}
