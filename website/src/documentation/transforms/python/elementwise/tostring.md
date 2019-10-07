---
layout: section
title: "ToString"
permalink: /documentation/transforms/python/elementwise/tostring/
section_menu: section-menu/documentation.html
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

<script type="text/javascript">
localStorage.setItem('language', 'language-py')
</script>

{% include button-pydoc.md path="apache_beam.transforms.util" class="ToString" %}

Transforms every element in an input collection to a string.

## Examples

Any non-string element can be converted to a string using standard Python functions and methods.
Many I/O transforms, such as
[`textio.WriteToText`](https://beam.apache.org/releases/pydoc/current/apache_beam.io.textio.html#apache_beam.io.textio.WriteToText),
expect their input elements to be strings.

### Example 1: Key-value pairs to string

The following example converts a `(key, value)` pair into a string delimited by `','`.
You can specify a different delimiter using the `delimiter` argument.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/elementwise/tostring.py tag:tostring_kvs %}```

{:.notebook-skip}
Output `PCollection` after `ToString`:

{:.notebook-skip}
```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/elementwise/tostring_test.py tag:plants %}```

{% include buttons-code-snippet.md
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/tostring.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/tostring"
%}

### Example 2: Elements to string

The following example converts a dictionary into a string.
The string output will be equivalent to `str(element)`.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/elementwise/tostring.py tag:tostring_element %}```

{:.notebook-skip}
Output `PCollection` after `ToString`:

{:.notebook-skip}
```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/elementwise/tostring_test.py tag:plant_lists %}```

{% include buttons-code-snippet.md
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/tostring.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/tostring"
%}

### Example 3: Iterables to string

The following example converts an iterable, in this case a list of strings,
into a string delimited by `','`.
You can specify a different delimiter using the `delimiter` argument.
The string output will be equivalent to `iterable.join(delimiter)`.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/elementwise/tostring.py tag:tostring_iterables %}```

{:.notebook-skip}
Output `PCollection` after `ToString`:

{:.notebook-skip}
```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/elementwise/tostring_test.py tag:plants_csv %}```

{% include buttons-code-snippet.md
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/tostring.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/tostring"
%}

## Related transforms

* [Map]({{ site.baseurl }}/documentation/transforms/python/elementwise/map) applies a simple 1-to-1 mapping function over each element in the collection

{% include button-pydoc.md path="apache_beam.transforms.util" class="ToString" %}
