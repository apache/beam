---
layout: section
title: "Filter"
permalink: /documentation/transforms/python/elementwise/filter/
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

# Filter

<script type="text/javascript">
localStorage.setItem('language', 'language-py')
</script>

<table>
  <td>
    <a class="button" target="_blank"
        href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.Filter">
      <img src="https://beam.apache.org/images/logos/sdks/python.png"
          width="20px" height="20px" alt="Pydoc" />
      Pydoc
    </a>
  </td>
</table>
<br>

Given a predicate, filter out all elements that don't satisfy that predicate.
May also be used to filter based on an inequality with a given value based
on the comparison ordering of the element.

## Examples

### Function

`Filter` accepts a function that receives the element as the first argument,
and returns `True` if the element will be kept, or `False` if the element will be filtered out.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/filter.py tag:filter_function %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/filter.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

### Lambda

Lambda functions can also be used.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/filter.py tag:filter_lambda %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/filter.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

### Multiple Arguments

Multiple function arguments can be passed as additional arguments to `Filter`.
They will be passed as additional positional arguments or keyword arguments to the function.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/filter.py tag:filter_multiple_arguments %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/filter.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

### Side Inputs - Singleton

If there is a single value in the `PCollection`, such as the average from another computation,
passing the `PCollection` as a *singleton* will access that value.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/filter.py tag:filter_side_inputs_singleton %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/filter.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

### Side Inputs - Iterator

If there are multiple values in the `PCollection`, it is recommended way to pass the `PCollection` as an *iterator*.
This will access elements lazily as they are needed,
so it is possible to iterate over very large `PCollection`s that don't fit into memory.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/filter.py tag:filter_side_inputs_iter %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/filter.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

> **Note**: It is also possible to pass the `PCollection` as a *list* with `beam.pvalue.AsList(pcollection)`,
> but this will require all the elements to fit into memory.

### Side Inputs - Dict

If the `PCollection` consists of a small enough number of key-value pairs that can fit into memory,
the `PCollection` can be passed as a *dictionary*.
Note that all the elements of the `PCollection` must fit into memory for this.
If the `PCollection` can be very large, use `beam.pvalue.AsIter(pcollection)` instead.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/filter.py tag:filter_side_inputs_dict %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/filter.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

## Related transforms

* [FlatMap]({{ site.baseurl }}/documentation/transforms/python/elementwise/flatmap) behaves the same as `Map`, but for
  each input it might produce zero or more outputs.
* [ParDo]({{ site.baseurl }}/documentation/transforms/python/elementwise/pardo) is the most general element-wise mapping
  operation, and includes other abilities such as multiple output collections and side-inputs.

<table align="left">
  <a target="_blank" class="button"
      href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.Filter">
    <img src="https://beam.apache.org/images/logos/sdks/python.png"
        width="20px" height="20px" alt="Pydoc" />
    Pydoc
  </a>
</table>
<br>
