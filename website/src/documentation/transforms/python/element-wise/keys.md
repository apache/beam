---
layout: section
title: "Keys"
permalink: /documentation/transforms/python/elementwise/keys/
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

# Keys

<script type="text/javascript">
localStorage.setItem('language', 'language-py')
</script>

<table>
  <td>
    <a class="button" target="_blank"
        href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html#apache_beam.transforms.util.Keys">
      <img src="https://beam.apache.org/images/logos/sdks/python.png"
          width="20px" height="20px" alt="Pydoc" />
      Pydoc
    </a>
  </td>
</table>
<br>

Takes a collection of key-value pairs and returns the key of each element.

## Example

In the following example, we create a pipeline with a `PCollection` of key-value pairs.
Then, we apply `Keys` to extract the keys and discard the values.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/keys.py tag:keys %}```

Output `PCollection` after `Keys`:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/keys_test.py tag:icons %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/keys.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

## Related transforms

* [KvSwap]({{ site.baseurl }}/documentation/transforms/python/elementwise/kvswap) swaps the key and value of each element.
* [Values]({{ site.baseurl }}/documentation/transforms/python/elementwise/values) for extracting the value of each element.

<table>
  <td>
    <a class="button" target="_blank"
        href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html#apache_beam.transforms.util.Keys">
      <img src="https://beam.apache.org/images/logos/sdks/python.png"
          width="20px" height="20px" alt="Pydoc" />
      Pydoc
    </a>
  </td>
</table>
<br>
