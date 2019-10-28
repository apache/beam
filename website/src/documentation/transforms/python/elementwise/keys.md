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

{% include button-pydoc.md path="apache_beam.transforms.util" class="Keys" %}

Takes a collection of key-value pairs and returns the key of each element.

## Example

In the following example, we create a pipeline with a `PCollection` of key-value pairs.
Then, we apply `Keys` to extract the keys and discard the values.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/elementwise/keys.py tag:keys %}```

{:.notebook-skip}
Output `PCollection` after `Keys`:

{:.notebook-skip}
```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/elementwise/keys_test.py tag:icons %}```

{% include buttons-code-snippet.md
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/keys.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/keys"
%}

## Related transforms

* [KvSwap]({{ site.baseurl }}/documentation/transforms/python/elementwise/kvswap) swaps the key and value of each element.
* [Values]({{ site.baseurl }}/documentation/transforms/python/elementwise/values) for extracting the value of each element.

{% include button-pydoc.md path="apache_beam.transforms.util" class="Keys" %}
