---
layout: section
title: "CoGroupByKey"
permalink: /documentation/transforms/python/aggregation/cogroupbykey/
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

# CoGroupByKey
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html#apache_beam.transforms.util.CoGroupByKey">
      <img src="https://beam.apache.org/images/logos/sdks/python.png" width="20px" height="20px"
           alt="Pydoc" />
     Pydoc
    </a>
</table>
<br>
Aggregates all input elements by their key and allows downstream processing
to consume all values associated with the key. While `GroupByKey` performs
this operation over a single input collection and thus a single type of input
values, `CoGroupByKey` operates over multiple input collections. As a result,
the result for each key is a tuple of the values associated with that key in
each input collection.

See more information in the [Beam Programming Guide]({{ site.baseurl }}/documentation/programming-guide/#cogroupbykey).

## Examples
See [BEAM-7390](https://issues.apache.org/jira/browse/BEAM-7390) for updates. 

## Related transforms
* [CombineGlobally]({{ site.baseurl }}/documentation/transforms/python/aggregation/combineglobally) to combine elements.
* [GroupByKey]({{ site.baseurl }}/documentation/transforms/python/aggregation/groupbykey) takes one input collection.