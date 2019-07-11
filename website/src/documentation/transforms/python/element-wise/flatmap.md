---
layout: section
title: "FlatMap"
permalink: /documentation/transforms/python/elementwise/flatmap/
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

# FlatMap
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.FlatMap">
      <img src="https://beam.apache.org/images/logos/sdks/python.png" width="20px" height="20px"
           alt="Pydoc" />
     Pydoc
    </a>
</table>
<br>
Applies a simple 1-to-many mapping function over each element in the collection.
The many elements are flattened into the resulting collection.

## Examples
See [BEAM-7389](https://issues.apache.org/jira/browse/BEAM-7389) for updates. 

## Related transforms 
* [Filter]({{ site.baseurl }}/documentation/transforms/python/elementwise/filter) is useful if the function is just 
  deciding whether to output an element or not.
* [ParDo]({{ site.baseurl }}/documentation/transforms/python/elementwise/pardo) is the most general element-wise mapping
  operation, and includes other abilities such as multiple output collections and side-inputs. 
* [Map]({{ site.baseurl }}/documentation/transforms/python/elementwise/map) behaves the same, but produces exactly one output for each input.