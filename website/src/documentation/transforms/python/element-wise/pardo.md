---
layout: section
title: "ParDo"
permalink: /documentation/transforms/python/elementwise/pardo/
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

# ParDo
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.ParDo">
      <img src="https://beam.apache.org/images/logos/sdks/python.png" width="20px" height="20px"
           alt="Pydoc" />
     Pydoc
    </a>
</table>
<br>
A transform for generic parallel processing. A `ParDo` transform considers each
element in the input `PCollection`, performs some processing function
(your user code) on that element, and emits zero or more elements to
an output PCollection.

See more information in the [Beam Programming Guide]({{ site.baseurl }}/documentation/programming-guide/#pardo).

## Examples
See [BEAM-7389](https://issues.apache.org/jira/browse/BEAM-7389) for updates. 

## Related transforms 
* [FlatMap]({{ site.baseurl }}/documentation/transforms/python/elementwise/flatmap) behaves the same as `Map`, but for each input it may produce zero or more outputs.
* [Filter]({{ site.baseurl }}/documentation/transforms/python/elementwise/filter) is useful if the function is just 
  deciding whether to output an element or not.