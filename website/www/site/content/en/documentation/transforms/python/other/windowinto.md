---
title: "WindowInto"
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

# WindowInto
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.window.html?highlight=window#module-apache_beam.transforms.window">
      <img src="https://beam.apache.org/images/logos/sdks/python.png" width="20px" height="20px" alt="Pydoc">
     Pydoc
    </a>
</table>
<br><br>


Logically divides up or groups the elements of a collection into finite
windows according to a function.

## Examples
See [BEAM-7391](https://issues.apache.org/jira/browse/BEAM-7391) for updates. 

## Related transforms
* [GroupByKey](/documentation/transforms/python/aggregation/groupbykey)
  produces a collection where each element consists of a key and all values associated
  with that key.
* [Timestamp](/documentation/transforms/python/elementwise/withtimestamps)
  applies a function to determine a timestamp to each element in the output collection.