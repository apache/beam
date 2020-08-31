---
title: "Reshuffle"
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

# Reshuffle
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html?highlight=reshuffle#apache_beam.transforms.util.Reshuffle">
      <img src="https://beam.apache.org/images/logos/sdks/python.png" width="20px" height="20px"
           alt="Pydoc" />
     Pydoc
    </a>
</table>
<br><br>


 Adds a temporary random key to each element in a collection, reshuffles
 these keys, and removes the temporary key. This redistributes the
 elements between workers and returns a collection equivalent to its
 input collection.  This is most useful for adjusting parallelism or
 preventing coupled failures.

## Examples
See [BEAM-7391](https://issues.apache.org/jira/browse/BEAM-7391) for updates.

## Related transforms
N/A
