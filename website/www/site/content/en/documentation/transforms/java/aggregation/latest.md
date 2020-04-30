---
title: "Latest"
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
# Latest
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/Latest.html">
      <img src="https://beam.apache.org/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

A transform and `Combine.CombineFn` for computing the latest element in a collection.

* `Latest.globally()` takes a collection of values and produces the collection
  containing the single value with the latest implicit timestamp.
* `Latest.perKey()` takes a collection of key value pairs, and returns the
  latest value for each key, according to the implicit timestamp.

For elements with the same timestamp, the output element is arbitrarily selected.

## Examples
**Example**: compute the latest value for each session
{{< highlight java >}}
 PCollection input = ...;
 PCollection sessioned = input
    .apply(Window.into(Sessions.withGapDuration(Duration.standardMinutes(5)));
 PCollection latestValues = sessioned.apply(Latest.globally());
{{< /highlight >}}

## Related transforms 
* [Reify](/documentation/transforms/java/elementwise/reify)
  converts between explicit and implicit form of various Beam values
* [WithTimestamps](/documentation/transforms/java/elementwise/withtimestamps)
  assigns timestamps to all the elements of a collection
