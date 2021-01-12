---
title: "Flatten"
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
# Flatten
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/Flatten.html">
      <img src="https://beam.apache.org/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>


Merges multiple `PCollection` objects into a single logical `PCollection`.

By default, the coder for the output `PCollection` is the same as the coder
for the first `PCollection` in the input `PCollectionList`. However, the
input `PCollection` objects can each use different coders, as long as
they all contain the same data type in your chosen language.

When using `Flatten` to merge `PCollection` objects that have a windowing
strategy applied, all of the `PCollection` objects you want to merge must
use a compatible windowing strategy and window sizing. For example, all
the collections you're merging must all use (hypothetically) identical
5-minute fixed windows or 4-minute sliding windows starting every 30 seconds.

If your pipeline attempts to use `Flatten` to merge `PCollection` objects
with incompatible windows, Beam generates an `IllegalStateException` error
when your pipeline is constructed

See more information in the [Beam Programming Guide](/documentation/programming-guide/#flatten).

## Examples
**Example**: Apply a `Flatten` transform to merge multiple `PCollection` objects

{{< highlight java >}}
// Flatten takes a PCollectionList of PCollection objects of a given type.
// Returns a single PCollection that contains all of the elements in the PCollection objects in that list.
PCollection<String> pc1 = Create.of("Hello");
PCollection<String> pc2 = Create.of("World", "Beam");
PCollection<String> pc3 = Create.of("Is", "Fun");
PCollectionList<String> collections = PCollectionList.of(pc1).and(pc2).and(pc3);

PCollection<String> merged = collections.apply(Flatten.<String>pCollections());
{{< /highlight >}}
The resulting collection now has all the elements: "Hello", "World",
"Beam", "Is", and "Fun".

## Related transforms
* [ParDo](/documentation/transforms/java/elementwise/pardo)
* [Partition](/documentation/transforms/java/elementwise/partition)
