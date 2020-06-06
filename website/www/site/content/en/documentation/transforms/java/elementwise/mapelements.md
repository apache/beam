---
title: "MapElements"
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
# MapElements
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/MapElements.html">
      <img src="https://beam.apache.org/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

Applies a simple 1-to-1 mapping function over each element in the collection.

## Examples
**Example 1**: providing the mapping function using a `SimpleFunction`

{{< highlight java >}}
PCollection<String> lines = Create.of("Hello World", "Beam is fun");
PCollection<Integer> lineLengths = lines.apply(MapElements.via(
    new SimpleFunction<String, Integer>() {
      @Override
      public Integer apply(String line) {
        return line.length();
      }
    });
{{< /highlight >}}

**Example 2**: providing the mapping function using a `SerializableFunction`,
which allows the use of Java 8 lambdas. Due to type erasure, you need
to provide a hint indicating the desired return type. 

{{< highlight java >}}
PCollection<String> lines = Create.of("Hello World", "Beam is fun");
PCollection<Integer> lineLengths = lines.apply(MapElements
    .into(TypeDescriptors.integers())
    .via((String line) -> line.length()));
{{< /highlight >}}

## Related transforms 
* [FlatMapElements](/documentation/transforms/java/elementwise/flatmapelements) behaves the same as `Map`, but for
  each input it may produce zero or more outputs.
* [Filter](/documentation/transforms/java/elementwise/filter) is useful if the function is just 
  deciding whether to output an element or not.
* [ParDo](/documentation/transforms/java/elementwise/pardo) is the most general element-wise mapping
  operation, and includes other abilities such as multiple output collections and side-inputs.