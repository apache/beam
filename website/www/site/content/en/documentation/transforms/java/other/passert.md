---
title: "PAssert"
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
# PAssert
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/PAssert.html">
      <img src="https://beam.apache.org/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

`PAssert` is a class included in the Beam Java SDK that is an
assertion on the contents of a `PCollection`. You can use `PAssert` to verify
that a `PCollection` contains a specific set of expected elements.

## Examples
For a given `PCollection`, you can use `PAssert` to verify the contents as follows:
{{< highlight java >}}
PCollection<String> output = ...;

// Check whether a PCollection contains some elements in any order.
PAssert.that(output)
.containsInAnyOrder(
  "elem1",
  "elem3",
  "elem2");
{{< /highlight >}}

Any code that uses `PAssert` must link in `JUnit` and `Hamcrest`.
If you're using Maven, you can link in `Hamcrest` by adding the
following dependency to your project's pom.xml file:

{{< highlight java >}}
<dependency>
    <groupId>org.hamcrest</groupId>
    <artifactId>hamcrest-all</artifactId>
    <version>1.3</version>
    <scope>test</scope>
</dependency>
{{< /highlight >}}

## Related transforms
* TestStream
