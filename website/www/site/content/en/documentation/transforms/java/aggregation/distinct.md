---
title: "Distinct"
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
# Distinct
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/Distinct.html">
      <img src="/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

Produces a collection containing distinct elements of the input collection.

On some data sets, it might be more efficient to compute an approximate
answer using `ApproximateUnique`, which also allows for determining distinct
values for each key.

## Examples

**Example 1**: Find the distinct element from a `PCollection` of `String`.

{{< highlight java >}}

static final String[] WORDS_ARRAY = new String[]{
            "hi", "hi", "sue",
            "sue",  "bob"
    };
static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

PCollection<String> input =
        pipeline.apply(Create.of(WORDS)).withCoder(StringUtf8Coder.of());

PCollection<String> distinctWords = input.apply(Distinct.create());

{{< /highlight >}}

**Example 2**: Find the distinct element from a `PCollection` of `Integer`.

{{< playground height="700px" >}}
{{< playground_snippet language="java" path="SDK_JAVA_Distinct" show="main_section" >}}
{{< /playground >}}

## Related transforms
* [Count](/documentation/transforms/java/aggregation/count)
  counts the number of elements within each aggregation.
* [ApproximateUnique](/documentation/transforms/java/aggregation/approximateunique)
  estimates the number of distinct elements in a collection.
