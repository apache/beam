---
title: "Filter"
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
# Filter
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/Filter.html">
      <img src="https://beam.apache.org/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

Given a predicate, filter out all elements that don't satisfy that predicate.
May also be used to filter based on an inequality with a given value based
on the natural ordering of the element.

## Examples
**Example 1**: Filtering with a predicate

{{< highlight java >}}
PCollection<String> allStrings = Create.of("Hello", "world", "hi");
PCollection<String> longStrings = allStrings
    .apply(Filter.by(new SerializableFunction<String, Boolean>() {
      @Override
      public Boolean apply(String input) {
        return input.length() > 3;
      }
    }));
{{< /highlight >}}
The result is a `PCollection` containing "Hello" and "world".

**Example 2**: Filtering with an inequality

{{< highlight java >}}
PCollection<Long> numbers = Create.of(1L, 2L, 3L, 4L, 5L);
PCollection<Long> bigNumbers = numbers.apply(Filter.greaterThan(3));
PCollection<Long> smallNumbers = numbers.apply(Filter.lessThanEq(3));
{{< /highlight >}}
Other variants include `Filter.greaterThanEq`, `Filter.lessThan` and `Filter.equal`.

## Related transforms 
* [FlatMapElements](/documentation/transforms/java/elementwise/flatmapelements) behaves the same as `Map`, but for
  each input it might produce zero or more outputs.
* [ParDo](/documentation/transforms/java/elementwise/pardo) is the most general element-wise mapping
  operation, and includes other abilities such as multiple output collections and side-inputs. 