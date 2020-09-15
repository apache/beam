---
title: "WithKeys"
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
# WithKeys
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/WithKeys.html">
      <img src="https://beam.apache.org/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

Takes a `PCollection<V>` and produces a `PCollection<KV<K, V>>` by associating
each input element with a key.

There are two versions of `WithKeys`, depending on how the key should be determined:

* `WithKeys.of(SerializableFunction<V, K> fn)` takes a function to
  compute the key from each value.
* `WithKeys.of(K key)` associates each value with the specified key.

## Examples
**Example**
{{< highlight java >}}
PCollection<String> words = Create.of("Hello", "World", "Beam", "is", "fun");
PCollection<KV<Integer, String>> lengthAndWord =
  words.apply(WithKeys.of(new SerialiazableFunction<String, Integer>() {
    @Override
    public Integer apply(String s) {
      return s.length();
    }
  });
{{< /highlight >}}

## Related transforms
* [Keys](/documentation/transforms/java/elementwise/keys) for extracting the key of each component.
* [Values](/documentation/transforms/java/elementwise/values) for extracting the value of each element.
* [KvSwap](/documentation/transforms/java/elementwise/kvswap) swaps key-value pair values.
