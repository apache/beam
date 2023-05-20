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

WithKeys takes a `PCollection<V>` and produces a `PCollection<KV<K, V>>` by associating each input element with a key.
{{if (eq .Sdk "java")}}
There are two versions of WithKeys, depending on how the key should be determined:

`WithKeys.of(SerializableFunction<V, K> fn)` takes a function to compute the key from each value.

```
PCollection<String> input = pipeline.apply(Create.of("Hello", "World", "Apache", "Beam"));
PCollection<KV<Integer, String>> lengthAndWord = input.apply(WithKeys.of(new SerializableFunction<String, Integer>() {
    @Override
    public Integer apply(String word) {
        return word.length();
    }
}));
```

Output

```
KV{6, Apache}
KV{5, World}
KV{4, Beam}
KV{5, Hello}
```


`WithKeys.of(K key)` associates each value with the specified key.

```
PCollection<String> input = pipeline.apply(Create.of("Hello", "World", "Apache", "Beam"));
PCollection<KV<String, String>> specifiedKeyAndWord = input.apply(WithKeys.of("SpecifiedKey"));
```

Output

```
KV{SpecifiedKey, Apache}
KV{SpecifiedKey, Hello}
KV{SpecifiedKey, World}
KV{SpecifiedKey, Beam}
```
{{end}}
{{if (eq .Sdk "python")}}
```
import apache_beam as beam

from log_elements import LogElements

with beam.Pipeline() as p:
  (p | beam.Create(['apple', 'banana', 'cherry', 'durian', 'guava', 'melon'])
     | beam.WithKeys(lambda word: word[0:1])
     | LogElements())
```

Output
```
('a', 'apple')
('b', 'banana')
('c', 'cherry')
('d', 'durian')
('g', 'guava')
('m', 'melon')
```
{{end}}
### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

The data is returned, which consists of a card with a key, which is the first letter of the word and the meaning of which is the word itself.

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.
