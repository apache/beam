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

WithKeys takes a ```PCollection<V>``` and produces a ```PCollection<KV<K, V>>``` by associating each input element with a key.

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

### Description for example

Given a list of strings, the output consists of a map with a key that is the first letter of the word , and whose value is the word itself.