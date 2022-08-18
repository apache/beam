# WithKeys

Takes a ```PCollection<V>``` and produces a ```PCollection<KV<K, V>>``` by associating each input element with a key.

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