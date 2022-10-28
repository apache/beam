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

# Max

Max provides a variety of different transforms for computing the maximum values in a collection, either globally or for each key.

### Maximum element in a PCollection

You can use ```CombineGlobally(lambda elements: max(elements or [None]))``` to get the maximum element from the entire ```PCollection```.

```
import apache_beam as beam

with beam.Pipeline() as pipeline:
  max_element = (
      pipeline
      | 'Create numbers' >> beam.Create([3, 4, 1, 2])
      | 'Get max value' >>
      beam.CombineGlobally(lambda elements: max(elements or [None]))
      | beam.Map(print))
```

Output

```
4
```

### Maximum elements for each key

You can use ```Combine.PerKey()``` to get the maximum element for each unique key in a PCollection of key-values.

```
import apache_beam as beam

with beam.Pipeline() as pipeline:
  elements_with_max_value_per_key = (
      pipeline
      | 'Create produce' >> beam.Create([
          ('🥕', 3),
          ('🥕', 2),
          ('🍆', 1),
          ('🍅', 4),
          ('🍅', 5),
          ('🍅', 3),
      ])
      | 'Get max value per key' >> beam.CombinePerKey(max)
      | beam.Map(print))
```

Output

```
('🥕', 3)
('🍆', 1)
('🍅', 5)
```

### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

`Top.Largest` returns larger numbers from `PCCollection` than specified in the function argument. If you replace the `integers input` with this `map input` and replace `beam.combiners.Top.Largest(5)` on `beam.CombinePerKey(max)` it will output the maximum numbers by key :

```
beam.Create([
    (1, 36),
    (2, 91),
    (3, 33),
    (3, 11),
    (4, 67),
]) | beam.CombinePerKey(max)
```

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.