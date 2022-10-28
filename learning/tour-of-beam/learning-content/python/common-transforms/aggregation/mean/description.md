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

# Mean

You can use Mean transforms to compute the arithmetic mean of the elements in a collection or the mean of the values associated with each key in a collection of key-value pairs.

### Mean of element in a PCollection

You can find the global mean value from the ```PCollection``` by using ```Mean.Globally()```

```
import apache_beam as beam

with beam.Pipeline() as pipeline:
  mean_element = (
      pipeline
      | 'Create numbers' >> beam.Create([3, 4, 1, 2])
      | 'Get mean value' >> beam.combiners.Mean.Globally()
      | beam.Map(print))
```

Output

```
2.5
```

### Mean of elements for each key

You can use ```Mean.PerKey()``` to get the average of the elements for each unique key in a ```PCollection``` of key-values.

```
import apache_beam as beam

with beam.Pipeline() as pipeline:
  elements_with_mean_value_per_key = (
      pipeline
      | 'Create produce' >> beam.Create([
          ('🥕', 3),
          ('🥕', 2),
          ('🍆', 1),
          ('🍅', 4),
          ('🍅', 5),
          ('🍅', 3),
      ])
      | 'Get mean value per key' >> beam.combiners.Mean.PerKey()
      | beam.Map(print))
```

Output

```
2.5
```

### Mean of elements for each key

You can use ```Mean.PerKey()``` to get the average of the elements for each unique key in a ```PCollection``` of key-values.

```
import apache_beam as beam

with beam.Pipeline() as pipeline:
  elements_with_mean_value_per_key = (
      pipeline
      | 'Create produce' >> beam.Create([
          ('🥕', 3),
          ('🥕', 2),
          ('🍆', 1),
          ('🍅', 4),
          ('🍅', 5),
          ('🍅', 3),
      ])
      | 'Get mean value per key' >> beam.combiners.Mean.Globally()
      | beam.Map(print))
```

Output

```
('🥕', 2.5)
('🍆', 1.0)
('🍅', 4.0)
```

### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

`beam.combiners.Mean.Globally()` returns mean from `PCCollection`. If you replace the `integers input` with this `map input` and replace `beam.combiners.Mean.Globally()` on `beam.combiners.Mean.PerKey()` it will output the mean by key :

```
beam.Create([
    (1, 36),
    (2, 91),
    (3, 33),
    (3, 11),
    (4, 67),
]) | beam.combiners.Mean.PerKey()
```

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.