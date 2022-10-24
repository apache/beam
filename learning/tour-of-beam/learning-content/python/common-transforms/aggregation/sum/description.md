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

# Sum

You can use Sum transforms to compute the sum of the elements in a collection or the sum of the values associated with each key in a collection of key-value pairs.

### Sum of the elements in a PCollection

You can find the global sum value from the ```PCollection``` by using ```CombineGlobally(sum)```

```
import apache_beam as beam

with beam.Pipeline() as pipeline:
  total = (
      pipeline
      | 'Create numbers' >> beam.Create([3, 4, 1, 2])
      | 'Sum values' >> beam.CombineGlobally(sum)
      | beam.Map(print))
```

Output

```
10
```

### Sum of the elements for each key

You can use ```Combine.PerKey()``` to get the sum of all the element values for each unique key in a ```PCollection``` of key-values.

```
import apache_beam as beam

with beam.Pipeline() as pipeline:
  totals_per_key = (
      pipeline
      | 'Create produce' >> beam.Create([
          ('🥕', 3),
          ('🥕', 2),
          ('🍆', 1),
          ('🍅', 4),
          ('🍅', 5),
          ('🍅', 3),
      ])
      | 'Sum values per key' >> beam.CombinePerKey(sum)
      | beam.Map(print))
```

Output
```
('🥕', 5)
('🍆', 1)
('🍅', 12)
```

You can find the full code of this example in the playground window, which you can run and experiment with.


`beam.CombineGlobally(sum)` returns sum from `PCCollection`. If you replace the `integers input` with this `map input` and replace `beam.CombineGlobally(sum)` on `beam.CombinePerKey(sum)` it will output the sum by key :

```
beam.Create([
    (1, 36),
    (2, 91),
    (3, 33),
    (3, 11),
    (4, 67),
]) | beam.CombinePerKey(sum)
```

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.