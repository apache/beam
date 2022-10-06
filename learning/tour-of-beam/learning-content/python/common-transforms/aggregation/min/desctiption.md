# Min

Min transforms find the minimum values globally or for each key in the input collection.

### Minimum element in a PCollection

You can use ```CombineGlobally(lambda elements: min(elements or [-1]))``` to get the minimum element from the entire ```PCollection```.

```
import apache_beam as beam

with beam.Pipeline() as pipeline:
  min_element = (
      pipeline
      | 'Create numbers' >> beam.Create([3, 4, 1, 2])
      | 'Get min value' >>
      beam.CombineGlobally(lambda elements: min(elements or [-1]))
      | beam.Map(print))
```

Output
```
1
```

### Minimum elements for each key

You can use ```Combine.PerKey()``` to get the minimum element for each unique key in a ```PCollection``` of key-values.

```
import apache_beam as beam

with beam.Pipeline() as pipeline:
  elements_with_min_value_per_key = (
      pipeline
      | 'Create produce' >> beam.Create([
          ('🥕', 3),
          ('🥕', 2),
          ('🍆', 1),
          ('🍅', 4),
          ('🍅', 5),
          ('🍅', 3),
      ])
      | 'Get min value per key' >> beam.CombinePerKey(min)
      | beam.Map(print))
```

Output

```
('🥕', 2)
('🍆', 1)
('🍅', 3)
```

### Description for example

Created a list of integers ```PCollection```. The ```beam.combiners.Top.Smallest(5)``` to return the small number than [5] from `PCollection`.