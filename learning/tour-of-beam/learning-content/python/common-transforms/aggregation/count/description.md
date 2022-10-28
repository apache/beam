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

# Count

`Count` provides many transformations for calculating the count of values in a `PCollection`, either globally or for each key.

### Counting all elements in a PCollection

You can use ```Count.Globally()``` to count all elements in a PCollection, even if there are duplicate elements.

```
import apache_beam as beam

with beam.Pipeline() as pipeline:
  total_elements = (
      pipeline
      | 'Create plants' >> beam.Create(
          ['🍓', '🥕', '🥕', '🥕', '🍆', '🍆', '🍅', '🍅', '🍅', '🌽'])
      | 'Count all elements' >> beam.combiners.Count.Globally()
      | beam.Map(print))
```

Output

```
10
```

### Counting elements for each key

You can use ```Count.PerKey()``` to count the elements for each unique key in a PCollection of key-values.

```
import apache_beam as beam

with beam.Pipeline() as pipeline:
  total_elements_per_keys = (
      pipeline
      | 'Create plants' >> beam.Create([
          ('spring', '🍓'),
          ('spring', '🥕'),
          ('summer', '🥕'),
          ('fall', '🥕'),
          ('spring', '🍆'),
          ('winter', '🍆'),
          ('spring', '🍅'),
          ('summer', '🍅'),
          ('fall', '🍅'),
          ('summer', '🌽'),
      ])
      | 'Count elements per key' >> beam.combiners.Count.PerKey()
      | beam.Map(print))
```

Output

```
('spring', 4)
('summer', 3)
('fall', 2)
('winter', 1)
```

### Counting all unique elements

You can use ```Count.PerElement()``` to count only the unique elements in a PCollection.

```
import apache_beam as beam

with beam.Pipeline() as pipeline:
  total_unique_elements = (
      pipeline
      | 'Create produce' >> beam.Create(
          ['🍓', '🥕', '🥕', '🥕', '🍆', '🍆', '🍅', '🍅', '🍅', '🌽'])
      | 'Count unique elements' >> beam.combiners.Count.PerElement()
      | beam.Map(print))
```

Output

```
('🍓', 1)
('🥕', 3)
('🍆', 2)
('🍅', 3)
('🌽', 1)
```

### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

`Count.globally` returns the number of integers from the `PCollection`. If you replace the `integers input` with this `map input` and replace `beam.combiners.Count.Globally` on `beam.combiners.Count.PerKey` it will output the count numbers by key :

```
beam.Create([
    (1, 36),
    (2, 91),
    (3, 33),
    (3, 11),
    (4, 67),
]) | beam.combiners.Count.PerKey()
```

And Count transforms work with strings too! Can you change the example to count the number of words in a given sentence and how often each word occurs?

Count how many words are repeated with `Count`:

```
  (p | beam.Create(["To be, or not to be: that is the question:Whether 'tis nobler in the mind to suffer The slings and arrows of outrageous fortune,Or to take arms against a sea of troubles,And by opposing end them. To die: to sleep"])
  | beam.ParDo(SplitWords())
  | beam.combiners.Count.PerElement()
  | Output(prefix='PCollection filtered value: '))
```

For split word you can use:

```
class SplitWords(beam.DoFn):
  def __init__(self, delimiter=' '):
    self.delimiter = delimiter

  def process(self, text):
    for word in text.split(self.delimiter):
      yield word
```

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.