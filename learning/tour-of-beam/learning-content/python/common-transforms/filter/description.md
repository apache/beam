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

### Using Filter

PCollection datasets can be filtered using the Filter transform. You can create a filter by supplying a predicate and, when applied, filtering out all the elements of PCollection that don’t satisfy the predicate.

```
import apache_beam as beam

from log_elements import LogElements

with beam.Pipeline() as p:
  (p | beam.Create(range(1, 11))
     | beam.Filter(lambda num: num % 2 == 0)
     | LogElements())
```


### Example 1: Filtering with a function

You can define a function `is_perennial()` which returns True if the element’s duration equals 'perennial', and False otherwise.

```
import apache_beam as beam

def is_perennial(plant):
  return plant['duration'] == 'perennial'

with beam.Pipeline() as pipeline:
  perennials = (
      pipeline
      | 'Gardening plants' >> beam.Create([
          {
              'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'
          },
          {
              'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'
          },
          {
              'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'
          },
          {
              'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'
          },
          {
              'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'
          },
      ])
      | 'Filter perennials' >> beam.Filter(is_perennial)
      | beam.Map(print))
```

Output

```
{'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'}
{'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'}
{'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'}
```

### Example 2: Filtering with a lambda function

You can also use lambda functions to simplify Example 1.

```
import apache_beam as beam

with beam.Pipeline() as pipeline:
  perennials = (
      pipeline
      | 'Gardening plants' >> beam.Create([
          {
              'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'
          },
          {
              'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'
          },
          {
              'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'
          },
          {
              'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'
          },
          {
              'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'
          },
      ])
      | 'Filter perennials' >>
      beam.Filter(lambda plant: plant['duration'] == 'perennial')
      | beam.Map(print))
```

Output

```
{'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'}
{'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'}
{'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'}
```

### Example 3: Filtering with multiple arguments

You can pass functions with multiple arguments to ```Filter```. They are passed as additional positional arguments or keyword arguments to the function.

In this example, ```has_duration``` takes ```plant``` and ```duration``` as arguments.

```
import apache_beam as beam

def has_duration(plant, duration):
  return plant['duration'] == duration

with beam.Pipeline() as pipeline:
  perennials = (
      pipeline
      | 'Gardening plants' >> beam.Create([
          {
              'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'
          },
          {
              'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'
          },
          {
              'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'
          },
          {
              'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'
          },
          {
              'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'
          },
      ])
      | 'Filter perennials' >> beam.Filter(has_duration, 'perennial')
      | beam.Map(print))
```

Output

```
{'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'}
{'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'}
{'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'}
```

### Example 4: Filtering with side inputs as singletons

If the ```PCollection``` has a single value, such as the average from another computation, passing the ```PCollection``` as a singleton accesses that value.

In this example, we pass a ```PCollection``` the value 'perennial' as a singleton. We then use that value to filter out perennials.

```
import apache_beam as beam

with beam.Pipeline() as pipeline:
  perennial = pipeline | 'Perennial' >> beam.Create(['perennial'])

  perennials = (
      pipeline
      | 'Gardening plants' >> beam.Create([
          {
              'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'
          },
          {
              'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'
          },
          {
              'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'
          },
          {
              'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'
          },
          {
              'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'
          },
      ])
      | 'Filter perennials' >> beam.Filter(
          lambda plant,
          duration: plant['duration'] == duration,
          duration=beam.pvalue.AsSingleton(perennial),
      )
      | beam.Map(print))
```

Output

```
{'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'}
{'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'}
{'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'}
```

### Example 5: Filtering with side inputs as iterators

If the ```PCollection``` has multiple values, pass the PCollection as an iterator. This accesses elements lazily as they are needed, so it is possible to iterate over large PCollections that won’t fit into memory.

```
import apache_beam as beam

with beam.Pipeline() as pipeline:
  valid_durations = pipeline | 'Valid durations' >> beam.Create([
      'annual',
      'biennial',
      'perennial',
  ])

  valid_plants = (
      pipeline
      | 'Gardening plants' >> beam.Create([
          {
              'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'
          },
          {
              'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'
          },
          {
              'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'
          },
          {
              'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'
          },
          {
              'icon': '🥔', 'name': 'Potato', 'duration': 'PERENNIAL'
          },
      ])
      | 'Filter valid plants' >> beam.Filter(
          lambda plant,
          valid_durations: plant['duration'] in valid_durations,
          valid_durations=beam.pvalue.AsIter(valid_durations),
      )
      | beam.Map(print))
```

Output

```
{'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'}
{'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'}
{'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'}
{'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'}
```

### Example 6: Filtering with side inputs as dictionaries

If a ```PCollection``` is small enough to fit into memory, then that PCollection can be passed as a dictionary. Each element must be a ```(key, value)``` pair. Note that all the elements of the ```PCollection``` must fit into memory for this. If the ```PCollection``` won’t fit into memory, use ```beam.pvalue.AsIter(pcollection)``` instead.

```
import apache_beam as beam

with beam.Pipeline() as pipeline:
  keep_duration = pipeline | 'Duration filters' >> beam.Create([
      ('annual', False),
      ('biennial', False),
      ('perennial', True),
  ])

  perennials = (
      pipeline
      | 'Gardening plants' >> beam.Create([
          {
              'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'
          },
          {
              'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'
          },
          {
              'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'
          },
          {
              'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'
          },
          {
              'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'
          },
      ])
      | 'Filter plants by duration' >> beam.Filter(
          lambda plant,
          keep_duration: keep_duration[plant['duration']],
          keep_duration=beam.pvalue.AsDict(keep_duration),
      )
      | beam.Map(print))
```

Output

```
{'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'}
{'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'}
{'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'}
```

You can find the full code of this example in the playground window, which you can run and experiment with.

`Filter` returns a number if it is divisible by 2 without remainder. You can use other method for filtering.

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.