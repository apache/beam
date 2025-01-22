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

`PCollection` datasets can be filtered using the `Filter` transform. You can create a filter by supplying a predicate and, when applied, filtering out all the elements of `PCollection` that don’t satisfy the predicate.

{{if (eq .Sdk "go")}}
```
import (
  "github.com/apache/fbeam/sdks/go/pkg/beam"
  "github.com/apache/beam/sdks/go/pkg/beam/transforms/filter"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
  return filter.Exclude(s, input, func(element int) bool {
    return element % 2 == 1
  })
}
```
{{end}}
{{if (eq .Sdk "java")}}
```
PCollection<String> input = pipeline
        .apply(Create.of(List.of("Hello","world","Hi")));

PCollection<String> filteredStrings = input
        .apply(Filter.by(new SerializableFunction<String, Boolean>() {
            @Override
            public Boolean apply(String input) {
                return input.length() > 3;
            }
        }));
```

Output

```
Hello
world
```

### Built-in filters

The Java SDK has several filter methods built-in, like `Filter.greaterThan` and `Filter.lessThan`  With `Filter.greaterThan`, the input `PCollection` can be filtered so that only the elements whose values are greater than the specified amount remain. Similarly, you can use `Filter.lessThan` to filter out elements of the input `PCollection` whose values are greater than the specified amount.

Other built-in filters are:

* Filter.greaterThanEq
* Filter.greaterThan
* Filter.lessThan
* Filter.lessThanEq
* Filter.equal


## Example 2: Filtering with built-in methods

```
// List of integers
PCollection<Integer> input = pipeline.apply(Create.of(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));

PCollection<Integer> greaterThanEqNumbers = input.apply(Filter.greaterThanEq(3));
// PCollection will contain [3, 4, 5, 6, 7, 8, 9, 10] at this point

PCollection<Integer> greaterThanNumbers = input.apply(Filter.greaterThan(4));
// PCollection will contain [5, 6, 7, 8, 9, 10] at this point


PCollection<Integer> lessThanNumbers = input.apply(Filter.lessThan(10));
// PCollection will contain [1, 2, 3, 4, 5, 6, 7, 8, 9] at this point


PCollection<Integer> lessThanEqNumbers = input.apply(Filter.lessThanEq(7));
// PCollection will contain [1, 2, 3, 4 5, 6, 7] at this point


PCollection<Integer> equalNumbers = input.apply(Filter.equal(9));
// PCollection will contain [9] at this point
```
{{end}}
{{if (eq .Sdk "python")}}
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

with beam.Pipeline() as p:
  perennials = (
      p | 'Gardening plants' >> beam.Create([
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

with beam.Pipeline() as p:
  perennials = (
      p | 'Gardening plants' >> beam.Create([
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

You can pass functions with multiple arguments to `Filter`. They are passed as additional positional arguments or keyword arguments to the function.

In this example, `has_duration` takes `plant` and `duration` as arguments.

```
import apache_beam as beam

def has_duration(plant, duration):
  return plant['duration'] == duration

with beam.Pipeline() as p:
  perennials = (
      p | 'Gardening plants' >> beam.Create([
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

If the `PCollection` has a single value, such as the average from another computation, passing the `PCollection` as a singleton accesses that value.

In this example, we pass a `PCollection` the value **perennial** as a singleton. We then use that value to filter out perennials.

```
import apache_beam as beam

with beam.Pipeline() as p:
  perennial = p | 'Perennial' >> beam.Create(['perennial'])

  perennials = (
      p | 'Gardening plants' >> beam.Create([
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

If the `PCollection` has multiple values, pass the PCollection as an iterator. This accesses elements lazily as they are needed, so it is possible to iterate over large PCollections that won’t fit into memory.

```
import apache_beam as beam

with beam.Pipeline() as p:
  valid_durations = p | 'Valid durations' >> beam.Create([
      'annual',
      'biennial',
      'perennial',
  ])

  valid_plants = (
      p | 'Gardening plants' >> beam.Create([
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

If a `PCollection` is small enough to fit into memory, then that `PCollection` can be passed as a dictionary. Each element must be a `(key, value)` pair. Note that all the elements of the `PCollection` must fit into memory for this. If the `PCollection` won’t fit into memory, use `beam.pvalue.AsIter(pcollection)` instead.

```
import apache_beam as beam

with beam.Pipeline() as p:
  keep_duration = p | 'Duration filters' >> beam.Create([
      ('annual', False),
      ('biennial', False),
      ('perennial', True),
  ])

  perennials = (
      p | 'Gardening plants' >> beam.Create([
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
{{end}}
### Playground exercise

You can find the complete code of the above example using 'Filter' in the playground window, which you can run and experiment with.

Filter transform can be used with both text and numerical collection. For example, let's try filtering the input collection that contains words so that only words that start with the letter 'a' are returned.

You can also chain several filter transforms to form more complex filtering based on several simple filters or implement more complex filtering logic within a single filter transform. For example, try both approaches to filter the same list of words such that only ones that start with a letter 'a' (regardless of the case) and containing more than three symbols are returned.

**Hint**

You can use the following code snippet to create an input PCollection:

Don't forget to add import:

```
import (
    "strings"
    ...
)
```

Create data for PCollection:

```
str := "To be, or not to be: that is the question: Whether 'tis nobler in the mind to suffer The slings and arrows of outrageous fortune, Or to take arms against a sea of troubles, And by opposing end them. To die: to sleep"

input := beam.CreateList(s,strings.Split(str, " "))
```

And filtering:

```
func applyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
    return filter.Include(s, input, func(word string) bool {
		return strings.HasPrefix(strings.ToUpper(word), "A")
    })
}
```
