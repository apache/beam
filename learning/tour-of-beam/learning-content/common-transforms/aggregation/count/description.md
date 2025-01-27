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

{{if (eq .Sdk "go")}}
Counts the number of elements within each aggregation. The Count transform has two varieties:

You can count the number of elements in a `PCollection` with `CountElms()`, it will return one element.

```
import (
    "github.com/apache/beam/sdks/go/pkg/beam"
    "github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
    return stats.CountElms(s, input)
}
```

You can use `Count()` to count how many elements are associated with a particular key. The result will be one output for each key.

```
import (
    "github.com/apache/beam/sdks/go/pkg/beam"
    "github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
    return stats.Count(s, input)
}
```
{{end}}
{{if (eq .Sdk "java")}}
Counts the number of elements within each aggregation. The Count transform has three varieties:

### Counting all elements in a PCollection

`Count.globally()` counts the number of elements in the entire `PCollection`. The result is a collection with a single element.

```
PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
PCollection<Long> output = input.apply(Count.globally());
```

Output
```
10
```

### Counting elements for each key

`Count.perKey()` counts how many elements are associated with each key. It ignores the values. The resulting collection has one output for every key in the input collection.

```
PCollection<KV<String, Integer>> input = pipeline.apply(
    Create.of(KV.of("🥕", 3),
              KV.of("🥕", 2),
              KV.of("🍆", 1),
              KV.of("🍅", 4),
              KV.of("🍅", 5),
              KV.of("🍅", 3)));
PCollection<KV<String, Long>> output = input.apply(Count.perKey());
```

Output

```
KV{🥕, 2}
KV{🍅, 3}
KV{🍆, 1}
```

### Counting all unique elements

`Count.perElement()` counts how many times each element appears in the input collection. The output collection is a key-value pair, containing each unique element and the number of times it appeared in the original collection.

```
PCollection<KV<String, Integer>> input = pipeline.apply(
    Create.of(KV.of("🥕", 3),
              KV.of("🥕", 2),
              KV.of("🍆", 1),
              KV.of("🍅", 3),
              KV.of("🍅", 5),
              KV.of("🍅", 3)));
PCollection<KV<String, Long>> output = input.apply(Count.perElement());
```

Output

```
KV{KV{🍅, 3}, 2}
KV{KV{🥕, 2}, 1}
KV{KV{🍆, 1}, 1}
KV{KV{🥕, 3}, 1}
KV{KV{🍅, 5}, 1}
```
{{end}}
{{if (eq .Sdk "python")}}
### Counting all elements in a PCollection

You can use `Count.Globally()` to count all elements in a PCollection, even if there are duplicate elements.

```
import apache_beam as beam

with beam.Pipeline() as p:
  total_elements = (
      p | 'Create plants' >> beam.Create(['🍓', '🥕', '🥕', '🥕', '🍆', '🍆', '🍅', '🍅', '🍅', '🌽'])
      | 'Count all elements' >> beam.combiners.Count.Globally()
      | beam.Map(print))
```

Output

```
10
```

### Counting elements for each key

You can use `Count.PerKey()` to count the elements for each unique key in a PCollection of key-values.

```
import apache_beam as beam

with beam.Pipeline() as p:
  total_elements_per_keys = (
      p | 'Create plants' >> beam.Create([
          ('spring', '🍓'),
          ('spring', '🥕'),
          ('summer', '🥕'),
          ('fall', '🥕'),
          ('spring', '🍆'),
          ('winter', '🍆'),
          ('spring', '🍅'),
          ('summer', '🍅'),
          ('fall', '🍅'),
          ('summer', '🌽'),])
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

You can use `Count.PerElement()` to count only the unique elements in a `PCollection`.

```
import apache_beam as beam

with beam.Pipeline() as p:
  total_unique_elements = (
      p | 'Create produce' >> beam.Create(['🍓', '🥕', '🥕', '🥕', '🍆', '🍆', '🍅', '🍅', '🍅', '🌽'])
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
{{end}}
### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.
{{if (eq .Sdk "python")}}
`Count.globally` returns the number of integers from the `PCollection`. If you replace the `integers input` with this `map input` and replace `beam.combiners.Count.Globally` with `beam.combiners.Count.PerKey` it will output the count numbers by key :

```
p | beam.Create([(1, 36), (2, 91), (3, 33), (3, 11), (4, 67),])
  | beam.combiners.Count.PerKey()
```

`Count` transforms work with strings too! Can you change the example to count the number of words in a given sentence and how often each word occurs?

Count how many words are repeated with `Count`:

```
p | beam.Create(["To be, or not to be: that is the question: Whether 'tis nobler in the mind to suffer, the slings and arrows of outrageous fortune, or to take arms against a sea of troubles, and by opposing end them. To die: to sleep"])
  | beam.ParDo(SplitWords()) | beam.combiners.Count.PerElement()
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
{{end}}
{{if (eq .Sdk "go")}}
`CountElms` returns the number of integers from the `PCollection`. If you replace `CountElms` with `Count` you can count the elements by the values of how many times they met.
{{end}}
{{if (eq .Sdk "java")}}
`Count` returns the count elements from the `PCollection`. If you replace the `integers input` with this `map input`:

```
PCollection<KV<Integer, Integer>> input = pipeline.apply(
    Create.of(KV.of(1, 11),
    KV.of(1, 36),
    KV.of(2, 91),
    KV.of(3, 33),
    KV.of(3, 11),
    KV.of(4, 33)));
```

And replace `Count.globally` with `Count.perKey` it will output the count numbers by key. It is also necessary to replace the generic type:

```
PCollection<KV<Integer, Long>> output = applyTransform(input);
```

```
static PCollection<KV<Integer, Long>> applyTransform(PCollection<KV<Integer, Integer>> input) {
        return input.apply(Count.globally());
}
```
{{end}}

{{if (eq .Sdk "go java")}}
And `Count` transforms work with strings too! Can you change the example to count the number of words in a given sentence and how often each word occurs?

Don't forget to add import:
{{end}}
{{if (eq .Sdk "go")}}
```
import (
    "strings"
    ...
)
```
{{end}}
{{if (eq .Sdk "java")}}
```
import java.util.Arrays;
import org.apache.beam.sdk.values.KV;
```
{{end}}

{{if (eq .Sdk "go java")}}
Create data for PCollection:

```
String str = "To be, or not to be: that is the question:Whether 'tis nobler in the mind to suffer The slings and arrows of outrageous fortune,Or to take arms against a sea of troubles,And by opposing end them. To die: to sleep";

PCollection<String> input = pipeline.apply(Create.of(Arrays.asList(str.split(" "))));
```
{{end}}
{{if (eq .Sdk "go")}}
Count how many words are repeated with `Count`:

```
func applyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return stats.Count(s, input)
}
```
{{end}}
{{if (eq .Sdk "java")}}
Count how many words are repeated with `Count.perElement`:

```
static PCollection<KV<String, Long>> applyTransform(PCollection<String> input) {
    return input.apply(Count.perElement());
}
```
{{end}}

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.
