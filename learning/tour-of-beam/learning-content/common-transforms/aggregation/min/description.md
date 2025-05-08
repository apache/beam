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

# Min

Min transforms find the minimum values globally or for each key in the input collection.
{{if (eq .Sdk "go")}}
You can find the global minimum value from the `PCollection` by using `Min()`

```
import (
  "github.com/apache/beam/sdks/go/pkg/beam"
  "github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
  return stats.Min(s, input)
}
```

You can use `MinPerKey()` to calculate the minimum Integer associated with each unique key (which is of type String).

```
import (
  "github.com/apache/beam/sdks/go/pkg/beam"
  "github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
  return stats.MinPerKey(s, input)
}
```
{{end}}
{{if (eq .Sdk "java")}}
You can find the global minimum value from the `PCollection` by using `Min.integersGlobally()`

```
PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
PCollection<Integer> min = input.apply(Min.integersGlobally());
```

Output

```
1
```

You can use `Min.integersPerKey()` to calculate the minimum Integer associated with each unique key (which is of type String).

```
PCollection<KV<String, Integer>> input = pipeline.apply(
    Create.of(KV.of("🥕", 3),
              KV.of("🥕", 2),
              KV.of("🍆", 1),
              KV.of("🍅", 4),
              KV.of("🍅", 5),
              KV.of("🍅", 3)));
PCollection<KV<String, Integer>> minPerKey = input.apply(Min.integersPerKey());
```

Output

```
KV{🍆, 1}
KV{🥕, 2}
KV{🍅, 3}
```
{{end}}
{{if (eq .Sdk "python")}}
### Minimum element in a PCollection

You can use `CombineGlobally(lambda elements: min(elements or [-1]))` to get the minimum element from the entire `PCollection`.

```
import apache_beam as beam

with beam.Pipeline() as p:
  min_element = (
      p | 'Create numbers' >> beam.Create([3, 4, 1, 2])
      | 'Get min value' >> beam.CombineGlobally(lambda elements: min(elements or [-1]))
      | beam.Map(print))
```

Output
```
1
```

### Minimum elements for each key

You can use `Combine.PerKey()` to get the minimum element for each unique key in a `PCollection` of key-values.

```
import apache_beam as beam

with beam.Pipeline() as p:
  elements_with_min_value_per_key = (
      p | 'Create produce' >> beam.Create([
          ('🥕', 3),
          ('🥕', 2),
          ('🍆', 1),
          ('🍅', 4),
          ('🍅', 5),
          ('🍅', 3),])
      | 'Get min value per key' >> beam.CombinePerKey(min)
      | beam.Map(print))
```

Output

```
('🥕', 2)
('🍆', 1)
('🍅', 3)
```
{{end}}


### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.
{{if (eq .Sdk "go")}}
`Min` returns the minimum number from the `PCollection`. If you replace the `integers input` with this `map input`:

```
input:= beam.ParDo(s, func(_ []byte, emit func(int, int)){
  emit(1,1)
  emit(1,4)
  emit(2,6)
  emit(2,3)
  emit(2,-4)
  emit(3,23)
}, beam.Impulse(s))
```

And replace `stats.Min` with `stats.MinPerKey` it will output the minimum numbers by key.
{{end}}
{{if (eq .Sdk "java")}}
`Min.integersGlobally` returns the minimum number from the `PCollection`. If you replace the `integers input` with this `map input`:

```
PCollection<KV<Integer, Integer>> input = pipeline.apply(
    Create.of(KV.of(1, 11),
    KV.of(1, 36),
    KV.of(2, 91),
    KV.of(3, 33),
    KV.of(3, 11),
    KV.of(4, 33)));
```

And replace `Min.integersGlobally` with `Min.integersPerKey` it will output the minimum numbers by key. It is also necessary to replace the generic type:

```
PCollection<KV<Integer, Integer>> output = applyTransform(input);
```

```
static PCollection<KV<Integer, Integer>> applyTransform(PCollection<KV<Integer, Integer>> input) {
        return input.apply(Min.integersPerKey());
    }
```
{{end}}
{{if (eq .Sdk "python")}}
`Top.Smallest` returns smaller numbers from `PCollection` than specified in the function argument. If you replace the `integers input` with this `map input` and replace `beam.combiners.Top.Smallest(5)` with `beam.CombinePerKey(min)` it will output the minimum numbers by key :

```
p | beam.Create([(1, 36),(2, 91),(3, 33),(3, 11),(4, 67),]) | beam.CombinePerKey(min)
```
{{end}}

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.
