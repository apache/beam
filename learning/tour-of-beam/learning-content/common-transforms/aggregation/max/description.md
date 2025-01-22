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

`Max` provides a variety of different transforms for computing the maximum values in a collection, either globally or for each key.

{{if (eq .Sdk "go")}}
You can find the global maximum value from the `PCollection` by using `Max()`

```
import (
  "github.com/apache/beam/sdks/go/pkg/beam"
  "github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
  return stats.Max(s, input)
}
```

You can use `MaxPerKey()` to calculate the maximum Integer associated with each unique key (which is of type String).

```
import (
  "github.com/apache/beam/sdks/go/pkg/beam"
  "github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
  return stats.MaxPerKey(s, input)
}
```
{{end}}
{{if (eq .Sdk "java")}}
You can find the global maximum value from the `PCollection` by using `Max.integersGlobally()`

```
PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
PCollection<Integer> max = input.apply(Max.integersGlobally());
```

Output

```
10
```

You can use `Max.integersPerKey()` to calculate the maximum Integer associated with each unique key (which is of type String).

```
PCollection<KV<String, Integer>> input = pipeline.apply(
    Create.of(KV.of("🥕", 3),
              KV.of("🥕", 2),
              KV.of("🍆", 1),
              KV.of("🍅", 4),
              KV.of("🍅", 5),
              KV.of("🍅", 3)));

PCollection<KV<String, Integer>> maxPerKey = input.apply(Max.integersPerKey());
```

Output

```
KV{🍅, 5}
KV{🥕, 3}
KV{🍆, 1}
```
{{end}}
{{if (eq .Sdk "python")}}

### Maximum element in a PCollection

You can use `CombineGlobally(lambda elements: max(elements or [None]))` to get the maximum element from the entire `PCollection`.

```
import apache_beam as beam

with beam.Pipeline() as p:
  max_element = (
      p | 'Create numbers' >> beam.Create([3, 4, 1, 2])
      | 'Get max value' >> beam.CombineGlobally(lambda elements: max(elements or [None]))
      | beam.Map(print))
```

Output

```
4
```

### Maximum elements for each key

You can use `Combine.PerKey()` to get the maximum element for each unique key in a PCollection of key-values.

```
import apache_beam as beam

with beam.Pipeline() as p:
  elements_with_max_value_per_key = (
      p | 'Create produce' >> beam.Create([
          ('🥕', 3),
          ('🥕', 2),
          ('🍆', 1),
          ('🍅', 4),
          ('🍅', 5),
          ('🍅', 3),])
      | 'Get max value per key' >> beam.CombinePerKey(max)
      | beam.Map(print))
```

Output

```
('🥕', 3)
('🍆', 1)
('🍅', 5)
```
{{end}}

### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.
{{if (eq .Sdk "go")}}
`Max` returns the maximum number from the `PCollection`. If you replace the `integers input` with this `map input`:

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

And replace `stats.Max` with `stats.MaxPerKey` it will output the maximum numbers by key.
{{end}}
{{if (eq .Sdk "java")}}
`Max.doublesGlobally` returns the maximum element from the `PCollection`. If you replace the `integers input` with this `map input`:

```
PCollection<KV<Integer, Integer>> input = pipeline.apply(
    Create.of(KV.of(1, 11),
    KV.of(1, 36),
    KV.of(2, 91),
    KV.of(3, 33),
    KV.of(3, 11),
    KV.of(4, 33)));
```

And replace `Max.doublesGlobally` with `Max.integersPerKey` it will output the maximum numbers by key. It is also necessary to replace the generic type:

```
PCollection<KV<Integer, Integer>> output = applyTransform(input);
```

```
static PCollection<KV<Integer, Integer>> applyTransform(PCollection<KV<Integer, Integer>> input) {
        return input.apply(Max.integersPerKey());
    }
```
{{end}}
{{if (eq .Sdk "python")}}
`Top.Largest` returns larger numbers from `PCCollection` than specified in the function argument. If you replace the `integers input` with this `map input` and replace `beam.combiners.Top.Largest(5)` with `beam.CombinePerKey(max)` it will output the maximum numbers by key :

```
beam.Create([
    (1, 36),
    (2, 91),
    (3, 33),
    (3, 11),
    (4, 67),
]) | beam.CombinePerKey(max)
```
{{end}}

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.
