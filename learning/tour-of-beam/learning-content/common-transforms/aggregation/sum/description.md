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
{{if (eq .Sdk "go")}}
```
import (
  "github.com/apache/beam/sdks/go/pkg/beam"
  "github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
  return stats.Sum(s, input)
}
```

You can use `SumPerKey()` to calculate the sum Integer associated with each unique key.

```
import (
  "github.com/apache/beam/sdks/go/pkg/beam"
  "github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
  return stats.SumPerKey(s, input)
}
```
{{end}}
{{if (eq .Sdk "java")}}
You can find the global sum value from the `PCollection` by using `Sum.doublesGlobally()`

```
PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
PCollection<Double> sum = input.apply(Sum.doublesGlobally());
```

Output

```
55
```

You can use `Sum.integersPerKey()` to calculate the sum Integer associated with each unique key (which is of type String).

```
PCollection<KV<String, Integer>> input = pipeline.apply(
    Create.of(KV.of("🥕", 3),
              KV.of("🥕", 2),
              KV.of("🍆", 1),
              KV.of("🍅", 4),
              KV.of("🍅", 5),
              KV.of("🍅", 3)));
PCollection<KV<String, Integer>> sumPerKey = input.apply(Sum.integersPerKey());
```

Output

```
KV{🍆, 1}
KV{🍅, 12}
KV{🥕, 5}
```
{{end}}
{{if (eq .Sdk "python")}}
### Sum of the elements in a PCollection

You can find the global sum value from the `PCollection` by using `CombineGlobally(sum)`

```
import apache_beam as beam

with beam.Pipeline() as p:
  total = (
      p | 'Create numbers' >> beam.Create([3, 4, 1, 2])
      | 'Sum values' >> beam.CombineGlobally(sum)
      | beam.Map(print))
```

Output

```
10
```

### Sum of the elements for each key

You can use `Combine.PerKey()` to get the sum of all the element values for each unique key in a `PCollection` of key-values.

```
import apache_beam as beam

with beam.Pipeline() as p:
  totals_per_key = (
      p | 'Create produce' >> beam.Create([
          ('🥕', 3),
          ('🥕', 2),
          ('🍆', 1),
          ('🍅', 4),
          ('🍅', 5),
          ('🍅', 3),])
      | 'Sum values per key' >> beam.CombinePerKey(sum)
      | beam.Map(print))
```

Output
```
('🥕', 5)
('🍆', 1)
('🍅', 12)
```
{{end}}
You can use `Sum()` to sum the elements of a `PCollection`.

### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

{{if (eq .Sdk "go")}}
`Sum` returns the sum from the `PCollection`. If you replace the `integers input` with this `map input`:

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

And replace `stats.Sum` with `stats.SumPerKey` it will output the sum by key.
{{end}}
{{if (eq .Sdk "java")}}
`Sum.integersGlobally` returns the sum from the `PCollection`. If you replace the `integers input` with this `map input`:

```
PCollection<KV<Integer, Integer>> input = pipeline.apply(
    Create.of(KV.of(1, 11),
    KV.of(1, 36),
    KV.of(2, 91),
    KV.of(3, 33),
    KV.of(3, 11),
    KV.of(4, 33)));
```

And replace `Sum.integersGlobally` with `Sum.integersPerKey` it will output the sum by key. It is also necessary to replace the generic type:

```
PCollection<KV<Integer, Integer>> output = applyTransform(input);
```

```
static PCollection<KV<Integer, Integer>> applyTransform(PCollection<KV<Integer, Integer>> input) {
        return input.apply(Sum.integersPerKey());
    }
```
{{end}}
{{if (eq .Sdk "python")}}
`beam.CombineGlobally(sum)` returns sum from `PCCollection`. If you replace the `integers input` with this `map input` and replace `beam.CombineGlobally(sum)` with `beam.CombinePerKey(sum)` it will output the sum by key :

```
p | beam.Create([(1, 36),(2, 91),(3, 33),(3, 11),(4, 67),]) | beam.CombinePerKey(sum)
```
{{end}}

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.
