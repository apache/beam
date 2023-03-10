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
{{if (eq .Sdk "go")}}
`Mean()` returns a transformation that returns a collection whose content is the average of the elements of the input collection. If there are no elements in the input collection, 0 is returned.

```
import (
  "github.com/apache/beam/sdks/go/pkg/beam"
  "github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
  return stats.Mean(s, input)
}
```

You can use `MeanPerKey()` to calculate the mean of the elements associated with each unique key.

```
import (
  "github.com/apache/beam/sdks/go/pkg/beam"
  "github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
  return stats.MeanPerKey(s, input)
}
```
{{end}}
{{if (eq .Sdk "java")}}
`Mean.globally()` returns a transformation that returns a collection whose content is the average of the elements of the input collection. If there are no elements in the input collection, 0 is returned.

```
PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
PCollection<Double> mean = input.apply(Mean.globally());
```

Output

```
5.5
```


`Mean.perKey()` returns a transform that returns a collection containing an output element mapping each distinct key in the input collection to the mean of the values associated with that key in the input collection.

```
PCollection<KV<String, Integer>> input = pipeline.apply(
    Create.of(KV.of("🥕", 3),
              KV.of("🥕", 2),
              KV.of("🍆", 1),
              KV.of("🍅", 4),
              KV.of("🍅", 5),
              KV.of("🍅", 3)));
PCollection<KV<String, Double>> meanPerKey = input.apply(Mean.perKey());
```

Output

```
KV{🍆, 1.0}
KV{🥕, 2.5}
KV{🍅, 4.0}
```
{{end}}
{{if (eq .Sdk "python")}}
{{end}}


### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.
{{if (eq .Sdk "go")}}
`Mean` returns the mean from the `PCollection`. If you replace the `integers input` with this `map input`:

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

And replace `stats.Mean` with `stats.MeanPerKey` it will output the mean by key.
{{end}}
{{if (eq .Sdk "java")}}
`Mean.globally` returns the mean from the `PCollection`. If you replace the `integers input` with this `map input`:

```
PCollection<KV<Integer, Integer>> input = pipeline.apply(
    Create.of(KV.of(1, 11),
    KV.of(1, 36),
    KV.of(2, 91),
    KV.of(3, 33),
    KV.of(3, 11),
    KV.of(4, 33)));
```

And replace `Mean.globally` with `Mean.perKey` it will output the means by key. It is also necessary to replace the generic type:

```
PCollection<KV<Integer, Double>> output = applyTransform(input);
```

```
static PCollection<KV<Integer, Double>> applyTransform(PCollection<KV<Integer, Integer>> input) {
        return input.apply(Mean.perKey());
    }
```
{{end}}
{{if (eq .Sdk "python")}}
`beam.combiners.Mean.Globally()` returns mean from `PCCollection`. If you replace the `integers input` with this `map input` and replace `beam.combiners.Mean.Globally()` on `beam.combiners.Mean.PerKey()` it will output the mean by key :

```
p | beam.Create([(1, 36),(2, 91),(3, 33),(3, 11),(4, 67),]) | beam.combiners.Mean.PerKey()
```
{{end}}

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.
