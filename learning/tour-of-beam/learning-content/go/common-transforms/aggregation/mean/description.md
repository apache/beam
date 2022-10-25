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

```Mean()``` returns a transformation that returns a collection whose content is the average of the elements of the input collection. If there are no elements in the input collection, 0 is returned.

```
import (
  "github.com/apache/beam/sdks/go/pkg/beam"
  "github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
  return stats.Mean(s, input)
}
```

You can use ```MeanPerKey()``` to calculate the mean of the elements associated with each unique key.

```
import (
  "github.com/apache/beam/sdks/go/pkg/beam"
  "github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
  return stats.MeanPerKey(s, input)
}
```

You can find the full code of this example in the playground window, which you can run and experiment with.

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

And replace `stats.Mean` on `stats.MeanPerKey` it will output the mean by key.

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.