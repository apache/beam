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

You can find the global minimum value from the ```PCollection``` by using ```Min()```

```
import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return stats.Min(s, input)
}
```

You can use ```MinPerKey()``` to calculate the minimum Integer associated with each unique key (which is of type String).

```
import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return stats.MinPerKey(s, input)
}
```

You can find the full code of this example in the playground window, which you can run and experiment with.

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

And replace `stats.Min` on `stats.MinPerKey` it will output the minimum numbers by key.

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.