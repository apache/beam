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

### The single global window

By default, all data in a `PCollection` is assigned to the single global window, and late data is discarded. If your data set is of a fixed size, you can use the global window default for your `PCollection`.

You can use the single global window if you are working with an unbounded data set (e.g. from a streaming data source) but use caution when applying aggregating transforms such as `GroupByKey` and `Combine`. The single global window with a default trigger generally requires the entire data set to be available before processing, which is not possible with continuously updating data. To perform aggregations on an unbounded `PCollection` that uses global windowing, you should specify a non-default trigger for that `PCollection`.

If your `PCollection` is limited (the size is fixed), you can assign all the elements to one global window by specifying explicitly.

{{if (eq .Sdk "go")}}
The first argument that we specify is the `scope` and from the library of `window` we choose which one we need, in this case a `NewGlobalWindows` is necessary. And the last argument is a `PCollection` that contains the elements to which window is applied.

```
globalWindowedItems := beam.WindowInto(s,
	window.NewGlobalWindows(),
	input)
```
{{end}}

{{if (eq .Sdk "java")}}
The first step is to apply the `Window` transformation. And we write what type of `PCollection` has by calling the `into` method.  The argument will be an object of the desired **window-class**.
```
PCollection<String> input = ...;
PCollection<String> batchItems = input.apply(
  Window.<String>into(new GlobalWindows()));
```
{{end}}

{{if (eq .Sdk "python")}}
The first step is to apply the `WindowInto` conversion, and from the `window` library we choose which one we need, in this case we need `GlobalWindows`.
```
from apache_beam import window

global_windowed_items = (
    input | 'window' >> beam.WindowInto(window.GlobalWindows()))
```
{{end}}

If you do not specify any windows, a single global window will be automatically applied.

### Playground exercise

`CombineFn` : This function allows you to perform operations such as counting, summing, or finding the minimum or maximum element within a global window.

`GroupByKey` : This function groups elements by a key, and allows you to apply a beam.CombineFn to each group of elements within a global window.

`Map` : This function allows you to apply a user-defined function to each element within a global window.

`Filter` : This function allows you to filter elements based on a user-defined condition, within a global window.

`FlatMap` : This function allows you to apply a user-defined function to each element within a global window and output zero or more elements.

These functions can be easily composed together to create complex data processing pipelines. Additionally, it's also possible to create your own custom functions to perform specific operations within a global window.

You can apply the functions on the playground example. You can supplement with **filter** and **count**.

{{if (eq .Sdk "go")}}

Import dependency:
```
"strings"
"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/filter"
```

Modify code:
```
filtered := applyTransform(s, input)

counted := stats.CountElms(s, filtered)
```

Filter function:
```
func applyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
    return filter.Exclude(s, input, func(element string) bool {
        return strings.HasPrefix(strings.ToLower(element), "w")
    })
}
```
{{end}}

{{if (eq .Sdk "java")}}
Modify code:
```
batchItems.apply(Filter.by(element -> element.toLowerCase().startsWith("w"))).apply(Count.globally());
```
{{end}}

{{if (eq .Sdk "python")}}

Modify code:
```
(p | beam.Create(['Hello Beam','It`s windowing'])
     | 'window' >>  beam.WindowInto(window.GlobalWindows())
     | 'filter' >> beam.Filter(lambda element: element.lower().startswith("h"))
     | 'count' >> beam.combiners.Count.Globally()
     | 'Log words' >> Output())
```
{{end}}