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

Counts the number of elements within each aggregation. The Count transform has two varieties:

You can count the number of elements in ```PCollection``` with ```CountElms()```, it will return one element.

```
import (
    "github.com/apache/beam/sdks/go/pkg/beam"
    "github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
    return stats.CountElms(s, input)
}
```

You can use ```Count()``` to count how many elements are associated with a particular key, the result will be one output for each key.

```
import (
    "github.com/apache/beam/sdks/go/pkg/beam"
    "github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
    return stats.Count(s, input)
}
```

You can find the full code of this example in the playground window, which you can run and experiment with.

`CountElms` returns the number of integers from the `PCollection`. If you replace `CountElms` but `Count`, you can count the elements by the values of how many times they met.

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.