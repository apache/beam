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

# Filter

### Using Filter

PCollection datasets can be filtered using the Filter transform. You can create a filter by supplying a predicate and, when applied, filtering out all the elements of PCollection that don’t satisfy the predicate.

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

You can find the full code of this example in the playground window, which you can run and experiment with.

`filter.Exclude` excludes a number if it is divisible by 2 with a remainder. If you make an `filte.Include` filter, it will work the other way around, it will filter elements that satisfy the filter logic

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.