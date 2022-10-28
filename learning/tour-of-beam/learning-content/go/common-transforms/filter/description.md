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

### Playground exercise

You can find the complete code of the above example using 'Filter' in the playground window, which you can run and experiment with.

Filter transform can be used with both text and numerical collection. For example, let's try filtering the input collection that contains words so that only words that start with the letter 'a' are returned.

You can also chain several filter transforms to form more complex filtering based on several simple filters or implement more complex filtering logic within a single filter transform. For example, try both approaches to filter the same list of words such that only ones that start with a letter 'a' (regardless of the case) and containing more than three symbols are returned.

**Hint**

You can use the following code snippet to create an input PCollection:

Don't forget to add import:

```
import (
    "strings"
    ...
)
```

Create data for PCollection:

```
str:= "To be, or not to be: that is the question: Whether 'tis nobler in the mind to suffer The slings and arrows of outrageous fortune, Or to take arms against a sea of troubles, And by opposing end them. To die: to sleep"

input := beam.CreateList(s,strings.Split(str, " "))
```

And filtering:

```
func applyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
    return filter.Include(s, input, func(word string) bool {
		return strings.HasPrefix(strings.ToUpper(word), "A")
    })
}
```