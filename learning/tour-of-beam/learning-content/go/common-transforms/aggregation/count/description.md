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

Count to get the total number of elements in different ways.

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

### Description for example

Given a list of integers `PCollection`. The `applyTransform()` function return count of numbers from `PCollection`.