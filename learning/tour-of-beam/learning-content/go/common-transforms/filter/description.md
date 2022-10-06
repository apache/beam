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

### Description for example 

Given a list of integers , printing even numbers using ```Filter```. The ```applyTransform()``` function implements a filter in which the logic determines the numbers are even.