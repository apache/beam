### Using Filter


You can filter the dataset by criteria. It can also be used for equality based.Filter accepts a function that keeps elements that return True, and filters out the remaining elements.

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