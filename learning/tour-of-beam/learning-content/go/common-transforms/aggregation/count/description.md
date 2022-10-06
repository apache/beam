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