### Count

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

To count how many elements are associated with a particular key, you can use ```Count()``` , the result will be one output for each key

```
import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return stats.Count(s, input)
}
```