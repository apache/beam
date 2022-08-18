# Mean

Transforms for computing the arithmetic mean of the elements in a collection, or the mean of the values associated with each key in a collection of key-value pairs.

```Mean()``` returns a transformation that returns a collection whose content is the average of the elements of the input collection. If there are no elements in the input collection, 0 is returned.

```
import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return stats.Mean(s, input)
}
```

To calculate the mean of the elements associated with each unique key, you can use ```MeanPerKey()```

```
import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return stats.MeanPerKey(s, input)
}
```
