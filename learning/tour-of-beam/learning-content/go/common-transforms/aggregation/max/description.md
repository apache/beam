# Max

Provides a variety of different transforms for computing the maximum values in a collection, either globally or for each key.

You can find the global maximum value from the ```PCollection``` by using ```Max()```

```
import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return stats.Max(s, input)
}
```

To calculate the maximum of the elements associated with each unique key, you can use ```MaxPerKey()```

```
import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return stats.MaxPerKey(s, input)
}
```
