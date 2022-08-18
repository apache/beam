# Min

Provides a variety of different transforms for computing the minimum values in a collection, either globally or for each key.

You can find the global minimum value from the ```PCollection``` by using ```Min()```

```
import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return stats.Min(s, input)
}
```

To calculate the minimum of the elements associated with each unique key, you can use ```MinPerKey()```

```
import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return stats.MinPerKey(s, input)
}
```
