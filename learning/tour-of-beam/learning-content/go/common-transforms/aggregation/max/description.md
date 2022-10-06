# Max

Max provides a variety of different transforms for computing the maximum values in a collection, either globally or for each key.

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

You can use ```MaxPerKey()``` to calculate the maximum Integer associated with each unique key (which is of type String).

```
import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return stats.MaxPerKey(s, input)
}
```

### Description for example

Given a list of integers ```PCollection```. The ```applyTransform()``` function return maximum number from ```PCollection```.