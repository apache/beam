# Min

Min transforms find the minimum values globally or for each key in the input collection.

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

You can use ```MinPerKey()``` to calculate the minimum Integer associated with each unique key (which is of type String).

```
import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return stats.MinPerKey(s, input)
}
```

### Description for example 

Given a list of integers ```PCollection```. The ```applyTransform()``` function return minimum number from ```PCollection```.