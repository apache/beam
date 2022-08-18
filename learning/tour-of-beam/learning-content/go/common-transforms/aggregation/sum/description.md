# Sum

Transforms for computing the sum of the elements in a collection, or the sum of the values associated with each key in a collection of key-value pairs.


You can use ```Sum()``` to sum the elements of a ```PCollection```.

```
import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return stats.Sum(s, input)
}
```

To calculate the sum of the elements associated with each unique key, you can use ```SumPerKey()```.

```
import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return stats.SumPerKey(s, input)
}
```
