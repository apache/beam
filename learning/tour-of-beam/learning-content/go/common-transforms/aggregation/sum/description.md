# Sum

You can use Sum transforms to compute the sum of the elements in a collection or the sum of the values associated with each key in a collection of key-value pairs.

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

You can use ```SumPerKey()```to calculate the sum Integer associated with each unique key.

```
import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return stats.SumPerKey(s, input)
}
```

### Description for example

Given a list of integers ```PCollection```. The ```applyTransform()``` function return sum of numbers from ```PCollection```.