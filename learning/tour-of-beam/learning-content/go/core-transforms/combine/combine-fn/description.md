<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# CombineFn

More complex combination operations might require you to create a subclass of `CombineFn` that has an accumulation type distinct from the **input/output** type.

> **Important**. In the Go SDK, DirectRunner does not parallelize data. Therefore, MergeAccumulators is not called.

The associativity and commutativity of a CombineFn allows runners to automatically apply some optimizations:

**Combiner lifting**: This is the most significant optimization. Input elements are combined per key and window before they are shuffled, so the volume of data shuffled might be reduced by many orders of magnitude. Another term for this optimization is “mapper-side combine.”

**Incremental combining**: When you have a `CombineFn` that reduces the data size by a lot, it is useful to combine elements as they emerge from a streaming shuffle. This spreads out the cost of doing combines over the time that your streaming computation might be idle. Incremental combining also reduces the storage of intermediate accumulators.

A general combining operation consists of four operations. When you create a subclass of `CombineFn`, you must provide four operations by overriding the corresponding methods:

1. Create Accumulator creates a new “local” accumulator. In the example case, taking a mean average, a local accumulator tracks the running sum of values (the numerator value for our final average division) and the number of values summed so far (the denominator value). It may be called any number of times in a distributed fashion.
2. Add Input adds an input element to an accumulator, returning the accumulator value. In our example, it would update the sum and increment the count. It may also be invoked in parallel.
3. Merge Accumulators merges several accumulators into a single accumulator; this is how data in multiple accumulators is combined before the final calculation. In the case of the mean average computation, the accumulators representing each portion of the division are merged together. It may be called again on its outputs any number of times.
4. Extract Output performs the final computation. In the case of computing a mean average, this means dividing the combined sum of all the values by the number of values summed. It is called once on the final, merged accumulator.

```
type averageFn struct{}

type averageAccum struct {
	Count, Sum int
}

func (fn *averageFn) CreateAccumulator() averageAccum {
	return averageAccum{0, 0}
}

func (fn *averageFn) AddInput(a averageAccum, v int) averageAccum {
	return averageAccum{Count: a.Count + 1, Sum: a.Sum + v}
}

func (fn *averageFn) MergeAccumulators(a, v averageAccum) averageAccum {
	return averageAccum{Count: a.Count + v.Count, Sum: a.Sum + v.Sum}
}

func (fn *averageFn) ExtractOutput(a averageAccum) float64 {
	if a.Count == 0 {
		return math.NaN()
	}
	return float64(a.Sum) / float64(a.Count)
}

func init() {
	register.Combiner3[averageAccum, int, float64](&averageFn{})
}
```

### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.



Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.