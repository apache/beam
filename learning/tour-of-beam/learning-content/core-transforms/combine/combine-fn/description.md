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
{{if (eq .Sdk "go")}}

> **Important**. In the Go SDK, DirectRunner does not parallelize data. Therefore, MergeAccumulators is not called.

{{end}}

The associativity and commutativity of a CombineFn allows runners to automatically apply some optimizations:

**Combiner lifting**: This is the most significant optimization. Input elements are combined per key and window before they are shuffled, so the volume of data shuffled might be reduced by many orders of magnitude. Another term for this optimization is “mapper-side combine.”

**Incremental combining**: When you have a `CombineFn` that reduces the data size by a lot, it is useful to combine elements as they emerge from a streaming shuffle. This spreads out the cost of doing combines over the time that your streaming computation might be idle. Incremental combining also reduces the storage of intermediate accumulators.

A general combining operation consists of four operations. When you create a subclass of `CombineFn`, you must provide four operations by overriding the corresponding methods:

1. Create Accumulator creates a new “local” accumulator. In the example case, taking a mean average, a local accumulator tracks the running sum of values (the numerator value for our final average division) and the number of values summed so far (the denominator value). It may be called any number of times in a distributed fashion.
2. Add Input adds an input element to an accumulator, returning the accumulator value. In our example, it would update the sum and increment the count. It may also be invoked in parallel.
3. Merge Accumulators merges several accumulators into a single accumulator; this is how data in multiple accumulators is combined before the final calculation. In the case of the mean average computation, the accumulators representing each portion of the division are merged together. It may be called again on its outputs any number of times.
4. Extract Output performs the final computation. In the case of computing a mean average, this means dividing the combined sum of all the values by the number of values summed. It is called once on the final, merged accumulator.

{{if (eq .Sdk "go")}}
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
{{end}}
{{if (eq .Sdk "java")}}
```
public class AverageFn extends CombineFn<Integer, AverageFn.Accum, Double> {
  public static class Accum {
    int sum = 0;
    int count = 0;
  }

  @Override
  public Accum createAccumulator() { return new Accum(); }

  @Override
  public Accum addInput(Accum accum, Integer input) {
      accum.sum += input;
      accum.count++;
      return accum;
  }

  @Override
  public Accum mergeAccumulators(Iterable<Accum> accums) {
    Accum merged = createAccumulator();
    for (Accum accum : accums) {
      merged.sum += accum.sum;
      merged.count += accum.count;
    }
    return merged;
  }

  @Override
  public Double extractOutput(Accum accum) {
    return ((double) accum.sum) / accum.count;
  }
}
```
{{end}}
{{if (eq .Sdk "python")}}
```
input = ...

class AverageFn(beam.CombineFn):
  def create_accumulator(self):
    return (0.0, 0)

  def add_input(self, sum_count, input):
    (sum, count) = sum_count
    return sum + input, count + 1

  def merge_accumulators(self, accumulators):
    sums, counts = zip(*accumulators)
    return sum(sums), sum(counts)

  def extract_output(self, sum_count):
    (sum, count) = sum_count
    return sum / count if count else float('NaN')
```
{{end}}
### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

Collect words of the same length:
{{if (eq .Sdk "go")}}
```
result := beam.CombinePerKey(s, funcx.CombineFn(
		func(key typex.Key, values func() funcx.Values) funcx.Values {
			var result []wordLen
			for v := range values() {
				result = append(result, v.(wordLen))
			}
			return result
		},
		func() funcx.Accumulator {
			return func() funcx.Accumulator {
				return func() funcx.Accumulator {
					var result []wordLen
					return func(value interface{}) {
						val := value.(wordLen)
						result = append(result, val)
					}
				}
			}
		},
		func(acc funcx.Accumulator) funcx.Accumulator {
			return acc
		},
		func(acc funcx.Accumulator) interface{} {
			return acc()
		}),
		beam.ParDo(s, func(word string) wordLen {
			return wordLen{len: len(word), word: word}
		}, beam.Create(s, input)))
```
{{end}}

{{if (eq .Sdk "java")}}
```
input
                .apply(ParDo.of(new DoFn<String, KV<Integer, String>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        c.output(KV.of(c.element().length(), c.element()));
                    }
                }))
                .apply(Combine.perKey(new Combine.CombineFn<Integer, String, List<String>>() {
                    @Override
                    public List<String> createAccumulator() {
                        return new ArrayList<>();
                    }
                    @Override
                    public List<String> addInput(List<String> accumulator, String input) {
                        accumulator.add(input);
                        return accumulator;
                    }
                    @Override
                    public List<String> mergeAccumulators(Iterable<List<String>> accumulators) {
                        List<String> merged = new ArrayList<>();
                        for (List<String> acc : accumulators) {
                            merged.addAll(acc);
                            }
                        return merged;
                        }
                    @Override
                    public List<String> extractOutput(List<String> accumulator) {
                        return accumulator;
                    }
                }))
                .setCoder(KvCoder.of(VarIntCoder.of(), ListCoder.of(StringUtf8Coder.of())));
```
{{end}}

{{if (eq .Sdk "python")}}
```
class CollectWordsFn(beam.CombineFn):
    def create_accumulator(self):
        return {}

    def add_input(self, accumulator, element):
        length = len(element)
        if length not in accumulator:
            accumulator[length] = []
        accumulator[length].append(element)
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = {}
        for acc in accumulators:
            for length in acc:
                if length not in merged:
                    merged[length] = []
                merged[length].extend(acc[length])
        return merged

    def extract_output(self, accumulator):
        return accumulator
```
{{end}}