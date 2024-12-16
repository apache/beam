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

# Combine

`Combine` is a Beam transform for combining collections of elements or values in your data. Combine has variants that work on entire `PCollections`, and some that combine the values for each key in `PCollections` of **key/value** pairs.

When you apply a `Combine` transform, you must provide the function that contains the logic for combining the elements or values. The combining function should be commutative and associative, as the function is not necessarily invoked exactly once on all values with a given key. Because the input data (including the value collection) may be distributed across multiple workers, the combining function might be called multiple times to perform partial combining on subsets of the value collection. The Beam SDK also provides some pre-built combine functions for common numeric combination operations such as sum, min, and max.

Simple combine operations, such as sums, can usually be implemented as a simple function.

{{if (eq .Sdk "go")}}
```
func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.Combine(s, func(sum, elem int) int {
		return sum + elem
	}, input)
}
```
{{end}}
{{if (eq .Sdk "java")}}
```
// Sum a collection of Integer values. The function SumInts implements the interface SerializableFunction.
public static class SumInts implements SerializableFunction<Iterable<Integer>, Integer> {
  @Override
  public Integer apply(Iterable<Integer> input) {
    int sum = 0;
    for (int item : input) {
      sum += item;
    }
    return sum;
  }
}
```
{{end}}
{{if (eq .Sdk "python")}}
```
input = [1, 10, 100, 1000]

def bounded_sum(values, bound=500):
  return min(sum(values), bound)

small_sum = input | beam.CombineGlobally(bounded_sum)  # [500]
large_sum = input | beam.CombineGlobally(bounded_sum, bound=5000)  # [1111]
```
{{end}}
### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

The input data consists of integers. `Combine` combines numbers.

It can also be combined with other types, for example: `strings` and others.
{{if (eq .Sdk "go")}}
```
input := beam.Create(s, "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog")

func applyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.Combine(s, func(sum, elem string) string {
		return sum+","+elem
	}, input)
}
```
{{end}}
{{if (eq .Sdk "java")}}
```
public static class ConcatenateStrings implements SerializableFunction<Iterable<String>, String> {
  @Override
  public String apply(Iterable<String> input) {
    StringBuilder concatenated = new StringBuilder();
    for (String item : input) {
      concatenated.append(",").append(item);
    }
    return concatenated.toString();
  }
}

PCollection<String> input = pipeline.apply(Create.of("quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"));
PCollection<String> concatenated = input.apply(Combine.globally(new ConcatenateStrings()));
```
{{end}}
{{if (eq .Sdk "python")}}
```
def concat(strings):
    total = ""

    for word in strings:
        total += word

    return total

with beam.Pipeline() as p:
  (p | beam.Create(["quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"])
     | beam.CombineGlobally(concat)
     | LogElements())
```
{{end}}