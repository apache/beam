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

# CombinePerKey

`CombinePerKey` is a transform in Apache Beam that applies a `CombineFn` function to each key of a `PCollection` of key-value pairs. The `CombineFn` function can be used to aggregate, sum, or combine the values associated with each key in the input `PCollection`.

The `CombinePerKey` transform takes in an instance of a `CombineFn` class and applies it to the input `PCollection`. The output of the transform is a new `PCollection` where each element is a key-value pair, where the key is the same as the input key, and the value is the result of applying the `CombineFn` function to all the values associated with that key in the input `PCollection`.

{{if (eq .Sdk "go")}}
```
input := beam.Create(s, []beam.T{
		beam.KV{"a", 1},
		beam.KV{"b", 2},
		beam.KV{"a", 3},
		beam.KV{"c", 4},
		beam.KV{"b", 5},
	})

	output := beam.CombinePerKey(s, input, combine.SumInts())
```
{{end}}
{{if (eq .Sdk "java")}}
```
PCollection<KV<String, Integer>> input = pipeline.apply(Create.of(
                KV.of("a", 1),
                KV.of("b", 2),
                KV.of("a", 3),
                KV.of("c", 4),
                KV.of("b", 5)
)).setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));

PCollection<KV<String, Integer>> output = input.apply(Combine.perKey(new SumIntegers()));
```
{{end}}
{{if (eq .Sdk "python")}}
```
with beam.Pipeline() as p:
  input_data = [('a', 1), ('b', 2), ('a', 3), ('c', 4), ('b', 5)]
  input_pcoll = p | beam.Create(input_data)
  output_pcoll = input_pcoll | CombinePerKey(SumInts())
```
{{end}}
### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

You can work with any types. For example: the key is the first letter of the word, the meaning of the word.

{{if (eq .Sdk "go")}}
```
input := beam.ParDo(s, func(_ []byte, emit func(string, string)){
		emit("a", "apple")
		emit("o", "orange")
		emit("a", "avocado")
		emit("l", "lemon")
		emit("l", "limes")
	}, beam.Impulse(s))
```

You combine by key by inserting a comma between them:
```
func applyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.CombinePerKey(s, func(name1, name2 string) string {
        return name1+","+name2
	}, input)
}
```
{{end}}
{{if (eq .Sdk "java")}}
```
PCollection<KV<String, Integer>> input = pipeline
                .apply(Create.of(
                        KV.of("a", "apple"),
                        KV.of("o", "orange"),
                        KV.of("a", "avocado),
                        KV.of("l", "lemon"),
                        KV.of("l", "limes")
                ));

static PCollection<KV<String, String>> applyTransform(PCollection<KV<String, String>> input) {
        return input.apply(Combine.perKey(new SumStringBinaryCombineFn()));
    }

    static class SumStringBinaryCombineFn extends BinaryCombineFn<String> {

        @Override
        public String apply(String left, String right) {
            return left +","+ right;
        }

    }
```
{{end}}
{{if (eq .Sdk "python")}}
```
class ConcatString(combiners.CombineFn):
    def create_accumulator(self):
        return ""

    def add_input(self, accumulator, input):
        return accumulator + "," + input

    def merge_accumulators(self, accumulators):
        return ''.join(accumulators)

    def extract_output(self, accumulator):
        return accumulator

input = (p | 'Create Cities To Time KV' >> beam.Create([
                 ('a', 'apple'),
                 ('o', 'orange'),
                 ('a', 'avocado'),
                 ('l', 'lemon'),
                 ('l', 'limes')])
)

output = input | 'Combine Per Key' >> CombinePerKey(ConcatString())
```
{{end}}