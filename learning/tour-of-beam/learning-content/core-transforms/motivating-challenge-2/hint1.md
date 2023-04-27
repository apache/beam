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
You can solve the challenge with this way:
{{if (eq .Sdk "go")}}
1. First you need to divide `PCollection` by `beam.ParDo(s, func(line string) (string,int){ .. .}`.
2. Combine by key the values should be combined `(score1 + score2)` `beam.CombinePerKey(s, func(score1, score2 int) int{ ... }`
{{end}}

{{if (eq .Sdk "java")}}
1. First you need to divide `PCollection` by `FlatMapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
   .via((SerializableFunction<String, Iterable<KV<String, Integer>>>) line -> { ... }`.
2. Combine by key the values should be combined `(score1 + score2)`, create class which extend Combine `static class SumWordLetterCombineFn extends Combine.BinaryCombineFn<Integer>{}`

{{end}}
{{if (eq .Sdk "python")}}
1. First you need to divide `PCollection` by `beam.FlatMap(lambda line : [line.split(':')])`.
2. It is necessary to divide by indexes the key will be the word the value of its counting, and it is necessary to remove the extra spaces `beam.Map(lambda arr : (arr[0],int(arr[1].strip())))`.
3. Combine by key the values should be combined `(score1 + score2)`, create class which extend Combine `beam.CombinePerKey(sum)`

{{end}}