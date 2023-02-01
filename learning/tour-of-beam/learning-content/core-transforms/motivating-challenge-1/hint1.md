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
1. First you need to divide `PCollection` by `beam.Partition(s, 3, func(word string) int{ ... }`.
2. For each PCol, make a count for each word by `stats.Count(s, input)`.
3. `upperCaseWords` and `capitalCaseWords` translate to lowercase with the help of `beam.ParDo(s, func(word string,value int) (string,int){ ... })`.
4. Merge `lowerCaseWords`, `newFirstPartPCollection`, `newSecondPartPCollection` to one PCollection with `beam.Flatten(s, aInput, bInput, cInput)`.
5. Group by key so that the key is the word and the values from the array of quantities using `beam.GroupByKey(s, input)`.
{{end}}

{{if (eq .Sdk "java")}}
1. First you need to divide `PCollection` by `Partition.of(3,
   (word, numPartitions) -> {
   if (word.equals(word.toUpperCase())) {
   return 0;
   } else if (Character.isUpperCase(word.charAt(0))) {
   return 1;
   } else {
   return 2;
   }
   })`.
2. For each PCol, make a count for each word by `Count.perElement()`.
3. `allLetterUpperCase` and `firstLetterUpperCase` translate to lowercase with the help of `MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs())).via(word -> KV.of(word.getKey().toLowerCase(), word.getValue()))`.
4. Merge `newFirstPartPCollection`, `newSecondPartPCollection`, `allLetterLowerCase` to one PCollection with `PCollectionList.of(input1).and(input2).and(input3).apply(Flatten.pCollections())`.
5. Group by key so that the key is the word and the values from the array of quantities using `GroupByKey.create()`.
{{end}}


{{if (eq .Sdk "python")}}
1. First you need to divide `PCollection` by
```
def partition_fn(word, num_partitions):
   if word.upper()==word:
   return 0
   if word[0].isupper():
   return 1
   else:
   return 2
```
2. For each PCollection, make a count for each word by `parts[0] | 'All upper' >> beam.combiners.Count.PerElement() | beam.Map(lambda key: (key[0].lower(),key[1]))`.
3. Merge `allLetterUpperCase`, `firstLetterUpperCase`, `allLetterLowerCase` to one PCollection with `(allLetterUpperCase, firstLetterUpperCase, allLetterLowerCase) | beam.Flatten()`.
4. Group by key so that the key is the word and the values from the array of quantities using `beam.GroupByKey()`.
{{end}}