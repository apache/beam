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
1. First you need to divide `PCollection` by `beam.Partition(s, 3, func(word string) int{ ... }`.
2. For each PCol, make a count for each word by `stats.Count(s, input)`.
3. `upperCaseWords` and `capitalCaseWords` translate to lowercase with the help of `beam.ParDo(s, func(word string,value int) (string,int){ ... })`.
4. Merge `lowerCaseWords`, `newFirstPartPCollection`, `newSecondPartPCollection` to one PCollection with `beam.Flatten(s, aInput, bInput, cInput)`.
5. Group by key so that the key is the word and the values from the array of quantities using `beam.GroupByKey(s, input)`.