You can solve the challenge with this way:
1. First you need to divide `PCollection` by `beam.Partition(s, 3, func(word string) int{ ... }`.
2. For each PCol, make a count for each word by `stats.Count(s, input)`.
3. `upperCaseWords` and `capitalCaseWords` translate to lowercase with the help of `beam.ParDo(s, func(word string,value int) (string,int){ ... })`.
4. Merge `lowerCaseWords`, `newFirstPartPCollection`, `newSecondPartPCollection` to one PCollection with `beam.Flatten(s, aInput, bInput, cInput)`.
5. Group by key so that the key is the word and the values from the array of quantities using `beam.GroupByKey(s, input)`.