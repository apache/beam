You can solve the challenge with this way:
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