You can solve the challenge with this way:
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