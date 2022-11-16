You can solve the challenge with this way:
1. Each input is a separate word. We need to make map a key that will be the first letter and the meaning of the word itself. So we have to save to a slice.
```
mutableAccumulator.current.add(Collections.singletonMap(input.substring(0, 1), input));
```

2. In `mergeAccumulators`, check whether there is a result with a letter if there is complement values, if not equate:
```
WordAccum resultWordAccum = new WordAccum();
Map<String, List<String>> map = new HashMap<>();
    for (WordAccum item : accumulators) {
        item.current.forEach(letterWordsMap -> letterWordsMap.forEach((letter, word) -> {
            List<String> listWithWords = map.getOrDefault(letter, new ArrayList<>());
            listWithWords.add(word);
            map.put(letter, listWithWords);
            }));
        }
resultWordAccum.result = map;
return resultWordAccum;
```