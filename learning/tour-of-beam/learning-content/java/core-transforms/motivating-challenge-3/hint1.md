You can solve the challenge with this way:
1. Complete the compositions function first of all filters all words that begin with "**s**" `ParDo.of(new WordsStartWith("s")))` in `WordsStartWith` class `if (c.element().toLowerCase().startsWith(letter)) {
   c.output(c.element());
   }`. Then, you can group the words so that they are unique. After that, we return only the key `apply(Count.perElement()).apply(MapElements.into(TypeDescriptors.strings()).via(KV::getKey)`.
2. To return two `PCollection` we need to use `ParDo2` with nested logic in this case checking for the case of letters:
```
wordsWithStartS.apply(ParDo.of(new DoFn<String, String>() {
      @ProcessElement
      public void processElement(@Element String word, MultiOutputReceiver out) {
         if (word.startsWith("S")) {
               out.get(wordWithUpperCase).output(word);
         } else {
               out.get(wordWithLowerCase).output(word);
            }
         }
   }).withOutputTags(wordWithUpperCase, TupleTagList.of(wordWithLowerCase)));
```

3. To check whether words with **lowercase** letters are contained in words with **uppercase** letters, you need to create a `view` using `side-input`.
```
input.apply(View.asList())
```

4. After that, you need to check for the presence of:
```
ParDo.of(new DoFn<String, String>() {
      @ProcessElement
      public void processElement(@Element String word, OutputReceiver<String> out, ProcessContext context) {
         List<String> upperCaseWords = context.sideInput(lowerCaseWordsView);
            if (upperCaseWords.contains(word.toLowerCase())) {
                  out.output(word);
            }
         }
}).withSideInputs(lowerCaseWordsView));
```