# Additional outputs

While `ParDo` always outputs the main output of `PCollection` (as a return value from apply), you can also force your `ParDo` to output any number of additional `PCollection` outputs. If you decide to have multiple outputs, your `ParDo` will return all the `PCollection` output (including the main output) combined. This will be useful when you are working with big data or a database that needs to be divided into different collections. You get a combined `PCollectionTuple`, you can use `TupleTag` to get a `PCollection`.

A `PCollectionTuple` is an immutable tuple of heterogeneously typed `PCollection`, "with keys" `TupleTags`. A `PCollectionTuple` can be used as input or output for `PTransform` receiving or creating multiple `PCollection` inputs or outputs, which can be of different types, for example, `ParDo` with multiple outputs.

A `TupleTag` is a typed tag used as the key of a heterogeneously typed tuple, for example `PCollectionTuple`. Its general type parameter allows you to track the static type of things stored in tuples.

### Tags for multiple outputs

```
  .of(new DoFn<String, String>() {
     public void processElement(@Element String word, MultiOutputReceiver out) {
       if (word.length() <= wordLengthCutOff) {
         // Emit short word to the main output.
         // In this example, it is the output with tag wordsBelowCutOffTag.
         out.get(wordsBelowCutOffTag).output(word);
       } else {
         // Emit long word length to the output with tag wordLengthsAboveCutOffTag.
         out.get(wordLengthsAboveCutOffTag).output(word.length());
       }
       if (word.startsWith("MARKER")) {
         // Emit word to the output with tag markedWordsTag.
         out.get(markedWordsTag).output(word);
       }
     }})).withOutputTags(wordsBelowCutOffTag,
          // Specify the tags for the two additional outputs as a TupleTagList.
                          TupleTagList.of(wordLengthsAboveCutOffTag).and(markedWordsTag)));
```

### Description for example

There are integers at the input. `applyTransform()` accepts a list of integers in the output additionally in addition to one `PCollection`, returns a second `PCollection`.