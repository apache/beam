# Additional outputs

While `ParDo` always produces a main output `PCollection` (as the return value from apply), you can also have your ParDo produce any number of additional output `PCollection`s. If you choose to have multiple outputs, your `ParDo` returns all the output `PCollections` (including the main output) bundled together.

### Tags for multiple outputs

```
// To emit elements to multiple output PCollections, create a TupleTag object to identify each collection
// that your ParDo produces. For example, if your ParDo produces three output PCollections (the main output
// and two additional outputs), you must create three TupleTags. The following example code shows how to
// create TupleTags for a ParDo with three output PCollections.

  // Input PCollection to our ParDo.
  PCollection<String> words = ...;

  // The ParDo will filter words whose length is below a cutoff and add them to
  // the main output PCollection<String>.
  // If a word is above the cutoff, the ParDo will add the word length to an
  // output PCollection<Integer>.
  // If a word starts with the string "MARKER", the ParDo will add that word to an
  // output PCollection<String>.
  final int wordLengthCutOff = 10;

  // Create three TupleTags, one for each output PCollection.
  // Output that contains words below the length cutoff.
  final TupleTag<String> wordsBelowCutOffTag =
      new TupleTag<String>(){};
  // Output that contains word lengths.
  final TupleTag<Integer> wordLengthsAboveCutOffTag =
      new TupleTag<Integer>(){};
  // Output that contains "MARKER" words.
  final TupleTag<String> markedWordsTag =
      new TupleTag<String>(){};

// Passing Output Tags to ParDo:
// After you specify the TupleTags for each of your ParDo outputs, pass the tags to your ParDo by invoking
// .withOutputTags. You pass the tag for the main output first, and then the tags for any additional outputs
// in a TupleTagList. Building on our previous example, we pass the three TupleTags for our three output
// PCollections to our ParDo. Note that all of the outputs (including the main output PCollection) are
// bundled into the returned PCollectionTuple.

  PCollectionTuple results =
      words.apply(ParDo
          .of(new DoFn<String, String>() {
            // DoFn continues here.
            ...
          })
          // Specify the tag for the main output.
          .withOutputTags(wordsBelowCutOffTag,
          // Specify the tags for the two additional outputs as a TupleTagList.
                          TupleTagList.of(wordLengthsAboveCutOffTag)
                                      .and(markedWordsTag)));
```

### Emitting to multiple outputs in your DoFn

```
// Inside your ParDo's DoFn, you can emit an element to a specific output PCollection by providing a
// MultiOutputReceiver to your process method, and passing in the appropriate TupleTag to obtain an OutputReceiver.
// After your ParDo, extract the resulting output PCollections from the returned PCollectionTuple.
// Based on the previous example, this shows the DoFn emitting to the main output and two additional outputs.

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
     }}));
```

### Accessing additional parameters in your DoFn

In addition to the element and the OutputReceiver, Beam will populate other parameters to your DoFn’s @ProcessElement method. Any combination of these parameters can be added to your process method in any order.

**Timestamp**: To access the timestamp of an input element, add a parameter annotated with @Timestamp of type Instant. For example:

```
.of(new DoFn<String, String>() {
     public void processElement(@Element String word, @Timestamp Instant timestamp) {
  }})
```

**Window**: To access the window an input element falls into, add a parameter of the type of the window used for the input `PCollection`. If the parameter is a window type (a subclass of BoundedWindow) that does not match the input `PCollection`, then an error will be raised. If an element falls in multiple windows (for example, this will happen when using `SlidingWindows`), then the `@ProcessElement` method will be invoked multiple time for the element, once for each window. For example, when fixed windows are being used, the window is of type `IntervalWindow`.

```
.of(new DoFn<String, String>() {
     public void processElement(@Element String word, IntervalWindow window) {
  }})
```

**PaneInfo**: When triggers are used, Beam provides a `PaneInfo` object that contains information about the current firing. Using `PaneInfo` you can determine whether this is an early or a late firing, and how many times this window has already fired for this key.

```
.of(new DoFn<String, String>() {
     public void processElement(@Element String word, PaneInfo paneInfo) {
  }})
```

**PipelineOptions**: The `PipelineOptions` for the current pipeline can always be accessed in a process method by adding it as a parameter:

```
.of(new DoFn<String, String>() {
     public void processElement(@Element String word, PipelineOptions options) {
  }})
```

`@OnTimer` methods can also access many of these parameters. Timestamp, Window, key, `PipelineOptions`, `OutputReceiver`, and `MultiOutputReceiver` parameters can all be accessed in an @OnTimer method. In addition, an `@OnTimer` method can take a parameter of type `TimeDomain` which tells whether the timer is based on event time or processing time. Timers are explained in more detail in the Timely (and Stateful) Processing with Apache Beam blog post.

