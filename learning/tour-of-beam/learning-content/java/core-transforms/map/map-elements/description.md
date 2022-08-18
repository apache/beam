# Map elements

### Lightweight DoFns and other abstractions

If your function is relatively straightforward, you can simplify your use of ```ParDo``` by providing a lightweight ```DoFn``` in-line, as an anonymous inner class instance .

Here’s the previous example, ```ParDo``` with ComputeLengthWordsFn, with the ```DoFn``` specified as an anonymous inner class instance :

```
PCollection<String> words = ...;

// Apply a ParDo with an anonymous DoFn to the PCollection words.
// Save the result as the PCollection wordLengths.
PCollection<Integer> wordLengths = words.apply(
  "ComputeWordLengths",                     // the transform name
  ParDo.of(new DoFn<String, Integer>() {    // a DoFn as an anonymous inner class instance
      @ProcessElement
      public void processElement(@Element String word, OutputReceiver<Integer> out) {
        out.output(word.length());
      }
    }));
```

If your ```ParDo``` performs a one-to-one mapping of input elements to output elements–that is, for each input element, it applies a function that produces exactly one output element, you can use the higher-level ```MapElements``` transform.MapElements can accept an anonymous Java 8 lambda function for additional brevity.

Here’s the previous example using ```MapElements``` :

```
// The input PCollection.
PCollection<String> words = ...;

// Apply a MapElements with an anonymous lambda function to the PCollection words.
// Save the result as the PCollection wordLengths.
PCollection<Integer> wordLengths = words.apply(
  MapElements.into(TypeDescriptors.integers())
             .via((String word) -> word.length()));
```