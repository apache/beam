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

### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

The `applyTransform()` accepts a list of integers at the output two `PCollection` one `PCollection` above 100 and second below 100.

You can also work with strings:

```
PCollection<String> input = pipeline.apply(Create.of("Apache Beam is an open source unified programming model","To define and execute data processing pipelines","Go SDK"));

PCollection<String> words = input
                .apply(FlatMapElements.into(TypeDescriptors.strings()).via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))));

PCollectionTuple outputTuple = applyTransform(words, upperCaseTag, lowerCaseTag);
```

Change `applyTransform`:
```
static PCollectionTuple applyTransform(
            PCollection<String> input, TupleTag<String> upperCase,
            TupleTag<String> lowerCase) {

            return input.apply(ParDo.of(new DoFn<String, String>() {

            @ProcessElement
            public void processElement(@Element String element, MultiOutputReceiver out) {
                if (element.equals(element.toLowerCase())) {
                    // First PCollection
                    out.get(lowerCase).output(element);
                } else {
                    // Additional PCollection
                    out.get(upperCase).output(element);
                }
            }

    }).withOutputTags(lowerCase, TupleTagList.of(upperCase)));
}
```

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.