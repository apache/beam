---
title: "ParDo"
---
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
# ParDo
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/ParDo.html">
      <img src="https://beam.apache.org/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

A transform for generic parallel processing. A `ParDo` transform considers each
element in the input `PCollection`, performs some processing function
(your user code) on that element, and emits zero or more elements to
an output PCollection.

See more information in the [Beam Programming Guide](/documentation/programming-guide/#pardo).

## Examples
**Example 1**: Passing side inputs

{{< highlight java >}}
  // Pass side inputs to your ParDo transform by invoking .withSideInputs.
  // Inside your DoFn, access the side input by using the method DoFn.ProcessContext.sideInput.

  // The input PCollection to ParDo.
  PCollection<String> words = ...;

  // A PCollection of word lengths that we'll combine into a single value.
  PCollection<Integer> wordLengths = ...; // Singleton PCollection

  // Create a singleton PCollectionView from wordLengths using Combine.globally and View.asSingleton.
  final PCollectionView<Integer> maxWordLengthCutOffView =
     wordLengths.apply(Combine.globally(new Max.MaxIntFn()).asSingletonView());


  // Apply a ParDo that takes maxWordLengthCutOffView as a side input.
  PCollection<String> wordsBelowCutOff =
  words.apply(ParDo
      .of(new DoFn<String, String>() {
          public void processElement(ProcessContext c) {
            String word = c.element();
            // In our DoFn, access the side input.
            int lengthCutOff = c.sideInput(maxWordLengthCutOffView);
            if (word.length() <= lengthCutOff) {
              c.output(word);
            }
          }
      }).withSideInputs(maxWordLengthCutOffView)
  );
{{< /highlight >}}

**Example 2**: Emitting to multiple outputs in your `DoFn`

{{< highlight java >}}
// To emit elements to multiple output PCollections, create a TupleTag object to identify each collection
// that your ParDo produces. For example, if your ParDo produces three output PCollections (the main output
// and two additional outputs), you must create three TupleTags. The following example code shows how to
// create TupleTags for a ParDo with three output PCollections.

  // Input PCollection to our ParDo.
  PCollection<String> words = ...;

  // The ParDo will filter words whose length is below a cutoff and add them to
  // the main ouput PCollection<String>.
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
{{< /highlight >}}

**Example 3**: Tags for multiple outputs

{{< highlight java >}}
// Inside your ParDo's DoFn, you can emit an element to a specific output PCollection by passing in the
// appropriate TupleTag when you call ProcessContext.output.
// After your ParDo, extract the resulting output PCollections from the returned PCollectionTuple.
// Based on the previous example, this shows the DoFn emitting to the main output and two additional outputs.

  .of(new DoFn<String, String>() {
     public void processElement(ProcessContext c) {
       String word = c.element();
       if (word.length() <= wordLengthCutOff) {
         // Emit short word to the main output.
         // In this example, it is the output with tag wordsBelowCutOffTag.
         c.output(word);
       } else {
         // Emit long word length to the output with tag wordLengthsAboveCutOffTag.
         c.output(wordLengthsAboveCutOffTag, word.length());
       }
       if (word.startsWith("MARKER")) {
         // Emit word to the output with tag markedWordsTag.
         c.output(markedWordsTag, word);
       }
     }}));
{{< /highlight >}}


## Related transforms 
* [MapElements](/documentation/transforms/java/elementwise/mapelements)
  applies a simple 1-to-1 mapping function over each element in the collection.
* [Filter](/documentation/transforms/java/elementwise/filter)
  is useful if the function is just deciding whether to output an element or not.