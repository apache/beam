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

1. Change `getAnalysisPCollection` so that it returns a `PCollection` with `Analysis` objects. Create your own `Coder` and install for `PCollection`.

   Write own DoFn `static class SentimentAnalysisExtractFn extends DoFn<String, Analysis> {
   @ProcessElement
   public void processElement(ProcessContext c) {
   String[] items = c.element().split(REGEX_FOR_CSV);
   if(!items[1].equals("Negative"))
   c.output(new Analysis(items[0].toLowerCase(), items[1], items[2], items[3], items[4], items[5], items[6], items[7]));
   }
   }`
2. To use the analyzed words in the `side-input`, turn to `.apply(View.asList())`
3. Add a fixed-window that runs for 30 seconds `Window.into(Fixed Windows.of(Duration.standard Seconds(30)))`. And add a trigger that works after the first element with a delay of 5 seconds `AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(5))`
4. Write your own function that works with `side-input`. Its logic should check whether the words from shakespeare are contained in the list of analyzed words 

   Function: `static PCollection<Analysis> getAnalysis(PCollection<String> pCollection, PCollectionView<List<Analysis>> viewAnalysisPCollection) {
   return pCollection.apply(ParDo.of(new DoFn<String, Analysis>() {
   @ProcessElement
   public void processElement(@Element String word, OutputReceiver<Analysis> out, ProcessContext context) {
   List<Analysis> analysisPCollection = context.sideInput(viewAnalysisPCollection);
   analysisPCollection.forEach(it -> {
   if (it.word.equals(word)) {
   out.output(it);
   }
   });
   }
   }).withSideInputs(viewAnalysisPCollection));
   }`
5. Divide the words into portions in the first **positive** words. In the **second** negative. And all the others in the third.

   Partition:`static PCollectionList<Analysis> applyTransform(PCollection<Analysis> input) {
   return input
   .apply(Partition.of(3,
   (Partition.PartitionFn<Analysis>) (analysis, numPartitions) -> {
   if (!analysis.positive.equals("0")) {
   return 0;
   }
   if (!analysis.negative.equals("0")) {
   return 1;
   }
   return 2;
   }));
   }`
6. To calculate the count with windows, use `Combine.globally` with `withoutDefaults()`. Apply the transformation `.apply(Combine.globally(Count.<Analysis>combineFn()).withoutDefaults())`