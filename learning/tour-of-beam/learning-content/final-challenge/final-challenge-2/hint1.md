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
{{if (eq .Sdk "go")}}
1. Process the file with the analyzed words to return a `PCollection` with `Analysis` objects.

   Write own DoFn `func parseAnalysis(s beam.Scope, input beam.PCollection) beam.PCollection {
   return beam.ParDo(s, func(line string, emit func(analysis Analysis)) {
   parts := strings.Split(line, ",")
   if parts[0] != "Word" {
   emit(Analysis{
   Word:         strings.ToLower(parts[0]),
   Negative:     parts[1],
   Positive:     parts[2],
   Uncertainty:  parts[3],
   Litigious:    parts[4],
   Strong:       parts[5],
   Weak:         parts[6],
   Constraining: parts[7],
   })
   }
   }, input)
   }`
2. Add a fixed-window that runs for 30 seconds. And add a trigger that works after the first element with a delay of 5 seconds.

   Window and trigger: `trigger := trigger.AfterEndOfWindow().EarlyFiring(trigger.AfterProcessingTime().
   PlusDelay(5 * time.Second)).
   LateFiring(trigger.Repeat(trigger.AfterCount(1)))
   fixedWindowedItems := beam.WindowInto(s, window.NewFixedWindows(30*time.Second), shakespeareWords,
   beam.Trigger(trigger),
   beam.AllowedLateness(30*time.Second),
   beam.PanesDiscard(),
   )
   `

3. Write your own function that works with `side-input`. Its logic should check whether the words from shakespeare are contained in the list of analyzed words

   Function: `func matchWords(s beam.Scope, input beam.PCollection, viewPCollection beam.PCollection) beam.PCollection {
   view := beam.SideInput{
   Input: viewPCollection,
   }
   return beam.ParDo(s, matchFn, input, view)
   }
   func matchFn(word string, view func(analysis *Analysis) bool, emit func(analysis Analysis)) {
   var newAnalysis Analysis
   for view(&newAnalysis) {
   if word == newAnalysis.Word {
   emit(newAnalysis)
   }
   }
   }`
4. Divide the words into portions in the first **positive** words. In the **second** negative. And all the others in the third.

   Partition:`func partition(s beam.Scope, input beam.PCollection) []beam.PCollection {
   return beam.Partition(s, 3, func(analysis Analysis) int {
   if analysis.Negative != "0" {
   return 0
   }
   if analysis.Positive != "0" {
   return 1
   }
   return 2
   }, input)
   }
   `
5. To calculate the count with windows, use `ParDo` with `stats.Count(s, col)`.

   Apply the transformation: `func extractCountFn(prefix string, s beam.Scope, input beam.PCollection) beam.PCollection {
   col := beam.ParDo(s, func(analysis Analysis, emit func(string2 string)) {
   emit(prefix)
   }, input)
   return stats.Count(s, col)
   }`

6. To identify words with amplifying effects, you need to add a filter.

   Function: `func extractModelCountFn(prefix string, s beam.Scope, input beam.PCollection) beam.PCollection {
   col := filter.Include(s, input, func(analysis Analysis) bool {
   return analysis.Strong != "0" || analysis.Weak != "0"
   })
   result := beam.ParDo(s, func(analysis Analysis, emit func(string2 string)) {
   emit(prefix)
   }, col)
   return stats.Count(s, result)
   }`
{{end}}

{{if (eq .Sdk "java")}}
1. Change `getAnalysisPCollection` so that it returns a `PCollection` with `Row` objects.

   Write own DoFn `static class SentimentAnalysisExtractFn extends DoFn<String, Row> {
   @ProcessElement
   public void processElement(ProcessContext c) {
   String[] items = c.element().split(",");
   if (!items[1].equals("Negative")) {
   c.output(Row.withSchema(schema)
   .addValues(items[0].toLowerCase(), items[1], items[2], items[3], items[4], items[5], items[6], items[7])
   .build());
   }
   }
   }`
2. To use the analyzed words in the `side-input`, turn to `.apply(View.asList())`
3. Add a fixed-window that runs for 30 seconds `Window.into(Fixed Windows.of(Duration.standard Seconds(30)))`. And add a trigger that works after the first element with a delay of 5 seconds `AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(5))`
4. Write your own function that works with `side-input`. Its logic should check whether the words from shakespeare are contained in the list of analyzed words

   Function: `static PCollection<Row> getAnalysis(PCollection<String> pCollection, PCollectionView<List<Row>> viewAnalysisPCollection) {
   return pCollection.apply(ParDo.of(new DoFn<String, Row>() {
   @ProcessElement
   public void processElement(@Element String word, OutputReceiver<Row> out, ProcessContext context) {
   List<Row> analysisPCollection = context.sideInput(viewAnalysisPCollection);
   analysisPCollection.forEach(it -> {
   if (it.getString("word").equals(word)) {
   out.output(it);
   }
   });
   }
   }).withSideInputs(viewAnalysisPCollection)).setCoder(RowCoder.of(schema));
   }`
5. Divide the words into portions in the first **positive** words. In the **second** negative. And all the others in the third.

   Partition:`static PCollectionList<Row> getPartitions(PCollection<Row> input) {
   return input
   .apply(Partition.of(3,
   (Partition.PartitionFn<Row>) (analysis, numPartitions) -> {
   if (!analysis.getString("positive").equals("0")) {
   return 0;
   }
   if (!analysis.getString("negative").equals("0")) {
   return 1;
   }
   return 2;
   }));
   }`
6. To calculate the count with windows, use `Combine.globally` with `withoutDefaults()`. Apply the transformation `.apply(Combine.globally(Count.<Row>combineFn()).withoutDefaults())`

7. To identify words with amplifying effects, you need to add a filter `.apply(Filter.by(it -> !it.getString("strong").equals("0") || !it.getString("weak").equals("0")))`
{{end}}
{{if (eq .Sdk "python")}}
1. Process the file with the analyzed words to return a `PCollection` with `Analysis` objects.

   Write own DoFn `class ExtractAnalysis(beam.DoFn):
   def process(self, element):
   items = re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', element)
   if items[1] != 'Negative':
   yield Analysis(items[0].lower(), items[1], items[2], items[3], items[4], items[5], items[6], items[7])
   `
2. Add a fixed-window that runs for 30 seconds. And add a trigger that works after the first element with a delay of 5 seconds.

   Window and trigger: `windowed_words = (shakespeare
   | 'Window' >> beam.WindowInto(window.FixedWindows(30), trigger=trigger.AfterWatermark(
   early=trigger.AfterProcessingTime(5).has_ontime_pane(), late=trigger.AfterAll()), allowed_lateness=30,
   accumulation_mode=trigger.AccumulationMode.DISCARDING))`
3. To use the analyzed words in the `side-input`, turn to `windowed_words | beam.ParDo(MatchWordDoFn(), beam.pvalue.AsList(analysis))`
4. Write your own function that works with `side-input`. Its logic should check whether the words from shakespeare are contained in the list of analyzed words

   Function: `class MatchWordDoFn(beam.DoFn):
   def process(self, element, analysis):
   for a in analysis:
   if a.word == element:
   yield a`
5. Divide the words into portions in the first **positive** words. In the **second** negative. And all the others in the third.
   Partition: `class Partition(beam.PTransform):def expand(self, pcoll):return pcoll | beam.Partition(self._analysis_partition_fn, 3)
   @staticmethod
   def _analysis_partition_fn(analysis, num_partitions):
   if analysis.positive != "0":
   return 0
   elif analysis.negative != "0":
   return 1
   else:return 2
   `
6. To calculate the count with windows, use `beam.CombineGlobally` with `withoutDefaults()`. Apply the transformation `beam.CombineGlobally(CountCombineFn()).without_defaults()`

7. To identify words with amplifying effects, you need to add a filter `beam.Filter(lambda analysis: analysis.strong != '0' or analysis.weak != '0')`
{{end}}