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
1. Parse the csv file into a `Transaction` object.

   Extract: `func getTransactions(s beam.Scope, input beam.PCollection) beam.PCollection {
   return beam.ParDo(s, func(line string, emit func(transaction Transaction)) {
   csv := strings.Split(line, ",")
   	if csv[0] != "TransactionNo" {
   		id, _ := strconv.ParseInt(csv[0], 10, 64)
   		price, _ := strconv.ParseFloat(csv[4], 64)
   		quantity, _ := strconv.ParseInt(csv[5], 10, 64)
   		customerID, _ := strconv.ParseInt(csv[6], 10, 64)
   		emit(Transaction{
   			ID:          id,
   			Date:        csv[1],
   			ProductID:   csv[2],
   			ProductName: csv[3],
   			Price:       price,
   			Quantity:    quantity,
   			CustomerID:  customerID,
   			Country:     csv[7],
   		})
   	}
   }, input)
   }`
2. Add a fixed-window that runs for 30 seconds

   Window: `fixedWindowedItems := beam.WindowInto(s, window.NewFixedWindows(30*time.Second), transactions,
   beam.Trigger(trigger),
   beam.AllowedLateness(30*time.Minute),
   beam.PanesDiscard(),
   )`.

   And add a trigger that works after the first element with a delay of 5 seconds.

   Trigger: `trigger := trigger.AfterEndOfWindow().
   EarlyFiring(trigger.AfterProcessingTime().
   PlusDelay(5 * time.Second)).
   LateFiring(trigger.Repeat(trigger.AfterCount(1)))`

3. Filter so that the quantity is higher or equal to 20.

   Filter: `func filtering(s beam.Scope, input beam.PCollection) beam.PCollection {
   return filter.Include(s, input, func(element Transaction) bool {
   return element.Quantity >= 20
   })
   }`

4. Divide transactions into parts, the first contains transactions whose **price** is more than 10. And the rest are in the second.

   Partition: `func getPartition(s beam.Scope, input beam.PCollection) []beam.PCollection {
   return beam.Partition(s, 2, func(element Transaction) int {
   if element.Price >= 10 {
   return 0
   }
   return 1
   }, input)
   }`

5. Create a map function `func mapIdWithPrice(s beam.Scope, input beam.PCollection) beam.PCollection {
   return beam.ParDo(s, func(element Transaction, emit func(int64, float64)) {
   emit(element.ID, element.Price)
   }, input)
   }` for group

6. Combine by key, the function that summarizes the prices `func sumCombine(s beam.Scope, input beam.PCollection) beam.PCollection {
   return stats.SumPerKey(s, input)
   }`

7. To write to a file, first you need to convert to a string `func convertToString(s beam.Scope, input beam.PCollection) beam.PCollection {
   return beam.ParDo(s, func(id int64, sum float64, emit func(string)) {
   emit(fmt.Sprint("id: ", id, " , sum: ", sum))
   }, input)
   }`

8. Write to a txt file: `textio.Write(s, "smallerThan10.txt", smallerThan10)`

{{end}}

{{if (eq .Sdk "java")}}
1. Parse the csv file into a `Transaction` object.

   Extract: `static class ExtractDataFn extends DoFn<String, Transaction> {
   @ProcessElement
   public void processElement(ProcessContext c) {
   String[] items = c.element().split(REGEX_FOR_CSV);
   try {
   c.output(new Transaction(Long.valueOf(items[0]), items[1], items[2], items[3], Double.valueOf(items[4]), Integer.parseInt(items[5]), Long.valueOf(items[6]), items[7]));
   } catch (Exception e) {
   System.out.println("Skip header");
   }
   }
   }`
2. Add a fixed-window that runs for 30 seconds `Window.into(Fixed Windows.of(Duration.standard Seconds(30)))`. And add a trigger that works after the first element with a delay of 5 seconds `AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(5))`
3. Filter so that the number is higher or equal to 20 `Filter.by(it -> it.quantity >= 20)`
4. Divide transactions into parts, the first contains transactions whose **price** is more than 10. And the rest are in the second.

   Partition: `static class TransactionPartitionFn extends PTransform<PCollection<Transaction>, PCollectionList<Transaction>> {
   @Override
   public PCollectionList<Transaction> expand(PCollection<Transaction> input) {
   return input.apply(Partition.of(2,
   (Partition.PartitionFn<Transaction>) (transaction, numPartitions) -> {
   if (transaction.price > 10) {
   return 0;
   } else {
   return 1;
   }
   }));
   }
   }`
5. Create a map function `MapElements.into(TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptors.doubles())).via(it->KV.of(it.id,it.price))` for group
6. Combine by key, the function that summarizes the prices `Combine.perKey(new SumDoubleBinaryCombineFn())`.

   Sum function: `static class SumDoubleBinaryCombineFn extends Combine.BinaryCombineFn<Double> {
   @Override
   public Double apply(Double left, Double right) {
   return left + right;
   }
   }
   `
7. To write to a file, first you need to convert to a string `.apply(MapElements.into(TypeDescriptor.of(String.class)).via(it -> it.toString()))`

8. Write to a txt file: `TextIO.write().to("biggerThan10").withSuffix(".txt")`
{{end}}

{{if (eq .Sdk "python")}}
1. Parse the csv file into a `Transaction` object.

   Extract: `class ExtractDataFn(beam.DoFn):
   def process(self, element):
   items = re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', element)
   if items[0] != 'TransactionNo':
   yield Transaction(items[0], items[1], items[2], items[3], items[4], items[5], items[6], items[7])`

2. Add a fixed-window that runs for 30 seconds. And add a trigger that works after the first element with a delay of 5 seconds.

   Window and trigger: `windowed_transactions = (transactions
   | 'Window' >> beam.WindowInto(window.FixedWindows(30), trigger=trigger.AfterWatermark(
   early=trigger.AfterProcessingTime(5).has_ontime_pane(), late=trigger.AfterAll()),
   allowed_lateness=30,
   accumulation_mode=trigger.AccumulationMode.DISCARDING))`.

3. Filter so that the number is higher or equal to 20 `'Filtering' >> beam.Filter(lambda t: int(t.quantity) >= 20)`
4. Divide transactions into parts, the first contains transactions whose **price** is more than 10. And the rest are in the second. `'Partition transactions' >> beam.Partition(partitionTransactions, 2))`
5. Create a map function `'Map id and price for bigger' >> beam.Map(lambda transaction: (transaction.transaction_no, float(transaction.price)))` for group
6. Combine by key, the function that summarizes the prices.
   Sum function: `'Calculate sum for biggerThan10' >> beam.CombinePerKey(sum)`

7. Write to a txt file: `'Write biggerThan10 results to text file' >> beam.io.WriteToText('biggerThan10', '.txt', shard_name_template=''))`
{{end}}