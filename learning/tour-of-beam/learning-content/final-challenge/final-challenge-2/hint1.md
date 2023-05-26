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
5. Create a `MapElements.into(TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptors.doubles())).via(it->KV.of(it.id,it.price))` to group
6. Combine by key, the function that summarizes the prices `Combine.perKey(new SumDoubleBinaryCombineFn())`. 

   Sum function: `static class SumDoubleBinaryCombineFn extends Combine.BinaryCombineFn<Double> {
   @Override
   public Double apply(Double left, Double right) {
   return left + right;
   }
   }
   `
7. Write to a txt file: `TextIO.write().to("biggerThan10").withSuffix(".txt")`