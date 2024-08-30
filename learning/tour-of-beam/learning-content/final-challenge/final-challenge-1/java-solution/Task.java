/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// beam-playground:
//   name: FinalSolution1
//   description: Final challenge solution 1.
//   multifile: true
//   files:
//     - name: input.csv
//   context_line: 50
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

import java.io.*;
import java.util.List;
import java.util.stream.Collectors;

public class Task {
    private static final Integer WINDOW_TIME = 30;
    private static final Integer TIME_OUTPUT_AFTER_FIRST_ELEMENT = 5;
    private static final Integer ALLOWED_LATENESS_TIME = 1;

    private static final String REGEX_FOR_CSV = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        Window<Transaction> window = Window.into(FixedWindows.of(Duration.standardSeconds(WINDOW_TIME)));

        Trigger trigger = AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(TIME_OUTPUT_AFTER_FIRST_ELEMENT));

        PCollection<Transaction> input = pipeline.apply(TextIO.read().from("input.csv"))
                .apply("Data", ParDo.of(new ExtractDataFn()))
                .setCoder(TransactionSerializableCoder.of());

        PCollectionList<Transaction> parts = input
                .apply(window.triggering(Repeatedly.forever(trigger)).withAllowedLateness(Duration.standardMinutes(ALLOWED_LATENESS_TIME)).discardingFiredPanes())
                .apply(Filter.by(it -> it.quantity >= 20))
                .apply(new TransactionPartitionFn());

        parts.get(0)
                .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles())).via(it -> KV.of(it.productId, it.price)))
                .apply(Combine.perKey(new SumDoubleBinaryCombineFn()))
                .apply(MapElements.into(TypeDescriptor.of(String.class)).via(it -> it.toString()))
                .apply("WriteToFile", TextIO.write().withoutSharding().to("price_more_than_10").withSuffix(".txt"));

        parts.get(1)
                .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles())).via(it -> KV.of(it.productId, it.price)))
                .apply(Combine.perKey(new SumDoubleBinaryCombineFn()))
                .apply(MapElements.into(TypeDescriptor.of(String.class)).via(it -> it.toString()))
                .apply("WriteToFile", TextIO.write().withoutSharding().to("price_less_than_10").withSuffix(".txt"));

        pipeline.run().waitUntilFinish();
    }

    static class SumDoubleBinaryCombineFn extends Combine.BinaryCombineFn<Double> {
        @Override
        public Double apply(Double left, Double right) {
            return left + right;
        }

    }

    static class TransactionPartitionFn extends PTransform<PCollection<Transaction>, PCollectionList<Transaction>> {
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
    }

    static class TransactionSerializableCoder extends Coder<Transaction> {
        private static final TransactionSerializableCoder INSTANCE = new TransactionSerializableCoder();

        public static TransactionSerializableCoder of() {
            return INSTANCE;
        }

        private static final String NAMES_SEPARATOR = "_";

        @Override
        public void encode(Transaction transaction, OutputStream outStream) throws IOException {
            String serializableRecord = transaction.id + NAMES_SEPARATOR + transaction.date + NAMES_SEPARATOR + transaction.productId +
                    NAMES_SEPARATOR + transaction.productName + NAMES_SEPARATOR + transaction.price + NAMES_SEPARATOR + transaction.quantity +
                    NAMES_SEPARATOR + transaction.customerId + NAMES_SEPARATOR + transaction.country;
            outStream.write(serializableRecord.getBytes());
        }

        @Override
        public Transaction decode(InputStream inStream) {
            String serializedRecord = new BufferedReader(new InputStreamReader(inStream)).lines()
                    .parallel().collect(Collectors.joining("\n"));
            String[] items = serializedRecord.split(NAMES_SEPARATOR);
            return new Transaction(Long.valueOf(items[0]), items[1], items[2], items[3], Double.valueOf(items[4]), Integer.parseInt(items[5]), Long.valueOf(items[6]), items[7]);
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
            return null;
        }

        @Override
        public void verifyDeterministic() {
        }
    }


    static class ExtractDataFn extends DoFn<String, Transaction> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] items = c.element().split(REGEX_FOR_CSV);
            try {
                c.output(new Transaction(Long.valueOf(items[0]), items[1], items[2], items[3], Double.valueOf(items[4]), Integer.parseInt(items[5]), Long.valueOf(items[6]), items[7]));
            } catch (Exception e) {
                System.out.println("Skip header");
            }
        }
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Transaction {
        public Long id;
        public String date;
        public String productId;
        public String productName;
        public Double price;
        public Integer quantity;
        public Long customerId;
        public String country;

        public Transaction() {
        }

        @SchemaCreate
        public Transaction(Long id, String date, String productId, String productName, Double price, Integer quantity, Long customerId, String country) {
            this.id = id;
            this.date = date;
            this.productId = productId;
            this.productName = productName;
            this.price = price;
            this.quantity = quantity;
            this.customerId = customerId;
            this.country = country;
        }

        @Override
        public String toString() {
            return "Transaction{" +
                    "id=" + id +
                    ", date='" + date + '\'' +
                    ", productId='" + productId + '\'' +
                    ", productName='" + productName + '\'' +
                    ", price='" + price + '\'' +
                    ", quantity=" + quantity +
                    ", customerId=" + customerId +
                    ", country='" + country + '\'' +
                    '}';
        }
    }
}