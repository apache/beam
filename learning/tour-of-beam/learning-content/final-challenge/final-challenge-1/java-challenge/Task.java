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
//   name: FinalChallenge1
//   description: Final challenge 1.
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
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {
    private static final Logger LOG = LoggerFactory.getLogger(Task.class);
    private static final Integer WINDOW_TIME = 30;
    private static final Integer TIME_OUTPUT_AFTER_FIRST_ELEMENT = 5;
    private static final Integer ALLOWED_LATENESS_TIME = 1;

    private static final String REGEX_FOR_CSV = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) {
        LOG.info("Running Task");
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> input = pipeline.apply(TextIO.read().from("input.csv"));

        pipeline.run().waitUntilFinish();
    }

    static class LogOutput<T> extends DoFn<T, T> {
        private final String prefix;

        LogOutput() {
            this.prefix = "Processing element";
        }

        LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @DoFn.ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info(prefix + ": {}", c.element());
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