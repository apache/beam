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
//   name: CommonTransformsSolution
//   description: Common Transforms motivating challenge solution.
//   multifile: false
//   context_line: 44
//   categories:
//     - Quickstart
//   complexity: BASIC
//   tags:
//     - hellobeam


import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // Create input PCollection
        PCollection<String> rides = pipeline.apply(TextIO.read().from("gs://apache-beam-samples/nyc_taxi/misc/sample1000.csv"));

        PCollection<Double> rideTotalAmounts = rides.apply(ParDo.of(new ExtractTaxiRideCostFn()));

        PCollection<Double> aboveCosts = getAboveCost(rideTotalAmounts);

        PCollection<Double> belowCosts = getBelowCost(rideTotalAmounts);

        aboveCosts.apply(Sum.doublesGlobally());
        belowCosts.apply(Sum.doublesGlobally());

        aboveCosts.apply(Count.globally());
        belowCosts.apply(Count.globally());

        System.out.println(mean);

        pipeline.run();
    }


    static PCollection<Long> applyTransform(PCollection<Double> input) {
        return input.apply(Count.globally());
    }


    static PCollection<KV<String, String>> applyTransform(PCollection<String> input) {
        return input.apply(WithKeys.of(number -> "Above count"));
    }

    static PCollection<Double> getAboveCost(PCollection<Double> input,double fixedCost){
        return input.apply(Filter.by(number -> number >= fixedCost));
    }

    static PCollection<Double> getBelowCost(PCollection<Double> input,double fixedCost){
        return input.apply(Filter.by(number -> number < fixedCost));
    }

    static PCollection<KV<String, Integer>> setKeyForNumbers(PCollection<Integer> input) {
        return input
                .apply(WithKeys.of(new SerializableFunction<Integer, String>() {
                    @Override
                    public String apply(Integer number) {
                        if (number % 2 == 0) {
                            return "even";
                        }
                        else {
                            return "odd";
                        }
                    }
                }));
    }

    static class ExtractTaxiRideCostFn extends DoFn<String, Double> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] items = c.element().split(",");
            Double totalAmount = tryParseTaxiRideCost(items);
            c.output(totalAmount);
        }
    }

    private static String tryParseString(String[] inputItems, int index) {
        return inputItems.length > index ? inputItems[index] : null;
    }
    private static Double tryParseTaxiRideCost(String[] inputItems) {
        try {
            return Double.parseDouble(tryParseString(inputItems, 16));
        } catch (NumberFormatException | NullPointerException e) {
            return 0.0;
        }
    }

    static class LogOutput<T> extends DoFn<T, T> {
        private String prefix;

        LogOutput() {
            this.prefix = "Processing element";
        }

        LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info(prefix + ": {}", c.element());
        }
    }
}