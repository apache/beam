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
//   name: TriggersChallenge
//   description: Triggers motivating challenge solution.
//   multifile: false
//   context_line: 51
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam


import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // Create input PCollection
        PCollection<String> input = pipeline.apply(TextIO.read().from("gs://apache-beam-samples/nyc_taxi/misc/sample1000.csv"));

        // Extract cost from PCollection
        PCollection<Double> rideTotalAmounts = input.apply(ParDo.of(new ExtractTaxiRideCostFn()));

        Window<Double> window = Window.into(FixedWindows.of(Duration.standardMinutes(5)));

        rideTotalAmounts.apply("Log below cost",ParDo.of(new LogOutput<>("Below pCollection output")));


        pipeline.run();
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