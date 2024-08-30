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
//   name: FinalSolution2
//   description: Final challenge solution 2.
//   multifile: true
//   files:
//     - name: analysis.csv
//   context_line: 98
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class Task {
    private static final Logger LOG = LoggerFactory.getLogger(Task.class);
    private static final Schema schema = Schema.builder()
            .addField("word", Schema.FieldType.STRING)
            .addField("negative", Schema.FieldType.STRING)
            .addField("positive", Schema.FieldType.STRING)
            .addField("uncertainty", Schema.FieldType.STRING)
            .addField("litigious", Schema.FieldType.STRING)
            .addField("strong", Schema.FieldType.STRING)
            .addField("weak", Schema.FieldType.STRING)
            .addField("constraining", Schema.FieldType.STRING)
            .build();

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> shakespeare = getPCollection(pipeline);
        PCollection<Row> analysisPCollection = getAnalysisPCollection(pipeline);

        PCollectionView<List<Row>> viewAnalysisPCollection = analysisPCollection
                .setCoder(RowCoder.of(schema))
                .apply(View.asList());

        PCollection<Row> result = getAnalysis(shakespeare, viewAnalysisPCollection);

        PCollectionList<Row> pCollectionList = getPartitions(result);

        PCollection<Row> positivePCollection = pCollectionList.get(0).setCoder(RowCoder.of(schema));
        PCollection<Row> negativePCollection = pCollectionList.get(1).setCoder(RowCoder.of(schema));

        positivePCollection
                .apply(Combine.globally(Count.<Row>combineFn()).withoutDefaults())
                .apply(ParDo.of(new LogOutput<>("Positive word count")));

        positivePCollection
                .apply(Filter.by(it -> !it.getString("strong").equals("0") || !it.getString("weak").equals("0")))
                .apply(Combine.globally(Count.<Row>combineFn()).withoutDefaults())
                .apply(ParDo.of(new LogOutput<>("Positive words with enhanced effect count")));

        negativePCollection
                .apply(Combine.globally(Count.<Row>combineFn()).withoutDefaults())
                .apply(ParDo.of(new LogOutput<>("Negative word count")));

        negativePCollection
                .apply(Filter.by(it -> !it.getString("strong").equals("0") || !it.getString("weak").equals("0")))
                .apply(Combine.globally(Count.<Row>combineFn()).withoutDefaults())
                .apply(ParDo.of(new LogOutput<>("Negative words with enhanced effect count")));

        pipeline.run().waitUntilFinish();
    }

    static PCollectionList<Row> getPartitions(PCollection<Row> input) {
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
    }

    static PCollection<Row> getAnalysis(PCollection<String> pCollection, PCollectionView<List<Row>> viewAnalysisPCollection) {
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
    }

    public static PCollection<String> getPCollection(Pipeline pipeline) {
        PCollection<String> rides = pipeline.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"));
        return rides.apply(FlatMapElements.into(TypeDescriptors.strings()).via(line -> Arrays.asList(line.toLowerCase().split(" "))))
                .apply(Filter.by((String word) -> !word.isEmpty()));
    }

    public static PCollection<Row> getAnalysisPCollection(Pipeline pipeline) {
        PCollection<String> words = pipeline.apply(TextIO.read().from("analysis.csv"));
        return words.apply(ParDo.of(new SentimentAnalysisExtractFn()));
    }

    static class SentimentAnalysisExtractFn extends DoFn<String, Row> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] items = c.element().split(",");
            if (!items[1].equals("Negative")) {
                c.output(Row.withSchema(schema)
                        .addValues(items[0].toLowerCase(), items[1], items[2], items[3], items[4], items[5], items[6], items[7])
                        .build());
            }
        }
    }

    static class LogOutput<T> extends DoFn<T, T> {

        private final String prefix;

        LogOutput() {
            this.prefix = "Processing element";
        }

        LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            LOG.info(prefix + ": " + c.element());
            c.output(c.element());
        }
    }
}