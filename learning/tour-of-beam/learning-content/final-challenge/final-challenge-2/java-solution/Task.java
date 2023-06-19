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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Task {
    private static final Logger LOG = LoggerFactory.getLogger(Task.class);
    private static final String REGEX_FOR_CSV = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
    private static final Integer WINDOW_TIME = 30;
    private static final Integer TIME_OUTPUT_AFTER_FIRST_ELEMENT = 5;
    private static final Integer ALLOWED_LATENESS_TIME = 1;

    @DefaultSchema(JavaFieldSchema.class)
    public static class Analysis {
        public String word;
        public String negative;
        public String positive;
        public String uncertainty;
        public String litigious;
        public String strong;
        public String weak;
        public String constraining;

        @SchemaCreate
        public Analysis(String word, String negative, String positive, String uncertainty, String litigious, String strong, String weak, String constraining) {
            this.word = word;
            this.negative = negative;
            this.positive = positive;
            this.uncertainty = uncertainty;
            this.litigious = litigious;
            this.strong = strong;
            this.weak = weak;
            this.constraining = constraining;
        }

        @Override
        public String toString() {
            return "Analysis{" +
                    "word='" + word + '\'' +
                    ", negative='" + negative + '\'' +
                    ", positive='" + positive + '\'' +
                    ", uncertainty='" + uncertainty + '\'' +
                    ", litigious='" + litigious + '\'' +
                    ", strong='" + strong + '\'' +
                    ", weak='" + weak + '\'' +
                    ", constraining='" + constraining + '\'' +
                    '}';
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> shakespeare = getPCollection(pipeline);
        PCollectionView<List<Analysis>> viewAnalysisPCollection = getAnalysisPCollection(pipeline).apply(View.asList());

        Window<String> window = Window.into(FixedWindows.of(Duration.standardSeconds(WINDOW_TIME)));
        Trigger trigger = AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(TIME_OUTPUT_AFTER_FIRST_ELEMENT));

        PCollection<String> pCollection = shakespeare.
                apply(window.triggering(Repeatedly.forever(trigger)).withAllowedLateness(Duration.standardMinutes(ALLOWED_LATENESS_TIME)).discardingFiredPanes());

        PCollection<Analysis> result = getAnalysis(pCollection, viewAnalysisPCollection);

        PCollectionList<Analysis> pCollectionList = getPartitions(result);

        PCollection<Analysis> positivePCollection = pCollectionList.get(0);
        PCollection<Analysis> negativePCollection = pCollectionList.get(1);

        positivePCollection
                .apply(Combine.globally(Count.<Analysis>combineFn()).withoutDefaults())
                .apply(ParDo.of(new LogOutput<>("Positive word count")));

        positivePCollection
                .apply(Filter.by(it -> !it.strong.equals("0") || !it.weak.equals("0")))
                .apply(Combine.globally(Count.<Analysis>combineFn()).withoutDefaults())
                .apply(ParDo.of(new LogOutput<>("Positive words with enhanced effect count")));

        negativePCollection
                .apply(Combine.globally(Count.<Analysis>combineFn()).withoutDefaults())
                .apply(ParDo.of(new LogOutput<>("Negative word count")));

        negativePCollection
                .apply(Filter.by(it -> !it.strong.equals("0") || !it.weak.equals("0")))
                .apply(Combine.globally(Count.<Analysis>combineFn()).withoutDefaults())
                .apply(ParDo.of(new LogOutput<>("Negative words with enhanced effect count")));

        pipeline.run();
    }

    static PCollectionList<Analysis> getPartitions(PCollection<Analysis> input) {
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
    }

    static PCollection<Analysis> getAnalysis(PCollection<String> pCollection, PCollectionView<List<Analysis>> viewAnalysisPCollection) {
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
    }

    public static PCollection<String> getPCollection(Pipeline pipeline) {
        PCollection<String> rides = pipeline.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"));
        return rides.apply(FlatMapElements.into(TypeDescriptors.strings()).via(line -> Arrays.asList(line.toLowerCase().split(" "))))
                .apply(Filter.by((String word) -> !word.isEmpty()));
    }

    public static PCollection<Analysis> getAnalysisPCollection(Pipeline pipeline) {
        PCollection<String> words = pipeline.apply(TextIO.read().from("analysis.csv"));
        PCollection<Analysis> analysisPCollection = words.apply(ParDo.of(new SentimentAnalysisExtractFn())).setCoder(AnalysisCoder.of());
        return analysisPCollection;
    }

    static class SentimentAnalysisExtractFn extends DoFn<String, Analysis> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] items = c.element().split(",");
            if(!items[1].equals("Negative"))
                c.output(new Analysis(items[0].toLowerCase(), items[1], items[2], items[3], items[4], items[5], items[6], items[7]));
        }
    }

    static class AnalysisCoder extends Coder<Analysis> {
        private static final AnalysisCoder INSTANCE = new AnalysisCoder();

        public static AnalysisCoder of() {
            return INSTANCE;
        }

        @Override
        public void encode(Analysis analysis, OutputStream outStream) throws IOException {
            String line = analysis.word+","+analysis.negative+","+analysis.positive+","+analysis.uncertainty+","+analysis.litigious+","+analysis.strong+","+analysis.weak+","+analysis.constraining;
            outStream.write(line.getBytes());
        }

        @Override
        public Analysis decode(InputStream inStream) throws IOException {
            final String serializedDTOs = new String(StreamUtils.getBytesWithoutClosing(inStream));
            String[] items = serializedDTOs.split(",");
            return new Analysis(items[0].toLowerCase(), items[1], items[2], items[3], items[4], items[5], items[6], items[7]);
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
            return Collections.emptyList();
        }

        @Override
        public void verifyDeterministic() {
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
        public void processElement(ProcessContext c) throws Exception {
            LOG.info(prefix + ": {}", c.element());
        }
    }
}