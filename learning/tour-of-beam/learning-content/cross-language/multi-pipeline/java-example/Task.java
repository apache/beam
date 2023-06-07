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
//   name: multi-pipeline
//   description: Multi pipeline example.
//   multifile: false
//   context_line: 33
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   never_run: true
//   tags:
//     - hellobeam

public class Task {
    public static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

    // Extract the words and create the rows for counting.
    static class ExtractWordsFn extends DoFn<String, Row> {
        public static final Schema SCHEMA =
                Schema.of(
                        Schema.Field.of("word", Schema.FieldType.STRING),
                        Schema.Field.of("count", Schema.FieldType.INT32));
        private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
        private final Distribution lineLenDist =
                Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<Row> receiver) {
            lineLenDist.update(element.length());
            if (element.trim().isEmpty()) {
                emptyLines.inc();
            }

            // Split the line into words.
            String[] words = element.split(TOKENIZER_PATTERN, -1);

            // Output each word encountered into the output PCollection.
            for (String word : words) {
                if (!word.isEmpty()) {
                    receiver.output(
                            Row.withSchema(SCHEMA)
                                    .withFieldValue("word", word)
                                    .withFieldValue("count", 1)
                                    .build());
                }
            }
        }
    }

    /**
     * A SimpleFunction that converts a counted row into a printable string.
     */
    public static class FormatAsTextFn extends SimpleFunction<Row, String> {
        @Override
        public String apply(Row input) {
            return input.getString("word") + ": " + input.getInt32("count");
        }
    }

    /**
     * Options supported by {@link PythonDataframeWordCount}.
     */
    public interface WordCountOptions extends PipelineOptions {

        /**
         * By default, this example reads from a public dataset containing the text of King Lear. Set
         * this option to choose a different input file or glob.
         */
        @Description("Path of the file to read from")
        @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
        String getInputFile();

        void setInputFile(String value);

        /**
         * Set this required option to specify where to write the output.
         */
        @Description("Path of the file to write to")
        @Required
        String getOutput();

        void setOutput(String value);

        /**
         * Set this option to specify Python expansion service URL.
         */
        @Description("URL of Python expansion service")
        String getExpansionService();

        void setExpansionService(String value);
    }

    static void runWordCount(WordCountOptions options) {
        options.setOutput("input.txt");
        Pipeline p = Pipeline.create(options);

        p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply(ParDo.of(new ExtractWordsFn()))
                .setRowSchema(ExtractWordsFn.SCHEMA)
                .apply(
                        PythonExternalTransform.<PCollection<Row>, PCollection<Row>>from(
                                        "apache_beam.dataframe.transforms.DataframeTransform",
                                        options.getExpansionService())
                                .withKwarg("func", PythonCallableSource.of("lambda df: df.groupby('word').sum()"))
                                .withKwarg("include_indexes", true))
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply("WriteCounts", TextIO.write().to(options.getOutput()));

        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        WordCountOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCountOptions.class);

        runWordCount(options);
    }
}