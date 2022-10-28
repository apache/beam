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
package org.apache.beam.examples.multilanguage;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.python.PythonExternalTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.PythonCallableSource;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * An example that counts words in Shakespeare and utilizes a Python external transform.
 *
 * <p>This class, {@link PythonDataframeWordCount}, uses Python DataframeTransform to count words
 * from the input text file.
 *
 * <p>The example command below shows how to run this pipeline on Dataflow runner:
 *
 * <pre>{@code
 * ./gradlew :examples:multi-language:pythonDataframeWordCount --args=" \
 * --runner=DataflowRunner \
 * --output=gs://{$OUTPUT_BUCKET}/count \
 * --sdkHarnessContainerImageOverrides=.*python.*,gcr.io/apache-beam-testing/beam-sdk/beam_python{$PYTHON_VERSION}_sdk:latest"
 * }</pre>
 */
public class PythonDataframeWordCount {
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

  /** A SimpleFunction that converts a counted row into a printable string. */
  public static class FormatAsTextFn extends SimpleFunction<Row, String> {
    @Override
    public String apply(Row input) {
      return input.getString("word") + ": " + input.getInt32("count");
    }
  }

  /** Options supported by {@link PythonDataframeWordCount}. */
  public interface WordCountOptions extends PipelineOptions {

    /**
     * By default, this example reads from a public dataset containing the text of King Lear. Set
     * this option to choose a different input file or glob.
     */
    @Description("Path of the file to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    String getInputFile();

    void setInputFile(String value);

    /** Set this required option to specify where to write the output. */
    @Description("Path of the file to write to")
    @Required
    String getOutput();

    void setOutput(String value);

    /** Set this option to specify Python expansion service URL. */
    @Description("URL of Python expansion service")
    String getExpansionService();

    void setExpansionService(String value);
  }

  static void runWordCount(WordCountOptions options) {
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
