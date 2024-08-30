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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.python.transforms.RunInference;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;

/**
 * An example Java Multi-language pipeline that Performs image classification on handwritten digits
 * from the <a href="https://en.wikipedia.org/wiki/MNIST_database">MNIST</a> database.
 *
 * <p>For more details and instructions for running this please see <a
 * href="https://github.com/apache/beam/tree/master/examples/multi-language">here</a>.
 */
public class SklearnMnistClassification {

  /**
   * We generate a Python function that produces a KV sklearn model loader and use that to
   * instantiate {@link RunInference}. Note that {@code RunInference} can be instantiated with any
   * arbitrary function that produces a model loader.
   */
  private String getModelLoaderScript() {
    String s = "from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerNumpy\n";
    s = s + "from apache_beam.ml.inference.base import KeyedModelHandler\n";
    s = s + "def get_model_handler(model_uri):\n";
    s = s + "  return KeyedModelHandler(SklearnModelHandlerNumpy(model_uri))\n";

    return s;
  }

  /** Filters out the header of the dataset that should not be used for the computation. */
  static class FilterNonRecordsFn implements SerializableFunction<String, Boolean> {

    @Override
    public Boolean apply(String input) {
      return !input.startsWith("label");
    }
  }

  /**
   * Seperates our input records to label and data. Each input record is a set of comma separated
   * string digits where first digit is the label and rest are data (pixels that represent the
   * digit).
   */
  static class RecordsToLabeledPixelsFn extends SimpleFunction<String, KV<Long, Iterable<Long>>> {

    @Override
    public KV<Long, Iterable<Long>> apply(String input) {
      String[] data = Splitter.on(',').splitToList(input).toArray(new String[] {});
      Long label = Long.valueOf(data[0]);
      List<Long> pixels = new ArrayList<Long>();
      for (int i = 1; i < data.length; i++) {
        pixels.add(Long.valueOf(data[i]));
      }

      return KV.of(label, pixels);
    }
  }

  /** Formats the output to a mapping from the expected digit to the inferred digit. */
  static class FormatOutput extends SimpleFunction<KV<Long, Row>, String> {

    @Override
    public String apply(KV<Long, Row> input) {
      return input.getKey() + "," + input.getValue().getString("inference");
    }
  }

  void runExample(SklearnMnistClassificationOptions options, String expansionService) {
    // Schema of the output PCollection Row type to be provided to the RunInference transform.
    Schema schema =
        Schema.of(
            Schema.Field.of("example", Schema.FieldType.array(Schema.FieldType.INT64)),
            Schema.Field.of("inference", FieldType.STRING));

    Pipeline pipeline = Pipeline.create(options);
    PCollection<KV<Long, Iterable<Long>>> col =
        pipeline
            .apply(TextIO.read().from(options.getInput()))
            .apply(Filter.by(new FilterNonRecordsFn()))
            .apply(MapElements.via(new RecordsToLabeledPixelsFn()));
    col.apply(
            RunInference.ofKVs(getModelLoaderScript(), schema, VarLongCoder.of())
                .withKwarg("model_uri", options.getModelPath())
                .withExpansionService(expansionService))
        .apply(MapElements.via(new FormatOutput()))
        .apply(TextIO.write().to(options.getOutput()));

    pipeline.run().waitUntilFinish();
  }

  public interface SklearnMnistClassificationOptions extends PipelineOptions {

    @Description("Path to an input file that contains labels and pixels to feed into the model")
    @Default.String("gs://apache-beam-samples/multi-language/mnist/example_input.csv")
    String getInput();

    void setInput(String value);

    @Description("Path for storing the output")
    @Required
    String getOutput();

    void setOutput(String value);

    @Description(
        "Path to a model file that contains the pickled file of a scikit-learn model trained on MNIST data")
    @Default.String("gs://apache-beam-samples/multi-language/mnist/example_model")
    String getModelPath();

    void setModelPath(String value);

    /** Set this option to specify Python expansion service URL. */
    @Description("URL of Python expansion service")
    @Default.String("")
    String getExpansionService();

    void setExpansionService(String value);
  }

  public static void main(String[] args) {
    SklearnMnistClassificationOptions options =
        PipelineOptionsFactory.fromArgs(args).as(SklearnMnistClassificationOptions.class);
    SklearnMnistClassification example = new SklearnMnistClassification();
    example.runExample(options, options.getExpansionService());
  }
}
