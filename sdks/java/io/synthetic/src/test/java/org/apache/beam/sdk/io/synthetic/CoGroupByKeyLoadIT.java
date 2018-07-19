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
package org.apache.beam.sdk.io.synthetic;

import static org.apache.beam.sdk.io.synthetic.SyntheticSourceTestUtils.fromString;

import java.io.IOException;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance fanout test for {@link CoGroupByKey}. */
@RunWith(JUnit4.class)
public class CoGroupByKeyLoadIT {

  private static Options options;

  // TODO: parse it in a more decent way
  private static SyntheticBoundedIO.SyntheticSourceOptions syntheticSourceOptions;

  private static final TupleTag<byte[]> INPUT_TAG = new TupleTag<>();
  private static final TupleTag<byte[]> CO_INPUT_TAG = new TupleTag<>();

  @Rule public TestPipeline pipeline = TestPipeline.create();

  /** Pipeline options for the test. */
  public interface Options extends TestPipelineOptions {

    @Description("The JSON representation of SyntheticBoundedInput.SourceOptions.")
    @Validation.Required
    String getInputOptions();

    void setInputOptions(String inputOptions);

    @Description("The number of gbk to perform")
    @Default.Integer(5)
    Integer getFanout();

    void setFanout(Integer shuffleFanout);
  }

  @BeforeClass
  public static void setup() throws IOException {
    PipelineOptionsFactory.register(Options.class);

    options =
        PipelineOptionsValidator.validate(
            Options.class, TestPipeline.testingPipelineOptions().as(Options.class));

    syntheticSourceOptions = fromString(options.getInputOptions());
  }

  @Test
  public void groupByKeyTest() {
    PCollection<KV<byte[], byte[]>> input =
        pipeline.apply("Read input", SyntheticBoundedIO.readFrom(syntheticSourceOptions));

    PCollection<KV<byte[], byte[]>> coInput =
        pipeline.apply("Read co-input", SyntheticBoundedIO.readFrom(syntheticSourceOptions));

    for (int branch = 0; branch < options.getFanout(); branch++) {
      coGroupAndUngroup(input, coInput, branch);
    }

    pipeline.run().waitUntilFinish();
  }

  private void coGroupAndUngroup(
      PCollection<KV<byte[], byte[]>> input, PCollection<KV<byte[], byte[]>> coInput, int branch) {

    PCollection<KV<byte[], CoGbkResult>> groupedData =
        KeyedPCollectionTuple.of(INPUT_TAG, input)
            .and(CO_INPUT_TAG, coInput)
            .apply(String.format("Co - group by key (%s)", branch), CoGroupByKey.create());
    groupedData.apply(String.format("Ungroup (%s)", branch), ParDo.of(new Ungroup()));
  }

  private static class Ungroup extends DoFn<KV<byte[], CoGbkResult>, KV<byte[], byte[]>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      byte[] key = c.element().getKey();
      CoGbkResult elementValue = c.element().getValue();

      Iterable<byte[]> inputs = elementValue.getAll(INPUT_TAG);
      Iterable<byte[]> coInputs = elementValue.getAll(CO_INPUT_TAG);

      for (byte[] value : inputs) {
        c.output(KV.of(key, value));
      }

      for (byte[] value : coInputs) {
        c.output(KV.of(key, value));
      }
    }
  }
}
