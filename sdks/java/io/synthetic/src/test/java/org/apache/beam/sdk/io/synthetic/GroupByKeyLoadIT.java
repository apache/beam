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
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance fanout test for {@link GroupByKey}. */
@RunWith(JUnit4.class)
public class GroupByKeyLoadIT {

  private static Options options;

  // TODO: parse it in a more decent way
  private static SyntheticBoundedIO.SyntheticSourceOptions syntheticSourceOptions;

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
  public void groupByKeyLoadTest() {
    PCollection<KV<byte[], byte[]>> input =
        pipeline.apply(SyntheticBoundedIO.readFrom(syntheticSourceOptions));

    for (int branch = 0; branch < options.getFanout(); branch++) {
      groupAndUngroup(input, branch);
    }

    pipeline.run().waitUntilFinish();
  }

  private void groupAndUngroup(PCollection<KV<byte[], byte[]>> input, int branchNumber) {
    PCollection<KV<byte[], Iterable<byte[]>>> groupedData =
        input.apply(String.format("Group by key (%s)", branchNumber), GroupByKey.create());

    groupedData.apply(
        String.format("Ungroup (%s)", branchNumber), ParDo.of(new UngroupFn()));
  }

  // TODO: remove in case you're sure that we don't perform this
  private void groupAndUngroupNTimesSequentially(
      final PCollection<KV<byte[], byte[]>> input, final Integer n) {
    if (n == 0) {
      return;
    } else {
      // todo: synthetic step.
      PCollection<KV<byte[], Iterable<byte[]>>> groupedCollection =
          input.apply(String.format("Group by key no: %s.", n), GroupByKey.create());

      PCollection<KV<byte[], byte[]>> nextInput =
          groupedCollection.apply(
              String.format("Ungroup by key no: %s.", n), ParDo.of(new UngroupFn()));

      groupAndUngroupNTimesSequentially(nextInput, n - 1);
    }
  }

  private static class UngroupFn extends DoFn<KV<byte[], Iterable<byte[]>>, KV<byte[], byte[]>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      byte[] key = c.element().getKey();
      for (byte[] value : c.element().getValue()) {
        c.output(KV.of(key, value));
      }
    }
  }
}
