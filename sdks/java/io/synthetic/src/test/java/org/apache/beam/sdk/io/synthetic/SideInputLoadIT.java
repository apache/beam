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

import static org.apache.beam.sdk.io.synthetic.SideInputLoadIT.SideInputType.ITERABLE;
import static org.apache.beam.sdk.io.synthetic.SideInputLoadIT.SideInputType.MAP;
import static org.apache.beam.sdk.io.synthetic.SyntheticSourceTestUtils.fromString;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance fanout test for {@link CoGroupByKey}. */
@RunWith(JUnit4.class)
public class SideInputLoadIT {

  private static Options options;

  private static SyntheticBoundedIO.SyntheticSourceOptions inputOptions;

  private static SyntheticBoundedIO.SyntheticSourceOptions sideInputOptions;

  @Rule public TestPipeline pipeline = TestPipeline.create();

  public enum SideInputType {
    ITERABLE,
    MAP
  }

  /** Pipeline options for the test. */
  public interface Options extends TestPipelineOptions {

    @Description("The JSON representation of SyntheticBoundedInput.SourceOptions for main input.")
    @Validation.Required
    String getInputOptions();

    void setInputOptions(String inputOptions);

    @Description("The JSON representation of SyntheticBoundedInput.SourceOptions for side input.")
    @Validation.Required
    String getSideInputOptions();

    void setSideInputOptions(String sideInputOptions);

    @Description("The number of gbk to perform")
    @Default.Integer(5)
    Integer getFanout();

    void setFanout(Integer shuffleFanout);

    @Description("Type of side input")
    @Default.Enum("ITERABLE")
    SideInputType getSideInputType();

    void setSideInputType(SideInputType sideInputType);
  }

  @BeforeClass
  public static void setup() throws IOException {
    PipelineOptionsFactory.register(Options.class);

    options =
        PipelineOptionsValidator.validate(
            Options.class, TestPipeline.testingPipelineOptions().as(Options.class));

    inputOptions = fromString(options.getInputOptions());
    sideInputOptions = fromString(options.getSideInputOptions());
  }

  @Test
  public void sideInputLoadTest() {
    PCollection<KV<byte[], byte[]>> input =
        pipeline.apply("Read input", SyntheticBoundedIO.readFrom(inputOptions));

    PCollection<KV<byte[], byte[]>> sideInputCollection =
        pipeline.apply("Read side input", SyntheticBoundedIO.readFrom(sideInputOptions));

    SideInputType sideInputType = options.getSideInputType();

    if (sideInputType == ITERABLE) {
      PCollectionView<Iterable<KV<byte[], byte[]>>> sideInput =
          sideInputCollection.apply(View.asIterable());
      input.apply(ParDo.of(new ConsumeIterable(sideInput)).withSideInputs(sideInput));
    } else if (sideInputType == MAP) {

      // For the sake of example completeness I used multimap here,
      // because Map requires none key duplicates in the PCollection (otherwise fails).
      // TODO: Should we GroupByKey or Combine.perKey() and use asMap() here?
      PCollectionView<Map<byte[], Iterable<byte[]>>> sideInput = sideInputCollection
          .apply(View.asMultimap());
      input.apply(ParDo.of(new ConsumeMap(sideInput)).withSideInputs(sideInput));
    }

    pipeline.run().waitUntilFinish();
  }

  /* For every element, iterate over the whole iterable side input. */
  private static class ConsumeIterable extends DoFn<KV<byte[], byte[]>, KV<byte[], byte[]>> {

    private PCollectionView<Iterable<KV<byte[], byte[]>>> sideInput;

    ConsumeIterable(PCollectionView<Iterable<KV<byte[], byte[]>>> sideInput) {
      this.sideInput = sideInput;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Iterable<KV<byte[], byte[]>> sideInput = c.sideInput(this.sideInput);

      for (KV<byte[], byte[]> sideInputElement : sideInput) {
        // TODO: count consumed bytes with the metrics API.
      }
    }
  }

  /* For every element, find its corresponding value in side input. */
  private static class ConsumeMap extends DoFn<KV<byte[], byte[]>, KV<byte[], byte[]>> {

    private PCollectionView<Map<byte[], Iterable<byte[]>>> sideInput;

    ConsumeMap(PCollectionView<Map<byte[], Iterable<byte[]>>> sideInput) {
      this.sideInput = sideInput;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Map<byte[], Iterable<byte[]>> map = c.sideInput(this.sideInput);

      Iterable<byte[]> bytes = map.get(c.element().getKey());

//      if(bytes == null) {
//        // TODO: missing key
//      } else {
//        // TODO: found key
//      }
    }
  }
}
