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
package org.apache.beam.runners.dataflow;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import org.apache.beam.runners.dataflow.testing.TestDataflowPipelineOptions;
import org.apache.beam.runners.dataflow.testing.TestDataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests that the Dataflow runner accepts various --XLocation options with and without trailing '/'.
 */
@RunWith(JUnit4.class)
public class DataflowLocationIT {

  /**
   * Builds and runs the test using the given {@link Pipeline} We have to invoke the code in this
   * awkward way so that the calling test can set options after the {@link TestDataflowRunner} has
   * set them.
   *
   * @see TestDataflowRunner#fromOptions(org.apache.beam.sdk.options.PipelineOptions)
   */
  private static void runTestPipeline(Pipeline p) {
    PCollection<Integer> ints = p.apply(Create.of(1, 2, 3));

    final PCollectionView<Integer> singletonSum =
        ints.apply(Sum.integersGlobally())
            .apply(View.<Integer>asSingleton());

    PCollection<String> sumPlusValue =
        ints.apply(ParDo.of(new DoFn<Integer, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            Integer sumPlusValue = c.element() + c.sideInput(singletonSum);
            c.output(sumPlusValue.toString());
          }
        }).withSideInputs(singletonSum));

    PAssert.that(sumPlusValue).containsInAnyOrder("7", "8", "9");

    p.run().waitUntilFinish();
  }

  private static String ensureSlash(String input) {
    if (!input.endsWith("/")) {
      return input + '/';
    }
    return input;
  }

  private static String ensureNoSlash(String input) {
    if (input.endsWith("/")) {
      return input.substring(0, input.length() - 1);
    }
    return input;
  }

  @Test
  public void testWithTrailingSlash() throws Exception {
    TestDataflowPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(TestDataflowPipelineOptions.class);
    Pipeline p = Pipeline.create(options);

    options.setTempLocation(ensureSlash(options.getTempLocation()));
    options.setGcpTempLocation(ensureSlash(options.getGcpTempLocation()));
    options.setStagingLocation(ensureSlash(options.getStagingLocation()));
    runTestPipeline(p);

    // Assert that the options were not changed, at least visibly.
    assertThat(options.getTempLocation(), equalTo(ensureSlash(options.getTempLocation())));
    assertThat(options.getGcpTempLocation(), equalTo(ensureSlash(options.getGcpTempLocation())));
    assertThat(options.getStagingLocation(), equalTo(ensureSlash(options.getStagingLocation())));
  }

  @Test
  public void testWithNoTrailingSlash() throws Exception {
    TestDataflowPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(TestDataflowPipelineOptions.class);
    Pipeline p = Pipeline.create(options);

    options.setTempLocation(ensureNoSlash(options.getTempLocation()));
    options.setGcpTempLocation(ensureNoSlash(options.getGcpTempLocation()));
    options.setStagingLocation(ensureNoSlash(options.getStagingLocation()));
    runTestPipeline(p);

    // Assert that the options were not changed, at least visibly.
    assertThat(options.getTempLocation(), equalTo(ensureNoSlash(options.getTempLocation())));
    assertThat(options.getGcpTempLocation(), equalTo(ensureNoSlash(options.getGcpTempLocation())));
    assertThat(options.getStagingLocation(), equalTo(ensureNoSlash(options.getStagingLocation())));
  }
}
