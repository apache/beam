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
package org.apache.beam.sdk.extensions.python;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesCrossLanguageTransforms;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.grpc.v1p43p2.io.grpc.ConnectivityState;
import org.apache.beam.vendor.grpc.v1p43p2.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p43p2.io.grpc.ManagedChannelBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PythonExternalTransformIT {

  @Rule public transient TestPipeline testPipeline = TestPipeline.create();
  private PipelineResult pipelineResult;

  private static String expansionAddr;
  private static String expansionJar;

  @BeforeClass
  public static void setUpClass() {
    expansionAddr =
        String.format("localhost:%s", Integer.valueOf(System.getProperty("expansionPort")));
    expansionJar = System.getProperty("expansionJar");
  }

  @Before
  public void setUp() {
    ExperimentalOptions.addExperiment(
        testPipeline.getOptions().as(ExperimentalOptions.class), "jar_packages=" + expansionJar);
    waitForReady();
  }

  @After
  public void tearDown() {
    pipelineResult = testPipeline.run();
    pipelineResult.waitUntilFinish();
    assertThat(pipelineResult.getState(), equalTo(PipelineResult.State.DONE));
  }

  private void waitForReady() {
    try {
      ManagedChannel channel = ManagedChannelBuilder.forTarget(expansionAddr).build();
      ConnectivityState state = channel.getState(true);
      for (int retry = 0; retry < 30 && state != ConnectivityState.READY; retry++) {
        Thread.sleep(500);
        state = channel.getState(true);
      }
      channel.shutdownNow();
    } catch (InterruptedException e) {
      throw new RuntimeException("interrupted.");
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesCrossLanguageTransforms.class})
  public void trivialPythonTransform() {
    PCollection<String> output =
        testPipeline
            .apply(Create.of(KV.of("A", "x"), KV.of("A", "y"), KV.of("B", "z")))
            .apply(
                PythonExternalTransform
                    .<PCollection<KV<String, String>>, PCollection<KV<String, Iterable<String>>>>
                        from("apache_beam.GroupByKey"))
            .apply(MapElements.into(TypeDescriptors.strings()).via(kv -> kv.getKey()));
    PAssert.that(output).containsInAnyOrder("A", "B");
  }
}
