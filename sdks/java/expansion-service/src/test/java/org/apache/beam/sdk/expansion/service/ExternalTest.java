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
package org.apache.beam.sdk.expansion.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.io.Serializable;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesJavaExpansionService;
import org.apache.beam.sdk.testing.UsesPythonExpansionService;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.construction.External;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ServerBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test External transforms. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class ExternalTest implements Serializable {
  @Rule public transient TestPipeline testPipeline = TestPipeline.create();
  private @MonotonicNonNull PipelineResult pipelineResult = null;

  private static final String TEST_URN_SIMPLE = "simple";
  private static final String TEST_URN_LE = "le";
  private static final String TEST_URN_MULTI = "multi";

  private static @MonotonicNonNull String localExpansionAddr = null;
  private static @MonotonicNonNull Server localExpansionServer = null;

  @BeforeClass
  public static void setUpClass() throws IOException {
    int localExpansionPort;
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      localExpansionPort = socket.getLocalPort();
    }
    localExpansionAddr = String.format("localhost:%s", localExpansionPort);

    localExpansionServer =
        ServerBuilder.forPort(localExpansionPort).addService(new ExpansionService()).build();
    localExpansionServer.start();
  }

  @AfterClass
  @RequiresNonNull("localExpansionServer")
  public static void tearDownClass() {
    localExpansionServer.shutdownNow();
  }

  @After
  @RequiresNonNull("pipelineResult")
  public void tearDown() {
    pipelineResult.waitUntilFinish();
    assertThat(pipelineResult.getState(), equalTo(PipelineResult.State.DONE));
  }

  @Test
  @Category({
    ValidatesRunner.class,
    UsesJavaExpansionService.class,
    UsesPythonExpansionService.class
  })
  @RequiresNonNull("localExpansionAddr")
  public void expandSingleTest() {
    PCollection<String> col =
        testPipeline
            .apply(Create.of("1", "2", "3"))
            .apply(External.of(TEST_URN_SIMPLE, new byte[] {}, localExpansionAddr));
    PAssert.that(col).containsInAnyOrder("11", "22", "33");
    pipelineResult = testPipeline.run();
  }

  @Test
  @Category({
    ValidatesRunner.class,
    UsesJavaExpansionService.class,
    UsesPythonExpansionService.class
  })
  @RequiresNonNull("localExpansionAddr")
  public void expandMultipleTest() {
    PCollection<String> pcol =
        testPipeline
            .apply(Create.of(1, 2, 3, 4, 5, 6))
            .apply(
                "filter <=3",
                External.of(TEST_URN_LE, "3".getBytes(StandardCharsets.UTF_8), localExpansionAddr))
            .apply(MapElements.into(TypeDescriptors.strings()).via(Object::toString))
            .apply("put simple", External.of(TEST_URN_SIMPLE, new byte[] {}, localExpansionAddr));

    PAssert.that(pcol).containsInAnyOrder("11", "22", "33");
    pipelineResult = testPipeline.run();
  }

  @Test
  @Category({
    ValidatesRunner.class,
    UsesJavaExpansionService.class,
    UsesPythonExpansionService.class
  })
  @RequiresNonNull("localExpansionAddr")
  public void expandMultiOutputTest() {
    PCollectionTuple pTuple =
        testPipeline
            .apply(Create.of(1, 2, 3, 4, 5, 6))
            .apply(
                External.of(TEST_URN_MULTI, new byte[] {}, localExpansionAddr).withMultiOutputs());

    PAssert.that(pTuple.get(new TupleTag<Integer>("even") {})).containsInAnyOrder(2, 4, 6);
    PAssert.that(pTuple.get(new TupleTag<Integer>("odd") {})).containsInAnyOrder(1, 3, 5);
    pipelineResult = testPipeline.run();
  }

  /** Test TransformProvider. */
  @AutoService(ExpansionService.ExpansionServiceRegistrar.class)
  public static class TestTransforms
      implements ExpansionService.ExpansionServiceRegistrar, Serializable {
    private final TupleTag<Integer> even = new TupleTag<Integer>("even") {};
    private final TupleTag<Integer> odd = new TupleTag<Integer>("odd") {};

    @Override
    public Map<String, TransformProvider> knownTransforms() {
      return ImmutableMap.of(
          TEST_URN_SIMPLE,
          (spec, options) -> MapElements.into(TypeDescriptors.strings()).via((String x) -> x + x),
          TEST_URN_LE,
          (spec, options) -> Filter.lessThanEq(Integer.parseInt(spec.getPayload().toStringUtf8())),
          TEST_URN_MULTI,
          (spec, options) ->
              ParDo.of(
                      new DoFn<Integer, Integer>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                          if (c.element() % 2 == 0) {
                            c.output(c.element());
                          } else {
                            c.output(odd, c.element());
                          }
                        }
                      })
                  .withOutputTags(even, TupleTagList.of(odd)));
    }
  }
}
