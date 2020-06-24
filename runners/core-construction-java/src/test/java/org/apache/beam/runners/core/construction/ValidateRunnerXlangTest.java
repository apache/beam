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
package org.apache.beam.runners.core.construction;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesCrossLanguageTransforms;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.ConnectivityState;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.ManagedChannelBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 *
 * <h1>Runner Validation Test Suite for Cross-language Transforms</h1>
 *
 * As per Beams's Portability Framework design, Cross-language transforms should work out of the
 * box. In spite of this, there always exists a possibility of rough edges existing. It could be
 * caused due to unpolished implementation of any part of the execution code path, for example:
 *
 * <ul>
 *   <li>Transform expansion [SDK]
 *   <li>Pipeline construction [SDK]
 *   <li>Cross-language artifact staging [Runner]
 *   <li>Language specific serialization/deserialization of PCollection (and other data types)
 *       [Runner/SDK]
 * </ul>
 *
 * In an effort to improve developer visibility into potential problems, this test suite validates a
 * cross-language runner against <i>5 Core Beam transforms</i> from any foreign language — {@link
 * ParDo}, {@link GroupByKey}, {@link CoGroupByKey}, {@link Combine}, {@link Flatten} and {@link
 * Partition}.
 *
 * <p>See <a
 * href="https://docs.google.com/document/d/1xQp0ElIV84b8OCVz8CD2hvbiWdR8w4BvWxPTZJZA6NA">Runner
 * Validation Test Plan for Cross-language transforms</a>for further details.
 */
@RunWith(JUnit4.class)
public class ValidateRunnerXlangTest implements Serializable {
  @Rule public transient TestPipeline testPipeline = TestPipeline.create();
  private PipelineResult pipelineResult;

  // URNs for core cross-language transforms.
  private static final String TEST_PREFIX_URN = "beam:transforms:xlang:test:prefix";
  private static final String TEST_MULTI_URN = "beam:transforms:xlang:test:multi";
  private static final String TEST_GBK_URN = "beam:transforms:xlang:test:gbk";
  private static final String TEST_CGBK_URN = "beam:transforms:xlang:test:cgbk";
  private static final String TEST_COMGL_URN = "beam:transforms:xlang:test:comgl";
  private static final String TEST_COMPK_URN = "beam:transforms:xlang:test:compk";
  private static final String TEST_FLATTEN_URN = "beam:transforms:xlang:test:flatten";
  private static final String TEST_PARTITION_URN = "beam:transforms:xlang:test:partition";

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

  /**
   *
   *
   * <ul>
   *   <li><b>Target transform</b> – {@link ParDo}
   *   <li><b>Test scenario</b> – Mapping elements from a single input collection to a single output
   *       collection
   *   <li><b>Boundary conditions checked</b> –
   *       <ul>
   *         <li>PCollection<?> to external transforms
   *         <li>PCollection<?> from external transforms
   *       </ul>
   * </ul>
   */
  @Test
  @Category({ValidatesRunner.class, UsesCrossLanguageTransforms.class})
  public void singleInputOutputTest() throws IOException {
    PCollection<String> col =
        testPipeline
            .apply(Create.of("1", "2", "3"))
            .apply(External.of(TEST_PREFIX_URN, toStringPayloadBytes("0"), expansionAddr));
    PAssert.that(col).containsInAnyOrder("01", "02", "03");
  }

  /**
   *
   *
   * <ul>
   *   <li><b>Target transform</b> – {@link ParDo}
   *   <li><b>Test scenario</b> – Mapping elements from multiple input collections (main and side)
   *       to multiple output collections (main and side)
   *   <li><b>Boundary conditions checked</b> –
   *       <ul>
   *         <li>PCollectionTuple to external transforms
   *         <li>PCollectionTuple from external transforms
   *       </ul>
   * </ul>
   */
  @Test
  @Category({ValidatesRunner.class, UsesCrossLanguageTransforms.class})
  public void multiInputOutputWithSideInputTest() {
    PCollection<String> main1 = testPipeline.apply("createMain1", Create.of("a", "bb"));
    PCollection<String> main2 = testPipeline.apply("createMain2", Create.of("x", "yy", "zzz"));
    PCollection<String> side = testPipeline.apply("createSide", Create.of("s"));
    PCollectionTuple pTuple =
        PCollectionTuple.of("main1", main1)
            .and("main2", main2)
            .and("side", side)
            .apply(External.of(TEST_MULTI_URN, new byte[] {}, expansionAddr).withMultiOutputs());
    PAssert.that(pTuple.get("main")).containsInAnyOrder("as", "bbs", "xs", "yys", "zzzs");
    PAssert.that(pTuple.get("side")).containsInAnyOrder("ss");
  }

  /**
   *
   *
   * <ul>
   *   <li><b>Target transform</b> – {@link GroupByKey}
   *   <li><b>Test scenario</b> – Grouping a collection of KV&lt;K,V&gt; to a collection of KV&lt;K,
   *       Iterable&lt; V&gt;&gt; by key
   *   <li><b>Boundary conditions checked</b> –
   *       <ul>
   *         <li>PCollection&lt;KV&lt;?, ?&gt;&gt; to external transforms
   *         <li>PCollection&lt;KV&lt;?, Iterable&lt;?&gt;&gt;&gt; from external transforms
   *       </ul>
   * </ul>
   */
  @Test
  @Category({ValidatesRunner.class, UsesCrossLanguageTransforms.class})
  public void groupByKeyTest() {
    PCollection<KV<Long, Iterable<String>>> gbkCol =
        testPipeline
            .apply(Create.of(KV.of(0L, "1"), KV.of(0L, "2"), KV.of(1L, "3")))
            .apply(External.of(TEST_GBK_URN, new byte[] {}, expansionAddr));
    PCollection<String> col =
        gbkCol.apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    (KV<Long, Iterable<String>> kv) -> {
                      String[] values = Iterables.toArray(kv.getValue(), String.class);
                      Arrays.sort(values);
                      return String.format("%s:%s", kv.getKey(), String.join(",", values));
                    }));
    PAssert.that(col).containsInAnyOrder("0:1,2", "1:3");
  }

  /**
   *
   *
   * <ul>
   *   <li><b>Target transform</b> – {@link CoGroupByKey}
   *   <li><b>Test scenario</b> – Grouping multiple input collections with keys to a collection of
   *       KV&lt;K, CoGbkResult&gt; by key
   *   <li><b>Boundary conditions checked</b> –
   *       <ul>
   *         <li>KeyedPCollectionTuple<?> to external transforms
   *         <li>PCollection&lt;KV&lt;?, Iterable&lt;?&gt;&gt;&gt; from external transforms
   *       </ul>
   * </ul>
   */
  @Test
  @Category({ValidatesRunner.class, UsesCrossLanguageTransforms.class})
  public void coGroupByKeyTest() {
    PCollection<KV<Long, String>> col1 =
        testPipeline.apply("createCol1", Create.of(KV.of(0L, "1"), KV.of(0L, "2"), KV.of(1L, "3")));
    PCollection<KV<Long, String>> col2 =
        testPipeline.apply("createCol2", Create.of(KV.of(0L, "4"), KV.of(1L, "5"), KV.of(1L, "6")));
    PCollection<KV<Long, Iterable<String>>> cgbkCol =
        KeyedPCollectionTuple.of("col1", col1)
            .and("col2", col2)
            .apply(External.of(TEST_CGBK_URN, new byte[] {}, expansionAddr));
    PCollection<String> col =
        cgbkCol.apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    (KV<Long, Iterable<String>> kv) -> {
                      String[] values = Iterables.toArray(kv.getValue(), String.class);
                      Arrays.sort(values);
                      return String.format("%s:%s", kv.getKey(), String.join(",", values));
                    }));
    PAssert.that(col).containsInAnyOrder("0:1,2,4", "1:3,5,6");
  }

  /**
   *
   *
   * <ul>
   *   <li><b>Target transform</b> – {@link Combine}
   *   <li><b>Test scenario</b> – Combining elements globally with a predefined simple CombineFn
   *   <li><b>Boundary conditions checked</b> –
   *       <ul>
   *         <li>PCollection<?> to external transforms
   *         <li>PCollection<?> from external transforms
   *       </ul>
   * </ul>
   */
  @Test
  @Category({ValidatesRunner.class, UsesCrossLanguageTransforms.class})
  public void combineGloballyTest() {
    PCollection<Long> col =
        testPipeline
            .apply(Create.of(1L, 2L, 3L))
            .apply(External.of(TEST_COMGL_URN, new byte[] {}, expansionAddr));
    PAssert.that(col).containsInAnyOrder(6L);
  }

  /**
   *
   *
   * <ul>
   *   <li><b>Target transform</b> – {@link Combine}
   *   <li><b>Test scenario</b> – Combining elements per key with a predefined simple merging
   *       function
   *   <li><b>Boundary conditions checked</b> –
   *       <ul>
   *         <li>PCollection<?> to external transforms
   *         <li>PCollection<?> from external transforms
   *       </ul>
   * </ul>
   */
  @Test
  @Category({ValidatesRunner.class, UsesCrossLanguageTransforms.class})
  public void combinePerKeyTest() {
    PCollection<KV<String, Long>> col =
        testPipeline
            .apply(Create.of(KV.of("a", 1L), KV.of("a", 2L), KV.of("b", 3L)))
            .apply(External.of(TEST_COMPK_URN, new byte[] {}, expansionAddr));
    PAssert.that(col).containsInAnyOrder(KV.of("a", 3L), KV.of("b", 3L));
  }

  /**
   *
   *
   * <ul>
   *   <li><b>Target transform</b> – {@link Flatten}
   *   <li><b>Test scenario</b> – Merging multiple collections into a single collection
   *   <li><b>Boundary conditions checked</b> –
   *       <ul>
   *         <li>PCollectionList<?> to external transforms
   *         <li>PCollection<?> from external transforms
   *       </ul>
   * </ul>
   */
  @Test
  @Category({ValidatesRunner.class, UsesCrossLanguageTransforms.class})
  public void flattenTest() {
    PCollection<Long> col1 = testPipeline.apply("createCol1", Create.of(1L, 2L, 3L));
    PCollection<Long> col2 = testPipeline.apply("createCol2", Create.of(4L, 5L, 6L));
    PCollection<Long> col =
        PCollectionList.of(col1)
            .and(col2)
            .apply(External.of(TEST_FLATTEN_URN, new byte[] {}, expansionAddr));
    PAssert.that(col).containsInAnyOrder(1L, 2L, 3L, 4L, 5L, 6L);
  }

  /**
   *
   *
   * <ul>
   *   <li><b>Target transform</b> – {@link Partition}
   *   <li><b>Test scenario</b> – Splitting a single collection into multiple collections with a
   *       predefined simple PartitionFn
   *   <li><b>Boundary conditions checked</b> –
   *       <ul>
   *         <li>PCollection<?> to external transforms
   *         <li>PCollectionList<?> from external transforms
   *       </ul>
   * </ul>
   */
  @Test
  @Category({ValidatesRunner.class, UsesCrossLanguageTransforms.class})
  public void partitionTest() {
    PCollectionTuple col =
        testPipeline
            .apply(Create.of(1L, 2L, 3L, 4L, 5L, 6L))
            .apply(
                External.of(TEST_PARTITION_URN, new byte[] {}, expansionAddr).withMultiOutputs());
    PAssert.that(col.get("0")).containsInAnyOrder(2L, 4L, 6L);
    PAssert.that(col.get("1")).containsInAnyOrder(1L, 3L, 5L);
  }

  private byte[] toStringPayloadBytes(String data) throws IOException {
    ExternalTransforms.ExternalConfigurationPayload payload =
        ExternalTransforms.ExternalConfigurationPayload.newBuilder()
            .putConfiguration(
                "data",
                ExternalTransforms.ConfigValue.newBuilder()
                    .addCoderUrn("beam:coder:string_utf8:v1")
                    .setPayload(ByteString.copyFrom(encodeString(data)))
                    .build())
            .build();
    return payload.toByteArray();
  }

  private static byte[] encodeString(String str) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    StringUtf8Coder.of().encode(str, baos);
    return baos.toByteArray();
  }
}
