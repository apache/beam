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
package org.apache.beam.sdk.util.construction;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.UsesJavaExpansionService;
import org.apache.beam.sdk.testing.UsesPythonExpansionService;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Runner Validation Test Suite for Cross-language Transforms.
 *
 * <p>As per Beams's Portability Framework design, Cross-language transforms should work out of the
 * box. In spite of this, there always exists a possibility of rough edges existing. It could be
 * caused due to unpolished implementation of any part of the execution code path, for example: –>
 * Transform expansion [SDK] –> Pipeline construction [SDK] –> Cross-language artifact staging
 * [Runner] –> Language specific serialization/deserialization of PCollection (and other data types)
 * [Runner/SDK]
 *
 * <p>In an effort to improve developer visibility into potential problems, this test suite
 * validates correct execution of 5 Core Beam transforms when used as cross-language transforms
 * within the Java SDK from any foreign SDK: –> ParDo
 * (https://beam.apache.org/documentation/programming-guide/#pardo) –> GroupByKey
 * (https://beam.apache.org/documentation/programming-guide/#groupbykey) –> CoGroupByKey
 * (https://beam.apache.org/documentation/programming-guide/#cogroupbykey) –> Combine
 * (https://beam.apache.org/documentation/programming-guide/#combine) –> Flatten
 * (https://beam.apache.org/documentation/programming-guide/#flatten) –> Partition
 * (https://beam.apache.org/documentation/programming-guide/#partition)
 *
 * <p>See Runner Validation Test Plan for Cross-language transforms
 * (https://docs.google.com/document/d/1xQp0ElIV84b8OCVz8CD2hvbiWdR8w4BvWxPTZJZA6NA") for further
 * details.
 */
public class ValidateRunnerXlangTest {
  static class ValidateRunnerXlangTestBase extends BaseExternalTest implements Serializable {
    // URNs for core cross-language transforms.
    // See https://docs.google.com/document/d/1xQp0ElIV84b8OCVz8CD2hvbiWdR8w4BvWxPTZJZA6NA for
    // further
    // details.
    private static final String TEST_PREFIX_URN = "beam:transforms:xlang:test:prefix";
    private static final String TEST_MULTI_URN = "beam:transforms:xlang:test:multi";
    private static final String TEST_GBK_URN = "beam:transforms:xlang:test:gbk";
    private static final String TEST_CGBK_URN = "beam:transforms:xlang:test:cgbk";
    private static final String TEST_COMGL_URN = "beam:transforms:xlang:test:comgl";
    private static final String TEST_COMPK_URN = "beam:transforms:xlang:test:compk";
    private static final String TEST_FLATTEN_URN = "beam:transforms:xlang:test:flatten";
    private static final String TEST_PARTITION_URN = "beam:transforms:xlang:test:partition";
    private static final String TEST_PYTHON_BS4_URN = "beam:transforms:xlang:test:python_bs4";

    private byte[] toStringPayloadBytes(String data) throws IOException {
      Row configRow =
          Row.withSchema(Schema.of(Field.of("data", FieldType.STRING)))
              .withFieldValue("data", data)
              .build();

      ByteStringOutputStream outputStream = new ByteStringOutputStream();
      try {
        RowCoder.of(configRow.getSchema()).encode(configRow, outputStream);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      ExternalTransforms.ExternalConfigurationPayload payload =
          ExternalTransforms.ExternalConfigurationPayload.newBuilder()
              .setSchema(SchemaTranslation.schemaToProto(configRow.getSchema(), false))
              .setPayload(outputStream.toByteString())
              .build();
      return payload.toByteArray();
    }

    protected void singleInputOutputTest(Pipeline pipeline) throws IOException {
      PCollection<String> col =
          pipeline
              .apply(Create.of("1", "2", "3"))
              .apply(External.of(TEST_PREFIX_URN, toStringPayloadBytes("0"), expansionAddr));
      PAssert.that(col).containsInAnyOrder("01", "02", "03");
    }

    protected void multiInputOutputWithSideInputTest(Pipeline pipeline) {
      PCollection<String> main1 = pipeline.apply("createMain1", Create.of("a", "bb"));
      PCollection<String> main2 = pipeline.apply("createMain2", Create.of("x", "yy", "zzz"));
      PCollection<String> side = pipeline.apply("createSide", Create.of("s"));
      PCollectionTuple pTuple =
          PCollectionTuple.of("main1", main1)
              .and("main2", main2)
              .and("side", side)
              .apply(External.of(TEST_MULTI_URN, new byte[] {}, expansionAddr).withMultiOutputs());
      PAssert.that(pTuple.get("main")).containsInAnyOrder("as", "bbs", "xs", "yys", "zzzs");
      PAssert.that(pTuple.get("side")).containsInAnyOrder("ss");
    }

    protected void groupByKeyTest(Pipeline pipeline) {
      PCollection<KV<Long, Iterable<String>>> gbkCol =
          pipeline
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

    protected void coGroupByKeyTest(Pipeline pipeline) {
      PCollection<KV<Long, String>> col1 =
          pipeline.apply("createCol1", Create.of(KV.of(0L, "1"), KV.of(0L, "2"), KV.of(1L, "3")));
      PCollection<KV<Long, String>> col2 =
          pipeline.apply("createCol2", Create.of(KV.of(0L, "4"), KV.of(1L, "5"), KV.of(1L, "6")));
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

    protected void combineGloballyTest(Pipeline pipeline) {
      PCollection<Long> col =
          pipeline
              .apply(Create.of(1L, 2L, 3L))
              .apply(External.of(TEST_COMGL_URN, new byte[] {}, expansionAddr));
      PAssert.that(col).containsInAnyOrder(6L);
    }

    protected void combinePerKeyTest(Pipeline pipeline) {
      PCollection<KV<String, Long>> col =
          pipeline
              .apply(Create.of(KV.of("a", 1L), KV.of("a", 2L), KV.of("b", 3L)))
              .apply(External.of(TEST_COMPK_URN, new byte[] {}, expansionAddr));
      PAssert.that(col).containsInAnyOrder(KV.of("a", 3L), KV.of("b", 3L));
    }

    protected void flattenTest(Pipeline pipeline) {
      PCollection<Long> col1 = pipeline.apply("createCol1", Create.of(1L, 2L, 3L));
      PCollection<Long> col2 = pipeline.apply("createCol2", Create.of(4L, 5L, 6L));
      PCollection<Long> col =
          PCollectionList.of(col1)
              .and(col2)
              .apply(External.of(TEST_FLATTEN_URN, new byte[] {}, expansionAddr));
      PAssert.that(col).containsInAnyOrder(1L, 2L, 3L, 4L, 5L, 6L);
    }

    protected void partitionTest(Pipeline pipeline) {
      PCollectionTuple col =
          pipeline
              .apply(Create.of(1L, 2L, 3L, 4L, 5L, 6L))
              .apply(
                  External.of(TEST_PARTITION_URN, new byte[] {}, expansionAddr).withMultiOutputs());
      PAssert.that(col.get("0")).containsInAnyOrder(2L, 4L, 6L);
      PAssert.that(col.get("1")).containsInAnyOrder(1L, 3L, 5L);
    }

    protected void pythonDependenciesTest(Pipeline pipeline) {
      String html =
          "<html><head><title>The Dormouse's story</title></head>\n"
              + "<body>\n"
              + "<p class=\"title\"><b>The Dormouse's story</b></p>\n"
              + "\n"
              + "<p class=\"story\">Once upon a time there were three little sisters; and their names were\n"
              + "<a href=\"http://example.com/elsie\" class=\"sister\" id=\"link1\">Elsie</a>,\n"
              + "<a href=\"http://example.com/lacie\" class=\"sister\" id=\"link2\">Lacie</a> and\n"
              + "<a href=\"http://example.com/tillie\" class=\"sister\" id=\"link3\">Tillie</a>;\n"
              + "and they lived at the bottom of a well.</p>\n"
              + "\n"
              + "<p class=\"story\">...</p>";
      PCollection<String> col =
          pipeline
              .apply(Create.of(html))
              .apply(External.of(TEST_PYTHON_BS4_URN, new byte[] {}, expansionAddr));
      PAssert.that(col).containsInAnyOrder("The Dormouse's story");
    }
  }
  /**
   * Motivation behind singleInputOutputTest.
   *
   * <p>Target transform – ParDo (https://beam.apache.org/documentation/programming-guide/#pardo)
   * Test scenario – Mapping elements from a single input collection to a single output collection
   * Boundary conditions checked – –> PCollection<?> to external transforms –> PCollection<?> from
   * external transforms
   */
  @RunWith(JUnit4.class)
  public static class SingleInputOutputTest extends ValidateRunnerXlangTestBase {
    @Test
    @Category({
      ValidatesRunner.class,
      UsesJavaExpansionService.class,
      UsesPythonExpansionService.class
    })
    public void test() throws IOException {
      singleInputOutputTest(testPipeline);
    }
  }

  /**
   * Motivation behind multiInputOutputWithSideInputTest.
   *
   * <p>Target transform – ParDo (https://beam.apache.org/documentation/programming-guide/#pardo)
   * Test scenario – Mapping elements from multiple input collections (main and side) to multiple
   * output collections (main and side) Boundary conditions checked – –> PCollectionTuple to
   * external transforms –> PCollectionTuple from external transforms
   */
  @RunWith(JUnit4.class)
  public static class MultiInputOutputWithSideInputTest extends ValidateRunnerXlangTestBase {
    @Test
    @Category({
      ValidatesRunner.class,
      UsesJavaExpansionService.class,
      UsesPythonExpansionService.class
    })
    public void test() {
      multiInputOutputWithSideInputTest(testPipeline);
    }
  }

  /**
   * Motivation behind groupByKeyTest.
   *
   * <p>Target transform – GroupByKey
   * (https://beam.apache.org/documentation/programming-guide/#groupbykey) Test scenario – Grouping
   * a collection of KV<K,V> to a collection of KV<K, Iterable<V>> by key Boundary conditions
   * checked – –> PCollection<KV<?, ?>> to external transforms –> PCollection<KV<?, Iterable<?>>>
   * from external transforms
   */
  @RunWith(JUnit4.class)
  public static class GroupByKeyTest extends ValidateRunnerXlangTestBase {
    @Test
    @Category({
      ValidatesRunner.class,
      UsesJavaExpansionService.class,
      UsesPythonExpansionService.class
    })
    public void test() {
      groupByKeyTest(testPipeline);
    }
  }

  /**
   * Motivation behind coGroupByKeyTest.
   *
   * <p>Target transform – CoGroupByKey
   * (https://beam.apache.org/documentation/programming-guide/#cogroupbykey) Test scenario –
   * Grouping multiple input collections with keys to a collection of KV<K, CoGbkResult> by key
   * Boundary conditions checked – –> KeyedPCollectionTuple<?> to external transforms –>
   * PCollection<KV<?, Iterable<?>>> from external transforms
   */
  @RunWith(JUnit4.class)
  public static class CoGroupByKeyTest extends ValidateRunnerXlangTestBase {
    @Test
    @Category({
      ValidatesRunner.class,
      UsesJavaExpansionService.class,
      UsesPythonExpansionService.class
    })
    public void test() {
      coGroupByKeyTest(testPipeline);
    }
  }

  /**
   * Motivation behind combineGloballyTest.
   *
   * <p>Target transform – Combine
   * (https://beam.apache.org/documentation/programming-guide/#combine) Test scenario – Combining
   * elements globally with a predefined simple CombineFn Boundary conditions checked – –>
   * PCollection<?> to external transforms –> PCollection<?> from external transforms
   */
  @RunWith(JUnit4.class)
  public static class CombineGloballyTest extends ValidateRunnerXlangTestBase {
    @Test
    @Category({
      ValidatesRunner.class,
      UsesJavaExpansionService.class,
      UsesPythonExpansionService.class
    })
    public void test() {
      combineGloballyTest(testPipeline);
    }
  }

  /**
   * Motivation behind combinePerKeyTest.
   *
   * <p>Target transform – Combine
   * (https://beam.apache.org/documentation/programming-guide/#combine) Test scenario – Combining
   * elements per key with a predefined simple merging function Boundary conditions checked – –>
   * PCollection<?> to external transforms –> PCollection<?> from external transforms
   */
  @RunWith(JUnit4.class)
  public static class CombinePerKeyTest extends ValidateRunnerXlangTestBase {
    @Test
    @Category({
      ValidatesRunner.class,
      UsesJavaExpansionService.class,
      UsesPythonExpansionService.class
    })
    public void test() {
      combinePerKeyTest(testPipeline);
    }
  }

  /**
   * Motivation behind flattenTest.
   *
   * <p>Target transform – Flatten
   * (https://beam.apache.org/documentation/programming-guide/#flatten) Test scenario – Merging
   * multiple collections into a single collection Boundary conditions checked – –>
   * PCollectionList<?> to external transforms –> PCollection<?> from external transforms
   */
  @RunWith(JUnit4.class)
  public static class FlattenTest extends ValidateRunnerXlangTestBase {
    @Test
    @Category({
      ValidatesRunner.class,
      UsesJavaExpansionService.class,
      UsesPythonExpansionService.class
    })
    public void test() {
      flattenTest(testPipeline);
    }
  }

  /**
   * Motivation behind partitionTest.
   *
   * <p>Target transform – Partition
   * (https://beam.apache.org/documentation/programming-guide/#partition) Test scenario – Splitting
   * a single collection into multiple collections with a predefined simple PartitionFn Boundary
   * conditions checked – –> PCollection<?> to external transforms –> PCollectionList<?> from
   * external transforms
   */
  @RunWith(JUnit4.class)
  public static class PartitionTest extends ValidateRunnerXlangTestBase {
    @Test
    @Category({
      ValidatesRunner.class,
      UsesJavaExpansionService.class,
      UsesPythonExpansionService.class
    })
    public void test() {
      partitionTest(testPipeline);
    }
  }

  @RunWith(JUnit4.class)
  public static class PythonDependenciesTest extends ValidateRunnerXlangTestBase {
    @Test
    @Category({ValidatesRunner.class, UsesPythonExpansionService.class})
    public void test() {
      pythonDependenciesTest(testPipeline);
    }
  }
}
