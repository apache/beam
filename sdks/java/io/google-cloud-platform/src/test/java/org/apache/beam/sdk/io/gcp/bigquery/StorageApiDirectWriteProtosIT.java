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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.BaseEncoding;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** An example that exports nested BigQuery record to a file. */
@RunWith(Parameterized.class)
public class StorageApiDirectWriteProtosIT {
  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return ImmutableList.of(
        new Object[] {true, false, false},
        new Object[] {false, true, false},
        new Object[] {false, false, true},
        new Object[] {true, false, true});
  }

  @Parameterized.Parameter(0)
  public boolean useStreamingExactlyOnce;

  @Parameterized.Parameter(1)
  public boolean useAtLeastOnce;

  @Parameterized.Parameter(2)
  public boolean useBatch;

  private static final BigqueryClient BQ_CLIENT =
      new BigqueryClient("StorageApiDirectWriteProtosIT");
  private static final String PROJECT =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
  private static final String BIG_QUERY_DATASET_ID =
      "storage_api_sink_direct_write_protos" + System.nanoTime();

  private BigQueryIO.Write.Method getMethod() {
    return useAtLeastOnce
        ? BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE
        : BigQueryIO.Write.Method.STORAGE_WRITE_API;
  }

  @BeforeClass
  public static void setUpTestEnvironment() throws IOException, InterruptedException {
    // Create one BQ dataset for all test cases.
    BQ_CLIENT.createNewDataset(PROJECT, BIG_QUERY_DATASET_ID);
  }

  @AfterClass
  public static void cleanup() {
    BQ_CLIENT.deleteDataset(PROJECT, BIG_QUERY_DATASET_ID);
  }

  @Test
  public void testDirectWriteProtos() throws Exception {
    Function<Integer, Proto3SchemaMessages.Primitive> getPrimitiveProto =
        (Integer i) ->
            Proto3SchemaMessages.Primitive.newBuilder()
                .setPrimitiveDouble(i)
                .setPrimitiveFloat(i)
                .setPrimitiveInt32(i)
                .setPrimitiveInt64(i)
                .setPrimitiveUint32(i)
                .setPrimitiveUint64(i)
                .setPrimitiveSint32(i)
                .setPrimitiveSint64(i)
                .setPrimitiveFixed32(i)
                .setPrimitiveFixed64(i)
                .setPrimitiveBool(true)
                .setPrimitiveString(Integer.toString(i))
                .setPrimitiveBytes(
                    ByteString.copyFrom(Integer.toString(i).getBytes(StandardCharsets.UTF_8)))
                .build();
    Function<Integer, TableRow> getPrimitiveRow =
        (Integer i) ->
            new TableRow()
                .set("primitive_double", Double.valueOf(i))
                .set("primitive_float", Float.valueOf(i).doubleValue())
                .set("primitive_int32", i.toString())
                .set("primitive_int64", i.toString())
                .set("primitive_uint32", i.toString())
                .set("primitive_uint64", i.toString())
                .set("primitive_sint32", i.toString())
                .set("primitive_sint64", i.toString())
                .set("primitive_fixed32", i.toString())
                .set("primitive_fixed64", i.toString())
                .set("primitive_bool", true)
                .set("primitive_string", i.toString())
                .set(
                    "primitive_bytes",
                    BaseEncoding.base64()
                        .encode(
                            ByteString.copyFrom(i.toString().getBytes(StandardCharsets.UTF_8))
                                .toByteArray()));

    List<Proto3SchemaMessages.Primitive> nestedItems =
        IntStream.range(1, 2)
            .mapToObj(i -> getPrimitiveProto.apply(i))
            .collect(Collectors.toList());

    Iterable<Proto3SchemaMessages.Nested> items =
        nestedItems.stream()
            .map(
                p ->
                    Proto3SchemaMessages.Nested.newBuilder()
                        .setNested(p)
                        .addAllNestedList(Lists.newArrayList(p, p, p))
                        .build())
            .collect(Collectors.toList());

    List<TableRow> expectedNestedItems =
        IntStream.range(1, 2).mapToObj(getPrimitiveRow::apply).collect(Collectors.toList());

    Iterable<TableRow> expectedItems =
        expectedNestedItems.stream()
            .map(
                p ->
                    new TableRow()
                        .set("nested_map", Lists.newArrayList())
                        .set("nested", p)
                        .set("nested_list", Lists.newArrayList(p, p, p)))
            .collect(Collectors.toList());

    String table = "table" + System.nanoTime();
    String tableSpec = PROJECT + "." + BIG_QUERY_DATASET_ID + "." + table;

    BigQueryIO.Write.Method method = getMethod();
    BigQueryIO.Write<Proto3SchemaMessages.Nested> write =
        BigQueryIO.writeProtos(Proto3SchemaMessages.Nested.class)
            .to(tableSpec)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withMethod(method);
    if (method == BigQueryIO.Write.Method.STORAGE_WRITE_API) {
      write = write.withNumStorageWriteApiStreams(1);
      if (useStreamingExactlyOnce) {
        write = write.withTriggeringFrequency(Duration.standardSeconds(1));
      }
    }

    Pipeline p = Pipeline.create();

    PCollection<Proto3SchemaMessages.Nested> input = p.apply("Create test cases", Create.of(items));
    if (useStreamingExactlyOnce) {
      input = input.setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED);
    }
    input.apply("Write using Storage Write API", write);
    p.run().waitUntilFinish();
    assertRowsWritten(tableSpec, expectedItems);
  }

  void assertRowsWritten(String tableSpec, Iterable<TableRow> expectedItems) throws Exception {
    List<TableRow> rows =
        BQ_CLIENT.queryUnflattened(
            String.format("SELECT * FROM %s", tableSpec), PROJECT, true, true);
    assertThat(rows, containsInAnyOrder(Iterables.toArray(expectedItems, TableRow.class)));
  }
}
