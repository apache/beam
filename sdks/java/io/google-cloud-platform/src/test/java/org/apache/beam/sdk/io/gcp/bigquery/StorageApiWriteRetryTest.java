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
import static org.hamcrest.Matchers.hasItems;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Regression tests for row bookkeeping across Storage Write API retries. */
@RunWith(Parameterized.class)
public class StorageApiWriteRetryTest {
  @Parameters(name = "method={0}, streaming={1}")
  public static Iterable<Object[]> parameters() {
    return ImmutableList.of(
        new Object[] {Method.STORAGE_WRITE_API, false},
        new Object[] {Method.STORAGE_WRITE_API, true},
        new Object[] {Method.STORAGE_API_AT_LEAST_ONCE, false});
  }

  @Parameter public Method method;

  @Parameter(1)
  public boolean streaming;

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final List<Integer> APPEND_SIZES = Collections.synchronizedList(new ArrayList<>());

  @Before
  public void setUp() throws Exception {
    FakeDatasetService.setUp();
    BigQueryIO.clearStaticCaches();
    APPEND_SIZES.clear();
    pipeline.getOptions().as(BigQueryOptions.class).setProject("project-id");
    pipeline.getOptions().as(DirectOptions.class).setTargetParallelism(1);
  }

  @After
  public void tearDown() throws Exception {
    BigQueryIO.clearStaticCaches();
  }

  @Test
  public void testConsecutiveSerializationErrorsPreserveOriginalRows() throws Exception {
    TableReference table =
        new TableReference()
            .setProjectId("project-id")
            .setDatasetId("dataset-id")
            .setTableId("table-id");
    TableSchema schema =
        new TableSchema()
            .setFields(ImmutableList.of(new TableFieldSchema().setName("id").setType("STRING")));
    FakeDatasetService dataset = new RejectOneRowPerAppend();
    dataset.createDataset("project-id", "dataset-id", "", "", null);
    dataset.createTable(new Table().setTableReference(table).setSchema(schema));

    PCollection<TableRow> rows;
    if (streaming) {
      rows =
          pipeline.apply(
              TestStream.create(TableRowJsonCoder.of())
                  .addElements(row("A"), row("B"), row("C"))
                  .advanceProcessingTime(Duration.standardSeconds(2))
                  .advanceWatermarkToInfinity());
    } else {
      // Emit all rows from one element so the unsharded writer receives one bundle.
      rows =
          pipeline
              .apply(Create.of(0))
              .apply(
                  FlatMapElements.into(TypeDescriptor.of(TableRow.class))
                      .via(ignored -> ImmutableList.of(row("A"), row("B"), row("C"))))
              .setCoder(TableRowJsonCoder.of());
    }

    BigQueryIO.Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .to(table)
            .withMethod(method)
            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
            .withFormatRecordOnFailureFunction(
                input -> new TableRow().set("original_id", input.get("id")))
            .withPropagateSuccessfulStorageApiWrites(true)
            .withTestServices(
                new FakeBigQueryServices()
                    .withDatasetService(dataset)
                    .withJobService(new FakeJobService()))
            .withoutValidation();
    if (streaming) {
      write =
          write
              .withNumStorageWriteApiStreams(1)
              .withTriggeringFrequency(Duration.standardSeconds(1));
    } else {
      write = write.withNumStorageWriteApiStreams(0);
    }
    WriteResult result = rows.apply(write);
    PAssert.that(
            result
                .getFailedStorageApiInserts()
                .apply(
                    MapElements.into(TypeDescriptor.of(TableRow.class))
                        .via(BigQueryStorageApiInsertError::getRow)))
        .containsInAnyOrder(
            new TableRow().set("original_id", "A"), new TableRow().set("original_id", "B"));
    PAssert.that(result.getSuccessfulStorageApiInserts()).containsInAnyOrder(row("C"));

    pipeline.run().waitUntilFinish();

    assertThat(
        dataset.getAllRows("project-id", "dataset-id", "table-id"), containsInAnyOrder(row("C")));
    // Ensure this exercised successive retries of the same multirow batch, not singleton writes.
    assertThat(APPEND_SIZES, hasItems(3, 2, 1));
  }

  private static TableRow row(String id) {
    return new TableRow().set("id", id);
  }

  private static class RejectOneRowPerAppend extends FakeDatasetService {
    @Override
    public BigQueryServices.StreamAppendClient getStreamAppendClient(
        String streamName,
        DescriptorProtos.DescriptorProto descriptor,
        boolean useConnectionPool,
        AppendRowsRequest.MissingValueInterpretation missingValueInterpretation)
        throws Exception {
      BigQueryServices.StreamAppendClient delegate =
          super.getStreamAppendClient(
              streamName, descriptor, useConnectionPool, missingValueInterpretation);
      Descriptor protoDescriptor = TableRowToStorageApiProto.wrapDescriptorProto(descriptor);
      return new BigQueryServices.StreamAppendClient() {
        @Override
        public ApiFuture<AppendRowsResponse> appendRows(long offset, ProtoRows rows)
            throws Exception {
          APPEND_SIZES.add(rows.getSerializedRowsCount());
          try {
            for (int i = 0; i < rows.getSerializedRowsCount(); i++) {
              String id =
                  (String)
                      DynamicMessage.parseFrom(protoDescriptor, rows.getSerializedRows(i))
                          .getField(protoDescriptor.findFieldByName("id"));
              if (!id.equals("C")) {
                return ApiFutures.immediateFailedFuture(
                    new Exceptions.AppendSerializationError(
                        400, "Invalid row", streamName, ImmutableMap.of(i, "Invalid id: " + id)));
              }
            }
          } catch (Exception e) {
            return ApiFutures.immediateFailedFuture(e);
          }
          return delegate.appendRows(offset, rows);
        }

        @Override
        public com.google.cloud.bigquery.storage.v1.@Nullable TableSchema getUpdatedSchema() {
          return delegate.getUpdatedSchema();
        }

        @Override
        public void pin() {
          delegate.pin();
        }

        @Override
        public void unpin() {
          delegate.unpin();
        }

        @Override
        public void close() throws Exception {
          delegate.close();
        }
      };
    }
  }
}
