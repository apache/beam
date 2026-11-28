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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

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
import com.google.cloud.bigquery.storage.v1.StorageError;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.StorageApiDynamicDestinations.MessageConverter;
import org.apache.beam.sdk.io.gcp.bigquery.StorageApiWriteUnshardedRecords.WriteRecordsDoFnImpl;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Regression tests for error reporting and {@code _default} stream offset handling in {@link
 * StorageApiWriteUnshardedRecords}.
 */
@RunWith(JUnit4.class)
public class StorageApiWriteUnshardedRecordsErrorTest {
  private static final String PROJECT_ID = "project-id";
  private static final String DATASET_ID = "dataset-id";
  private static final String TABLE_ID = "table-id";
  private static final String TABLE_SPEC = PROJECT_ID + ":" + DATASET_ID + "." + TABLE_ID;

  private static final TableReference TABLE_REF =
      new TableReference().setProjectId(PROJECT_ID).setDatasetId(DATASET_ID).setTableId(TABLE_ID);

  private static final TableSchema TABLE_SCHEMA =
      new TableSchema()
          .setFields(ImmutableList.of(new TableFieldSchema().setName("id").setType("STRING")));

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Before
  public void setUp() throws Exception {
    FakeDatasetService.setUp();
    BigQueryIO.clearStaticCaches();
    pipeline.getOptions().as(BigQueryOptions.class).setProject(PROJECT_ID);
    pipeline.getOptions().as(DirectOptions.class).setTargetParallelism(1);
  }

  @After
  public void tearDown() throws Exception {
    BigQueryIO.clearStaticCaches();
  }

  private FakeDatasetService createDatasetWithTable() throws Exception {
    FakeDatasetService dataset = new FakeDatasetService();
    dataset.createDataset(PROJECT_ID, DATASET_ID, "", "", null);
    dataset.createTable(new Table().setTableReference(TABLE_REF).setSchema(TABLE_SCHEMA));
    return dataset;
  }

  private PipelineExecutionException runWriteExpectingFailure(
      FakeDatasetService dataset, Method method) {
    pipeline
        .apply(Create.of(ImmutableList.of(row("first"))).withCoder(TableRowJsonCoder.of()))
        .apply(
            BigQueryIO.writeTableRows()
                .to(TABLE_REF)
                .withMethod(method)
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withNumStorageWriteApiStreams(0)
                .withoutValidation()
                .withTestServices(
                    new FakeBigQueryServices()
                        .withDatasetService(dataset)
                        .withJobService(new FakeJobService())));

    return assertThrows(PipelineExecutionException.class, () -> pipeline.run().waitUntilFinish());
  }

  @Test
  public void testDefaultStreamOutOfRangePreservesRootCause() throws Exception {
    FakeDatasetService dataset = createDatasetWithTable();
    dataset.setAppendRowsError(
        new StatusRuntimeException(Status.OUT_OF_RANGE.withDescription("Partition out of range")));

    PipelineExecutionException failure =
        runWriteExpectingFailure(dataset, Method.STORAGE_API_AT_LEAST_ONCE);

    Throwable appendFailure = failure.getCause();
    assertThat(appendFailure.getMessage(), containsString("Status Code OUT_OF_RANGE"));
    assertThat(appendFailure.getMessage(), containsString("Partition out of range"));
    assertThat(appendFailure.getMessage(), not(containsString("invalid offset")));
    assertTrue(
        Throwables.getCausalChain(appendFailure).stream()
            .anyMatch(
                t ->
                    t instanceof StatusRuntimeException
                        && ((StatusRuntimeException) t).getStatus().getCode()
                            == Status.Code.OUT_OF_RANGE));
  }

  @Test
  public void testDefaultStreamAlreadyExistsPreservesRootCause() throws Exception {
    FakeDatasetService dataset = createDatasetWithTable();
    dataset.setAppendRowsError(
        new StatusRuntimeException(Status.ALREADY_EXISTS.withDescription("Entity already exists")));

    PipelineExecutionException failure =
        runWriteExpectingFailure(dataset, Method.STORAGE_API_AT_LEAST_ONCE);

    Throwable appendFailure = failure.getCause();
    assertThat(appendFailure.getMessage(), containsString("Status Code ALREADY_EXISTS"));
    assertThat(appendFailure.getMessage(), containsString("Entity already exists"));
    assertThat(appendFailure.getMessage(), not(containsString("invalid offset")));
    assertTrue(
        Throwables.getCausalChain(appendFailure).stream()
            .anyMatch(
                t ->
                    t instanceof StatusRuntimeException
                        && ((StatusRuntimeException) t).getStatus().getCode()
                            == Status.Code.ALREADY_EXISTS));
  }

  @Test
  public void testPendingStreamOffsetOutOfRangePreservesRootCause() throws Exception {
    FakeDatasetService dataset = createDatasetWithTable();
    dataset.setAppendRowsStorageError(
        StorageError.StorageErrorCode.OFFSET_OUT_OF_RANGE, "expected offset 10, received 0", 1);

    PipelineExecutionException failure =
        runWriteExpectingFailure(dataset, Method.STORAGE_WRITE_API);

    Throwable appendFailure = failure.getCause();
    assertThat(appendFailure.getMessage(), containsString("invalid offset of 0"));
    assertThat(appendFailure.getMessage(), containsString("expected offset 10, received 0"));
    assertTrue(
        Throwables.getCausalChain(appendFailure).stream()
            .anyMatch(Exceptions.OffsetOutOfRange.class::isInstance));
  }

  @Test
  public void testPendingStreamOffsetAlreadyExistsPreservesRootCause() throws Exception {
    FakeDatasetService dataset = createDatasetWithTable();
    dataset.setAppendRowsStorageError(
        StorageError.StorageErrorCode.OFFSET_ALREADY_EXISTS, "expected offset 5, received 0", 1);

    PipelineExecutionException failure =
        runWriteExpectingFailure(dataset, Method.STORAGE_WRITE_API);

    Throwable appendFailure = failure.getCause();
    assertThat(appendFailure.getMessage(), containsString("invalid offset of 0"));
    assertThat(appendFailure.getMessage(), containsString("expected offset 5, received 0"));
    assertTrue(
        Throwables.getCausalChain(appendFailure).stream()
            .anyMatch(Exceptions.OffsetAlreadyExists.class::isInstance));
  }

  @Test
  public void testPendingStreamNonOffsetOutOfRangeDoesNotReportInvalidOffset() throws Exception {
    FakeDatasetService dataset = createDatasetWithTable();
    dataset.setAppendRowsError(
        new StatusRuntimeException(Status.OUT_OF_RANGE.withDescription("Partition out of range")));

    PipelineExecutionException failure =
        runWriteExpectingFailure(dataset, Method.STORAGE_WRITE_API);

    Throwable appendFailure = failure.getCause();
    assertThat(appendFailure.getMessage(), containsString("Status Code OUT_OF_RANGE"));
    assertThat(appendFailure.getMessage(), containsString("Partition out of range"));
    assertThat(appendFailure.getMessage(), not(containsString("invalid offset")));
    assertTrue(
        Throwables.getCausalChain(appendFailure).stream()
            .anyMatch(
                t ->
                    t instanceof StatusRuntimeException
                        && ((StatusRuntimeException) t).getStatus().getCode()
                            == Status.Code.OUT_OF_RANGE));
  }

  @Test
  public void testDefaultStreamAppendSerializationErrorKeepsUnsetOffsetsAcrossQueuedContexts()
      throws Exception {
    List<Long> observedOffsets = Collections.synchronizedList(new ArrayList<>());
    FakeDatasetService dataset = new RecordingRejectBadRowDatasetService(observedOffsets);
    dataset.createDataset(PROJECT_ID, DATASET_ID, "", "", null);
    dataset.createTable(new Table().setTableReference(TABLE_REF).setSchema(TABLE_SCHEMA));

    List<BigQueryStorageApiInsertError> failedRows =
        runMultiContextSerializationRetry(dataset, true);

    assertThat(observedOffsets, contains(-1L, -1L, -1L, -1L));
    assertEquals(1, failedRows.size());
    assertEquals("bad", failedRows.get(0).getRow().get("id"));
    // _default streams are at-least-once: batch 2 succeeds on the initial pass and is replayed by
    // RETRY_ALL_OPERATIONS after batch 1's AppendSerializationError, always with unset offset -1.
    assertThat(
        dataset.getAllRows(PROJECT_ID, DATASET_ID, TABLE_ID),
        containsInAnyOrder(row("good1"), row("good2"), row("good3"), row("good3")));
  }

  @Test
  public void testPendingStreamAppendSerializationErrorUpdatesOffsetsAcrossQueuedContexts()
      throws Exception {
    List<Long> observedOffsets = Collections.synchronizedList(new ArrayList<>());
    FakeDatasetService dataset = new RecordingRejectBadRowDatasetService(observedOffsets);
    dataset.createDataset(PROJECT_ID, DATASET_ID, "", "", null);
    dataset.createTable(new Table().setTableReference(TABLE_REF).setSchema(TABLE_SCHEMA));

    List<BigQueryStorageApiInsertError> failedRows =
        runMultiContextSerializationRetry(dataset, false);

    // Initial attempt: batch 1 at offset 0 (3 rows, 1 rejected -> fails with
    // AppendSerializationError), batch 2 at offset 3 (1 row).
    // Retry after removing bad row: batch 1 at offset 0 (2 rows), batch 2 updated to offset 2.
    assertThat(observedOffsets, contains(0L, 3L, 0L, 2L));
    assertEquals(1, failedRows.size());
    assertEquals("bad", failedRows.get(0).getRow().get("id"));
  }

  private List<BigQueryStorageApiInsertError> runMultiContextSerializationRetry(
      FakeDatasetService dataset, boolean useDefaultStream) throws Exception {
    BigQueryOptions options = PipelineOptionsFactory.as(BigQueryOptions.class);
    options.setProject(PROJECT_ID);

    StorageApiDynamicDestinations<TableRow, TableDestination> dynamicDestinations =
        new StorageApiDynamicDestinationsTableRow<>(
            DynamicDestinationsHelpers.ConstantTableDestinations.fromTableSpec(
                ValueProvider.StaticValueProvider.of(TABLE_SPEC), null, false),
            BigQueryIO.TableRowFormatFunction.fromSerializableFunction(element -> element),
            null,
            false,
            CreateDisposition.CREATE_NEVER,
            false,
            false,
            false);

    WriteRecordsDoFnImpl<TableDestination, TableRow> doFnImpl =
        new WriteRecordsDoFnImpl<>(
            "test-op",
            dynamicDestinations,
            new FakeBigQueryServices()
                .withDatasetService(dataset)
                .withJobService(new FakeJobService()),
            useDefaultStream,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            1,
            new TupleTag<KV<String, String>>() {},
            new TupleTag<BigQueryStorageApiInsertError>() {},
            null,
            Predicates.alwaysTrue(),
            false,
            null,
            null,
            false,
            CreateDisposition.CREATE_NEVER,
            null,
            false,
            AppendRowsRequest.MissingValueInterpretation.MISSING_VALUE_INTERPRETATION_UNSPECIFIED,
            5,
            null,
            0);

    TableDestination tableDestination = new TableDestination(TABLE_REF, null);
    WriteRecordsDoFnImpl<TableDestination, TableRow>.DestinationState destinationState =
        doFnImpl.createDestinationState(
            options, tableDestination, false, dataset, dataset, options);

    MessageConverter<TableRow> converter =
        dynamicDestinations.getMessageConverter(tableDestination, options, dataset, dataset);

    List<BigQueryStorageApiInsertError> failedRows = new ArrayList<>();
    DoFn.OutputReceiver<BigQueryStorageApiInsertError> failedRowsReceiver =
        value ->
            WindowedValues.<BigQueryStorageApiInsertError>builder()
                .setValue(value)
                .setTimestamp(GlobalWindow.INSTANCE.maxTimestamp())
                .setWindow(GlobalWindow.INSTANCE)
                .setPaneInfo(PaneInfo.NO_FIRING)
                .setReceiver(windowedValue -> failedRows.add(windowedValue.getValue()));

    RetryManager<AppendRowsResponse, WriteRecordsDoFnImpl.AppendRowsContext> retryManager =
        new RetryManager<>(Duration.millis(1), Duration.millis(10), 5);

    try {
      destinationState.addMessage(toPayload(converter, row("bad")));
      destinationState.addMessage(toPayload(converter, row("good1")));
      destinationState.addMessage(toPayload(converter, row("good2")));
      destinationState.flush(retryManager, null, failedRowsReceiver, null, (d, m) -> {});

      destinationState.addMessage(toPayload(converter, row("good3")));
      destinationState.flush(retryManager, null, failedRowsReceiver, null, (d, m) -> {});

      retryManager.run(true);
    } finally {
      destinationState.teardown();
    }
    return failedRows;
  }

  private static StoragePayloadWithDeadline toPayload(
      MessageConverter<TableRow> converter, TableRow row) throws Exception {
    StorageApiWritePayload payload =
        converter
            .toMessage(row, null, TableRowToStorageApiProto.ErrorCollector.DONT_COLLECT)
            .withTimestamp(Instant.EPOCH);
    return StoragePayloadWithDeadline.of(payload, GlobalWindow.INSTANCE.maxTimestamp());
  }

  private static TableRow row(String id) {
    return new TableRow().set("id", id);
  }

  private static class RecordingRejectBadRowDatasetService extends FakeDatasetService {
    private final List<Long> observedOffsets;

    RecordingRejectBadRowDatasetService(List<Long> observedOffsets) {
      this.observedOffsets = observedOffsets;
    }

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
          observedOffsets.add(offset);
          for (int i = 0; i < rows.getSerializedRowsCount(); i++) {
            String id =
                (String)
                    DynamicMessage.parseFrom(protoDescriptor, rows.getSerializedRows(i))
                        .getField(protoDescriptor.findFieldByName("id"));
            if ("bad".equals(id)) {
              return ApiFutures.immediateFailedFuture(
                  new Exceptions.AppendSerializationError(
                      Status.Code.INVALID_ARGUMENT.value(),
                      "Invalid row",
                      streamName,
                      ImmutableMap.of(i, "Invalid id: " + id)));
            }
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
