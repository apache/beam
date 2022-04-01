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
package org.apache.beam.sdk.io.gcp.bigtable;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsRequest.Entry;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.runners.core.metrics.GcpResourceIdentifiers;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoMetricName;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO.BigtableSource;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Unit tests of BigtableServiceImpl. */
@RunWith(JUnit4.class)
public class BigtableServiceImplTest {

  private static final String PROJECT_ID = "project";
  private static final String INSTANCE_ID = "instance";
  private static final String TABLE_ID = "table";

  private static final int MINI_BATCH_ROW_LIMIT = 100;

  private static final BigtableTableName TABLE_NAME =
      new BigtableInstanceName(PROJECT_ID, INSTANCE_ID).toTableName(TABLE_ID);

  @Mock private BigtableSession mockSession;

  @Mock private BulkMutation mockBulkMutation;

  @Mock private BigtableDataClient mockBigtableDataClient;

  @Mock private BigtableSource mockBigtableSource;

  @Captor private ArgumentCaptor<ReadRowsRequest> requestCaptor;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    BigtableOptions options =
        new BigtableOptions.Builder().setProjectId(PROJECT_ID).setInstanceId(INSTANCE_ID).build();
    when(mockSession.getOptions()).thenReturn(options);
    when(mockSession.createBulkMutation(eq(TABLE_NAME))).thenReturn(mockBulkMutation);
    when(mockSession.getDataClient()).thenReturn(mockBigtableDataClient);
    // Setup the ProcessWideContainer for testing metrics are set.
    MetricsContainerImpl container = new MetricsContainerImpl(null);
    MetricsEnvironment.setProcessWideContainer(container);
  }

  /**
   * This test ensures that protobuf creation and interactions with {@link BigtableDataClient} work
   * as expected.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testRead() throws IOException {
    ByteKey start = ByteKey.copyFrom("a".getBytes(StandardCharsets.UTF_8));
    ByteKey end = ByteKey.copyFrom("b".getBytes(StandardCharsets.UTF_8));
    when(mockBigtableSource.getRanges()).thenReturn(Arrays.asList(ByteKeyRange.of(start, end)));
    when(mockBigtableSource.getTableId()).thenReturn(StaticValueProvider.of(TABLE_ID));
    @SuppressWarnings("unchecked")
    ResultScanner<Row> mockResultScanner = Mockito.mock(ResultScanner.class);
    Row expectedRow = Row.newBuilder().setKey(ByteString.copyFromUtf8("a")).build();
    when(mockResultScanner.next()).thenReturn(expectedRow).thenReturn(null);
    when(mockBigtableDataClient.readRows(any(ReadRowsRequest.class))).thenReturn(mockResultScanner);
    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableReaderImpl(mockSession, mockBigtableSource);

    underTest.start();
    Assert.assertEquals(expectedRow, underTest.getCurrentRow());
    Assert.assertFalse(underTest.advance());
    underTest.close();

    verify(mockResultScanner, times(1)).close();
    verifyMetricWasSet("google.bigtable.v2.ReadRows", "ok", 1);
  }

  /**
   * This test ensures that protobuf creation and interactions with {@link BigtableDataClient} work
   * as expected. This test checks that a single row is returned from the future.
   *
   * @throws IOException
   */
  @Test
  public void testReadSingleRangeBelowMiniBatchLimit() throws Exception {
    ByteKey start = ByteKey.copyFrom("a".getBytes(StandardCharsets.UTF_8));
    ByteKey end = ByteKey.copyFrom("b".getBytes(StandardCharsets.UTF_8));

    when(mockBigtableSource.getRanges()).thenReturn(Arrays.asList(ByteKeyRange.of(start, end)));
    when(mockBigtableSource.getTableId()).thenReturn(StaticValueProvider.of(TABLE_ID));
    Row expectedRow = Row.newBuilder().setKey(ByteString.copyFromUtf8("a")).build();
    when(mockBigtableDataClient.readRowsAsync(any(ReadRowsRequest.class)))
        .thenReturn(Futures.immediateFuture(Arrays.asList(expectedRow)));

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableMiniBatchReaderImpl(mockSession, mockBigtableSource);

    underTest.start();
    Assert.assertEquals(expectedRow, underTest.getCurrentRow());
    Assert.assertFalse(underTest.advance());
    underTest.close();

    verifyMetricWasSet("google.bigtable.v2.ReadRows", "ok", 1);
  }

  /**
   * This test ensures that all the rows are properly added to the buffer and read. This example
   * uses a single range with MINI_BATCH_ROW_LIMIT*2+1 rows. Range: [a, b1, ... b199, c)
   *
   * @throws IOException
   */
  @Test
  public void testReadSingleRangeAboveMiniBatchLimit() throws IOException {
    List<Row> expectedFirstRangeRows = new ArrayList<>();
    List<Row> expectedSecondRangeRows = new ArrayList<>();

    for (int i = 0; i < MINI_BATCH_ROW_LIMIT; i++) {
      expectedFirstRangeRows.add(Row.newBuilder().setKey(ByteString.copyFromUtf8("b" + i)).build());
      expectedSecondRangeRows.add(
          Row.newBuilder()
              .setKey(ByteString.copyFromUtf8("b" + (i + MINI_BATCH_ROW_LIMIT)))
              .build());
    }
    expectedFirstRangeRows.set(0, Row.newBuilder().setKey(ByteString.copyFromUtf8("a")).build());

    ByteKey start = ByteKey.copyFrom("a".getBytes(StandardCharsets.UTF_8));
    ByteKey end = ByteKey.copyFrom("c".getBytes(StandardCharsets.UTF_8));

    when(mockBigtableSource.getRanges()).thenReturn(Arrays.asList(ByteKeyRange.of(start, end)));
    when(mockBigtableSource.getTableId()).thenReturn(StaticValueProvider.of(TABLE_ID));
    when(mockBigtableDataClient.readRowsAsync(any(ReadRowsRequest.class)))
        .thenReturn(Futures.immediateFuture(expectedFirstRangeRows))
        .thenReturn(Futures.immediateFuture(expectedSecondRangeRows))
        .thenReturn(null);

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableMiniBatchReaderImpl(mockSession, mockBigtableSource);

    underTest.start();
    Assert.assertEquals(expectedFirstRangeRows.get(0), underTest.getCurrentRow());
    for (int i = 0; i < MINI_BATCH_ROW_LIMIT; i++) {
      underTest.advance();
    }
    for (int i = 0; i < MINI_BATCH_ROW_LIMIT; i++) {
      underTest.advance();
    }
    Assert.assertEquals(
        expectedSecondRangeRows.get(expectedSecondRangeRows.size() - 1), underTest.getCurrentRow());
    Assert.assertFalse(underTest.advance());

    underTest.close();
    verifyMetricWasSet("google.bigtable.v2.ReadRows", "ok", 2);
  }

  /**
   * This test ensures that all the rows are properly added to the buffer and read. This example
   * uses two ranges with MINI_BATCH_ROW_LIMIT rows. The buffer should be refilled twice and
   * ReadRowsAsync should be called twice. The following test follows this example: FirstRange:
   * [a,b1,...,b99,c) SecondRange: [c,d1,...,d99,e)
   *
   * @throws IOException
   */
  @Test
  public void testReadMultipleRanges() throws IOException {
    List<Row> expectedFirstRangeRows = new ArrayList<>();
    expectedFirstRangeRows.add(Row.newBuilder().setKey(ByteString.copyFromUtf8("a")).build());

    List<Row> expectedSecondRangeRows = new ArrayList<>();
    expectedSecondRangeRows.add(Row.newBuilder().setKey(ByteString.copyFromUtf8("c")).build());

    for (int i = 1; i < MINI_BATCH_ROW_LIMIT; i++) {
      expectedFirstRangeRows.add(Row.newBuilder().setKey(ByteString.copyFromUtf8("b" + i)).build());
      expectedSecondRangeRows.add(
          Row.newBuilder().setKey(ByteString.copyFromUtf8("d" + i)).build());
    }

    ByteKey firstStart = ByteKey.copyFrom("a".getBytes(StandardCharsets.UTF_8));
    ByteKey sharedKeyEnd = ByteKey.copyFrom("c".getBytes(StandardCharsets.UTF_8));
    ByteKey secondEnd = ByteKey.copyFrom("e".getBytes(StandardCharsets.UTF_8));

    when(mockBigtableSource.getRanges())
        .thenReturn(
            Arrays.asList(
                ByteKeyRange.of(firstStart, sharedKeyEnd),
                ByteKeyRange.of(sharedKeyEnd, secondEnd)));
    when(mockBigtableSource.getTableId()).thenReturn(StaticValueProvider.of(TABLE_ID));
    when(mockBigtableDataClient.readRowsAsync(any(ReadRowsRequest.class)))
        .thenReturn(Futures.immediateFuture(expectedFirstRangeRows))
        .thenReturn(Futures.immediateFuture(expectedSecondRangeRows))
        .thenReturn(null);

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableMiniBatchReaderImpl(mockSession, mockBigtableSource);

    underTest.start();
    Assert.assertEquals(expectedFirstRangeRows.get(0), underTest.getCurrentRow());
    for (int i = 1; i < MINI_BATCH_ROW_LIMIT; i++) {
      underTest.advance();
    }
    Assert.assertEquals(
        expectedFirstRangeRows.get(expectedFirstRangeRows.size() - 1), underTest.getCurrentRow());
    for (int i = 0; i < MINI_BATCH_ROW_LIMIT; i++) {
      underTest.advance();
    }

    Assert.assertEquals(
        expectedSecondRangeRows.get(expectedSecondRangeRows.size() - 1), underTest.getCurrentRow());
    Assert.assertFalse(underTest.advance());
    underTest.close();

    verifyMetricWasSet("google.bigtable.v2.ReadRows", "ok", 2);
  }

  /**
   * This test ensures that all the rows are properly added to the buffer and read. This example
   * uses three overlapping ranges. The logic should remove all keys that were added to the buffer.
   * The following test follows this example: FirstRange: [a,b1,...,b99,b100) SecondRange:
   * [b50,d1,...,d99,c) ThirdRange: [b70, c)
   *
   * @throws IOException
   */
  @Test
  public void testReadMultipleRangesOverlappingKeys() throws IOException {
    List<Row> expectedFirstRangeRows = new ArrayList<>();
    expectedFirstRangeRows.add(Row.newBuilder().setKey(ByteString.copyFromUtf8("a")).build());

    List<Row> expectedSecondRangeRows = new ArrayList<>();
    expectedSecondRangeRows.add(Row.newBuilder().setKey(ByteString.copyFromUtf8("c")).build());

    for (int i = 1; i < 2 * MINI_BATCH_ROW_LIMIT; i++) {
      if (i < MINI_BATCH_ROW_LIMIT) {
        expectedFirstRangeRows.add(
            Row.newBuilder().setKey(ByteString.copyFromUtf8("b" + i)).build());
      }
      if (i > MINI_BATCH_ROW_LIMIT / 2) {
        expectedSecondRangeRows.add(
            Row.newBuilder().setKey(ByteString.copyFromUtf8("b" + i)).build());
      }
    }

    ByteKey firstStart = ByteKey.copyFrom("a".getBytes(StandardCharsets.UTF_8));
    ByteKey firstEnd =
        ByteKey.copyFrom(("b" + (MINI_BATCH_ROW_LIMIT)).getBytes(StandardCharsets.UTF_8));

    ByteKey secondStart =
        ByteKey.copyFrom(("b" + (MINI_BATCH_ROW_LIMIT / 2)).getBytes(StandardCharsets.UTF_8));
    ByteKey secondEnd = ByteKey.copyFrom("c".getBytes(StandardCharsets.UTF_8));

    ByteKey thirdStart =
        ByteKey.copyFrom(("b" + (MINI_BATCH_ROW_LIMIT / 2 + 20)).getBytes(StandardCharsets.UTF_8));

    when(mockBigtableSource.getRanges())
        .thenReturn(
            Arrays.asList(
                ByteKeyRange.of(firstStart, firstEnd),
                ByteKeyRange.of(secondStart, secondEnd),
                ByteKeyRange.of(thirdStart, secondEnd)));
    when(mockBigtableSource.getTableId()).thenReturn(StaticValueProvider.of(TABLE_ID));

    when(mockBigtableDataClient.readRowsAsync(any(ReadRowsRequest.class)))
        .thenReturn(Futures.immediateFuture(expectedFirstRangeRows))
        .thenReturn(
            Futures.immediateFuture(
                expectedSecondRangeRows.subList(
                    MINI_BATCH_ROW_LIMIT / 2, expectedSecondRangeRows.size())))
        .thenReturn(null);

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableMiniBatchReaderImpl(mockSession, mockBigtableSource);

    underTest.start();
    Assert.assertEquals(expectedFirstRangeRows.get(0), underTest.getCurrentRow());
    for (int i = 1; i < MINI_BATCH_ROW_LIMIT; i++) {
      underTest.advance();
    }
    Assert.assertEquals(
        expectedFirstRangeRows.get(expectedFirstRangeRows.size() - 1), underTest.getCurrentRow());
    for (int i = 0; i < MINI_BATCH_ROW_LIMIT; i++) {
      underTest.advance();
    }
    Assert.assertEquals(
        expectedSecondRangeRows.get(expectedSecondRangeRows.size() - 1), underTest.getCurrentRow());
    Assert.assertFalse(underTest.advance());
    underTest.close();

    verifyMetricWasSet("google.bigtable.v2.ReadRows", "ok", 2);
  }

  /**
   * This test ensures that all the rows are properly added to the buffer and read. This example
   * uses an empty request to trigger a full table scan. RowRange: [b0, b1, ... b299)
   *
   * @throws IOException
   */
  @Test
  public void testReadFullTableScan() throws IOException {
    List<Row> expectedRangeRows = new ArrayList<>();

    for (int i = 0; i < MINI_BATCH_ROW_LIMIT * 3; i++) {
      if (i < MINI_BATCH_ROW_LIMIT) {
        expectedRangeRows.add(Row.newBuilder().setKey(ByteString.copyFromUtf8("b" + i)).build());
      } else if (i < MINI_BATCH_ROW_LIMIT * 2) {
        expectedRangeRows.add(Row.newBuilder().setKey(ByteString.copyFromUtf8("b" + i)).build());
      } else {
        expectedRangeRows.add(Row.newBuilder().setKey(ByteString.copyFromUtf8("b" + i)).build());
      }
    }

    when(mockBigtableSource.getRanges()).thenReturn(Arrays.asList());
    when(mockBigtableSource.getTableId()).thenReturn(StaticValueProvider.of(TABLE_ID));
    when(mockBigtableDataClient.readRowsAsync(any(ReadRowsRequest.class)))
        .thenReturn(Futures.immediateFuture(expectedRangeRows.subList(0, MINI_BATCH_ROW_LIMIT)))
        .thenReturn(
            Futures.immediateFuture(
                expectedRangeRows.subList(MINI_BATCH_ROW_LIMIT, MINI_BATCH_ROW_LIMIT * 2)))
        .thenReturn(
            Futures.immediateFuture(
                expectedRangeRows.subList(MINI_BATCH_ROW_LIMIT * 2, MINI_BATCH_ROW_LIMIT * 3)))
        .thenReturn(null);

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableMiniBatchReaderImpl(mockSession, mockBigtableSource);

    underTest.start();
    Assert.assertEquals(expectedRangeRows.get(0), underTest.getCurrentRow());
    for (int i = 1; i < 3 * MINI_BATCH_ROW_LIMIT; i++) {
      underTest.advance();
    }
    Assert.assertFalse(underTest.advance());
    underTest.close();
  }

  /**
   * This test compares the amount of RowRanges being requested after the buffer is refilled. After
   * reading the first buffer, the first two RowRanges should be removed and the RowRange containing
   * c should be requested.
   *
   * @throws IOException
   */
  @Test
  public void testReadFillBuffer() throws IOException {
    List<Row> rowsInRowRanges = new ArrayList<>();

    for (int i = 0; i < MINI_BATCH_ROW_LIMIT + MINI_BATCH_ROW_LIMIT / 2; i++) {
      if (i < MINI_BATCH_ROW_LIMIT) {
        rowsInRowRanges.add(Row.newBuilder().setKey(ByteString.copyFromUtf8("b" + i)).build());
      } else {
        rowsInRowRanges.add(Row.newBuilder().setKey(ByteString.copyFromUtf8("c" + i)).build());
      }
    }
    rowsInRowRanges.set(0, Row.newBuilder().setKey(ByteString.copyFromUtf8("a")).build());

    ByteKey firstStart = ByteKey.copyFrom("a".getBytes(StandardCharsets.UTF_8));
    ByteKey firstEnd =
        ByteKey.copyFrom(("b" + MINI_BATCH_ROW_LIMIT / 2).getBytes(StandardCharsets.UTF_8));

    ByteKey secondStart =
        ByteKey.copyFrom(("b" + MINI_BATCH_ROW_LIMIT / 2).getBytes(StandardCharsets.UTF_8));
    ByteKey secondEnd =
        ByteKey.copyFrom(("b" + (MINI_BATCH_ROW_LIMIT - 1)).getBytes(StandardCharsets.UTF_8));

    ByteKey thirdStart = ByteKey.copyFrom(("c0").getBytes(StandardCharsets.UTF_8));
    ByteKey thirdEnd =
        ByteKey.copyFrom(("c" + (MINI_BATCH_ROW_LIMIT / 2 - 1)).getBytes(StandardCharsets.UTF_8));

    when(mockBigtableSource.getRanges())
        .thenReturn(
            Arrays.asList(
                ByteKeyRange.of(firstStart, firstEnd),
                ByteKeyRange.of(secondStart, secondEnd),
                ByteKeyRange.of(thirdStart, thirdEnd)));
    when(mockBigtableSource.getTableId()).thenReturn(StaticValueProvider.of(TABLE_ID));
    when(mockBigtableDataClient.readRowsAsync(any(ReadRowsRequest.class)))
        .thenReturn(Futures.immediateFuture(rowsInRowRanges.subList(0, MINI_BATCH_ROW_LIMIT)))
        .thenReturn(
            Futures.immediateFuture(
                rowsInRowRanges.subList(
                    MINI_BATCH_ROW_LIMIT, MINI_BATCH_ROW_LIMIT + MINI_BATCH_ROW_LIMIT / 2)))
        .thenReturn(null);

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableMiniBatchReaderImpl(mockSession, mockBigtableSource);

    underTest.start();
    verify(mockBigtableDataClient, times(1)).readRowsAsync(requestCaptor.capture());
    Assert.assertEquals(3, requestCaptor.getValue().getRows().getRowRangesCount());
    Assert.assertEquals(rowsInRowRanges.get(0), underTest.getCurrentRow());
    for (int i = 0; i < MINI_BATCH_ROW_LIMIT; i++) {
      underTest.advance();
    }
    verify(mockBigtableDataClient, times(2)).readRowsAsync(requestCaptor.capture());
    for (int i = 0; i < MINI_BATCH_ROW_LIMIT; i++) {
      underTest.advance();
    }
    Assert.assertEquals(rowsInRowRanges.get(rowsInRowRanges.size() - 1), underTest.getCurrentRow());
    Assert.assertEquals(1, requestCaptor.getValue().getRows().getRowRangesCount());
    Assert.assertFalse(underTest.advance());

    underTest.close();
    verifyMetricWasSet("google.bigtable.v2.ReadRows", "ok", 2);
  }

  /**
   * This test ensures that protobuf creation and interactions with {@link BulkMutation} work as
   * expected.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testWrite() throws IOException, InterruptedException {
    BigtableService.Writer underTest =
        new BigtableServiceImpl.BigtableWriterImpl(mockSession, TABLE_NAME);

    Mutation mutation =
        Mutation.newBuilder()
            .setSetCell(SetCell.newBuilder().setFamilyName("Family").build())
            .build();
    ByteString key = ByteString.copyFromUtf8("key");

    SettableFuture<MutateRowResponse> fakeResponse = SettableFuture.create();
    when(mockBulkMutation.add(any(MutateRowsRequest.Entry.class))).thenReturn(fakeResponse);

    underTest.writeRecord(KV.of(key, ImmutableList.of(mutation)));
    Entry expected =
        MutateRowsRequest.Entry.newBuilder().setRowKey(key).addMutations(mutation).build();
    verify(mockBulkMutation, times(1)).add(expected);

    underTest.close();
    verify(mockBulkMutation, times(1)).flush();
  }

  private void verifyMetricWasSet(String method, String status, long count) {
    // Verify the metric as reported.
    HashMap<String, String> labels = new HashMap<>();
    labels.put(MonitoringInfoConstants.Labels.PTRANSFORM, "");
    labels.put(MonitoringInfoConstants.Labels.SERVICE, "BigTable");
    labels.put(MonitoringInfoConstants.Labels.METHOD, method);
    labels.put(
        MonitoringInfoConstants.Labels.RESOURCE,
        GcpResourceIdentifiers.bigtableResource(PROJECT_ID, INSTANCE_ID, TABLE_ID));
    labels.put(MonitoringInfoConstants.Labels.BIGTABLE_PROJECT_ID, PROJECT_ID);
    labels.put(MonitoringInfoConstants.Labels.INSTANCE_ID, INSTANCE_ID);
    labels.put(
        MonitoringInfoConstants.Labels.TABLE_ID,
        GcpResourceIdentifiers.bigtableTableID(PROJECT_ID, INSTANCE_ID, TABLE_ID));
    labels.put(MonitoringInfoConstants.Labels.STATUS, status);

    MonitoringInfoMetricName name =
        MonitoringInfoMetricName.named(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, labels);
    MetricsContainerImpl container =
        (MetricsContainerImpl) MetricsEnvironment.getProcessWideContainer();
    assertEquals(count, (long) container.getCounter(name).getCumulative());
  }
}
