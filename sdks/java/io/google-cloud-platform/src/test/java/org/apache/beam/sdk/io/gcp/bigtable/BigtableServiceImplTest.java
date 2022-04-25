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
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.FlatRowConverter;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.grpc.scanner.ScanHandler;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/** Unit tests of BigtableServiceImpl. */
@RunWith(JUnit4.class)
public class BigtableServiceImplTest {

  private static final String PROJECT_ID = "project";
  private static final String INSTANCE_ID = "instance";
  private static final String TABLE_ID = "table";

  private static final int MINI_BATCH_ROW_LIMIT = 100;
  private static final double DEFAULT_BYTE_LIMIT_PERCENTAGE = .8;
  private static final long DEFAULT_ROW_SIZE = 1024 * 1024 * 256;

  private static final BigtableTableName TABLE_NAME =
      new BigtableInstanceName(PROJECT_ID, INSTANCE_ID).toTableName(TABLE_ID);

  @Mock private BigtableSession mockSession;

  @Mock private BulkMutation mockBulkMutation;

  @Mock private BigtableDataClient mockBigtableDataClient;

  @Mock private BigtableSource mockBigtableSource;

  @Mock private ScanHandler scanHandler;

  @Captor private ArgumentCaptor<ReadRowsRequest> requestCaptor;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    BigtableOptions options =
        new BigtableOptions.Builder().setProjectId(PROJECT_ID).setInstanceId(INSTANCE_ID).build();
    when(mockSession.getOptions()).thenReturn(options);
    when(mockSession.createBulkMutation(eq(TABLE_NAME))).thenReturn(mockBulkMutation);
    when(mockSession.getDataClient()).thenReturn(mockBigtableDataClient);
    when(mockBigtableSource.getTableId()).thenReturn(StaticValueProvider.of(TABLE_ID));
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
  public void testReadSingleRangeBelowSegmentLimit() throws Exception {
    ByteKey start = ByteKey.copyFrom("a".getBytes(StandardCharsets.UTF_8));
    ByteKey end = ByteKey.copyFrom("b".getBytes(StandardCharsets.UTF_8));

    when(mockBigtableSource.getRanges()).thenReturn(Arrays.asList(ByteKeyRange.of(start, end)));
    FlatRow expectedRow = FlatRow.newBuilder().withRowKey(ByteString.copyFromUtf8("a")).build();

    when(mockBigtableDataClient.readFlatRows(any(ReadRowsRequest.class), any()))
        .thenAnswer(mockReadRowsAnswer(Arrays.asList(expectedRow)));

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(mockSession, mockBigtableSource);

    underTest.start();
    Assert.assertEquals(FlatRowConverter.convert(expectedRow), underTest.getCurrentRow());
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
  public void testReadSingleRangeAboveSegmentLimit() throws IOException {
    List<FlatRow> expectedFirstRangeRows = new ArrayList<>();
    List<FlatRow> expectedSecondRangeRows = new ArrayList<>();

    for (int i = 0; i < MINI_BATCH_ROW_LIMIT; i++) {
      expectedFirstRangeRows.add(
          FlatRow.newBuilder()
              .withRowKey(ByteString.copyFromUtf8("b" + String.format("%05d", i)))
              .build());
      expectedSecondRangeRows.add(
          FlatRow.newBuilder()
              .withRowKey(
                  ByteString.copyFromUtf8("b" + String.format("%05d", i + MINI_BATCH_ROW_LIMIT)))
              .build());
    }
    expectedFirstRangeRows.set(
        0, FlatRow.newBuilder().withRowKey(ByteString.copyFromUtf8("a")).build());

    ByteKey start = ByteKey.copyFrom("a".getBytes(StandardCharsets.UTF_8));
    ByteKey end = ByteKey.copyFrom("c".getBytes(StandardCharsets.UTF_8));

    when(mockBigtableSource.getRanges()).thenReturn(Arrays.asList(ByteKeyRange.of(start, end)));

    when(mockBigtableDataClient.readFlatRows(any(ReadRowsRequest.class), any()))
        .thenAnswer(mockReadRowsAnswer(expectedFirstRangeRows))
        .thenAnswer(mockReadRowsAnswer(expectedSecondRangeRows))
        .thenAnswer(mockReadRowsAnswer(new ArrayList<FlatRow>()));

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(mockSession, mockBigtableSource);

    underTest.start();
    Assert.assertEquals(
        FlatRowConverter.convert(expectedFirstRangeRows.get(0)), underTest.getCurrentRow());
    for (int i = 0; i < MINI_BATCH_ROW_LIMIT - 1; i++) {
      underTest.advance();
    }
    for (int i = 0; i < MINI_BATCH_ROW_LIMIT; i++) {
      underTest.advance();
    }
    Assert.assertEquals(
        FlatRowConverter.convert(expectedSecondRangeRows.get(expectedSecondRangeRows.size() - 1)),
        underTest.getCurrentRow());
    Assert.assertFalse(underTest.advance());

    underTest.close();
    verifyMetricWasSet("google.bigtable.v2.ReadRows", "ok", 3);
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
    List<FlatRow> expectedFirstRangeRows = new ArrayList<>();
    expectedFirstRangeRows.add(
        FlatRow.newBuilder().withRowKey(ByteString.copyFromUtf8("a")).build());

    List<FlatRow> expectedSecondRangeRows = new ArrayList<>();
    expectedSecondRangeRows.add(
        FlatRow.newBuilder().withRowKey(ByteString.copyFromUtf8("c")).build());

    for (int i = 0; i < MINI_BATCH_ROW_LIMIT - 1; i++) {
      expectedFirstRangeRows.add(
          FlatRow.newBuilder()
              .withRowKey(ByteString.copyFromUtf8("b" + String.format("%05d", i)))
              .build());
      expectedSecondRangeRows.add(
          FlatRow.newBuilder()
              .withRowKey(ByteString.copyFromUtf8("d" + String.format("%05d", i)))
              .build());
    }

    ByteKey firstStart = ByteKey.copyFrom("a".getBytes(StandardCharsets.UTF_8));
    ByteKey sharedKeyEnd = ByteKey.copyFrom("c".getBytes(StandardCharsets.UTF_8));
    ByteKey secondEnd = ByteKey.copyFrom("e".getBytes(StandardCharsets.UTF_8));

    when(mockBigtableSource.getRanges())
        .thenReturn(
            Arrays.asList(
                ByteKeyRange.of(firstStart, sharedKeyEnd),
                ByteKeyRange.of(sharedKeyEnd, secondEnd)));
    when(mockBigtableDataClient.readFlatRows(any(ReadRowsRequest.class), any()))
        .thenAnswer(mockReadRowsAnswer(expectedFirstRangeRows))
        .thenAnswer(mockReadRowsAnswer(expectedSecondRangeRows))
        .thenAnswer(mockReadRowsAnswer(new ArrayList<FlatRow>()));

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(mockSession, mockBigtableSource);

    underTest.start();
    Assert.assertEquals(
        FlatRowConverter.convert(expectedFirstRangeRows.get(0)), underTest.getCurrentRow());
    for (int i = 0; i < MINI_BATCH_ROW_LIMIT - 1; i++) {
      underTest.advance();
    }
    Assert.assertEquals(
        FlatRowConverter.convert(expectedFirstRangeRows.get(expectedFirstRangeRows.size() - 1)),
        underTest.getCurrentRow());
    for (int i = 0; i < MINI_BATCH_ROW_LIMIT; i++) {
      underTest.advance();
    }

    Assert.assertEquals(
        FlatRowConverter.convert(expectedSecondRangeRows.get(expectedSecondRangeRows.size() - 1)),
        underTest.getCurrentRow());
    Assert.assertFalse(underTest.advance());
    underTest.close();

    verifyMetricWasSet("google.bigtable.v2.ReadRows", "ok", 3);
  }

  /**
   * This test ensures that all the rows are properly added to the buffer and read. This example
   * uses three overlapping ranges. The logic should remove all keys that were added to the buffer.
   * The following test follows this example: FirstRange: [a,b1,...,b99,b100) SecondRange:
   * [b50,b51...b100,d1,...,d199,c) ThirdRange: [b70, c)
   *
   * @throws IOException
   */
  @Test
  public void testReadMultipleRangesOverlappingKeys() throws IOException {
    List<FlatRow> expectedFirstRangeRows = new ArrayList<>();
    expectedFirstRangeRows.add(
        FlatRow.newBuilder().withRowKey(ByteString.copyFromUtf8("a")).build());

    List<FlatRow> expectedSecondRangeRows = new ArrayList<>();
    expectedSecondRangeRows.add(
        FlatRow.newBuilder().withRowKey(ByteString.copyFromUtf8("c")).build());

    for (int i = 1; i < 2 * MINI_BATCH_ROW_LIMIT; i++) {
      if (i < MINI_BATCH_ROW_LIMIT) {
        expectedFirstRangeRows.add(
            FlatRow.newBuilder()
                .withRowKey(ByteString.copyFromUtf8("b" + String.format("%05d", i)))
                .build());
      }
      if (i > MINI_BATCH_ROW_LIMIT / 2) {
        expectedSecondRangeRows.add(
            FlatRow.newBuilder()
                .withRowKey(ByteString.copyFromUtf8("b" + String.format("%05d", i)))
                .build());
      }
    }

    ByteKey firstStart = ByteKey.copyFrom("a".getBytes(StandardCharsets.UTF_8));
    ByteKey firstEnd =
        ByteKey.copyFrom(
            ("b" + String.format("%05d", (MINI_BATCH_ROW_LIMIT))).getBytes(StandardCharsets.UTF_8));

    ByteKey secondStart =
        ByteKey.copyFrom(
            ("b" + String.format("%05d", (MINI_BATCH_ROW_LIMIT / 2)))
                .getBytes(StandardCharsets.UTF_8));
    ByteKey secondEnd = ByteKey.copyFrom("c".getBytes(StandardCharsets.UTF_8));

    ByteKey thirdStart =
        ByteKey.copyFrom(
            ("b" + String.format("%05d", (MINI_BATCH_ROW_LIMIT / 2 + 20)))
                .getBytes(StandardCharsets.UTF_8));

    when(mockBigtableSource.getRanges())
        .thenReturn(
            Arrays.asList(
                ByteKeyRange.of(firstStart, firstEnd),
                ByteKeyRange.of(secondStart, secondEnd),
                ByteKeyRange.of(thirdStart, secondEnd)));

    when(mockBigtableDataClient.readFlatRows(any(ReadRowsRequest.class), any()))
        .thenAnswer(mockReadRowsAnswer(expectedFirstRangeRows))
        .thenAnswer(
            mockReadRowsAnswer(
                expectedSecondRangeRows.subList(
                    MINI_BATCH_ROW_LIMIT / 2, MINI_BATCH_ROW_LIMIT + MINI_BATCH_ROW_LIMIT / 2)))
        .thenAnswer(mockReadRowsAnswer(new ArrayList<FlatRow>()));

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(mockSession, mockBigtableSource);

    underTest.start();
    Assert.assertEquals(
        FlatRowConverter.convert(expectedFirstRangeRows.get(0)), underTest.getCurrentRow());
    for (int i = 1; i < MINI_BATCH_ROW_LIMIT; i++) {
      underTest.advance();
    }
    Assert.assertEquals(
        FlatRowConverter.convert(expectedFirstRangeRows.get(expectedFirstRangeRows.size() - 1)),
        underTest.getCurrentRow());
    for (int i = 0; i < MINI_BATCH_ROW_LIMIT; i++) {
      underTest.advance();
    }
    Assert.assertEquals(
        FlatRowConverter.convert(expectedSecondRangeRows.get(expectedSecondRangeRows.size() - 1)),
        underTest.getCurrentRow());
    Assert.assertFalse(underTest.advance());
    underTest.close();

    verifyMetricWasSet("google.bigtable.v2.ReadRows", "ok", 3);
  }

  /**
   * This test ensures that all the rows are properly added to the buffer and read. This example
   * uses an empty request to trigger a full table scan. RowRange: [b0, b1, ... b299)
   *
   * @throws IOException
   */
  @Test
  public void testReadFullTableScan() throws IOException {
    List<FlatRow> expectedRangeRows = new ArrayList<>();

    for (int i = 0; i < MINI_BATCH_ROW_LIMIT * 3; i++) {
      if (i < MINI_BATCH_ROW_LIMIT) {
        expectedRangeRows.add(
            FlatRow.newBuilder()
                .withRowKey(ByteString.copyFromUtf8("b" + String.format("%05d", i)))
                .build());
      } else if (i < MINI_BATCH_ROW_LIMIT * 2) {
        expectedRangeRows.add(
            FlatRow.newBuilder()
                .withRowKey(ByteString.copyFromUtf8("b" + String.format("%05d", i)))
                .build());
      } else {
        expectedRangeRows.add(
            FlatRow.newBuilder()
                .withRowKey(ByteString.copyFromUtf8("b" + String.format("%05d", i)))
                .build());
      }
    }

    when(mockBigtableSource.getRanges()).thenReturn(Arrays.asList());
    when(mockBigtableDataClient.readFlatRows(any(ReadRowsRequest.class), any()))
        .thenAnswer(mockReadRowsAnswer(expectedRangeRows.subList(0, MINI_BATCH_ROW_LIMIT)))
        .thenAnswer(
            mockReadRowsAnswer(
                expectedRangeRows.subList(MINI_BATCH_ROW_LIMIT, MINI_BATCH_ROW_LIMIT * 2)))
        .thenAnswer(
            mockReadRowsAnswer(
                expectedRangeRows.subList(MINI_BATCH_ROW_LIMIT * 2, MINI_BATCH_ROW_LIMIT * 3)))
        .thenAnswer(mockReadRowsAnswer(new ArrayList<FlatRow>()));

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(mockSession, mockBigtableSource);

    underTest.start();
    Assert.assertEquals(
        FlatRowConverter.convert(expectedRangeRows.get(0)), underTest.getCurrentRow());
    for (int i = 0; i < 3 * MINI_BATCH_ROW_LIMIT - 1; i++) {
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
    List<FlatRow> rowsInRowRanges = new ArrayList<>();

    for (int i = 0; i < MINI_BATCH_ROW_LIMIT + MINI_BATCH_ROW_LIMIT / 2; i++) {
      if (i < MINI_BATCH_ROW_LIMIT) {
        rowsInRowRanges.add(
            FlatRow.newBuilder()
                .withRowKey(ByteString.copyFromUtf8("b" + String.format("%05d", i)))
                .build());
      } else {
        rowsInRowRanges.add(
            FlatRow.newBuilder()
                .withRowKey(ByteString.copyFromUtf8("c" + String.format("%05d", i)))
                .build());
      }
    }
    rowsInRowRanges.set(0, FlatRow.newBuilder().withRowKey(ByteString.copyFromUtf8("a")).build());

    ByteKey firstStart = ByteKey.copyFrom("a".getBytes(StandardCharsets.UTF_8));
    ByteKey firstEnd =
        ByteKey.copyFrom(
            ("b" + String.format("%05d", MINI_BATCH_ROW_LIMIT / 2))
                .getBytes(StandardCharsets.UTF_8));

    ByteKey secondStart =
        ByteKey.copyFrom(
            ("b" + String.format("%05d", MINI_BATCH_ROW_LIMIT / 2))
                .getBytes(StandardCharsets.UTF_8));
    ByteKey secondEnd =
        ByteKey.copyFrom(
            ("b" + String.format("%05d", (MINI_BATCH_ROW_LIMIT - 1)))
                .getBytes(StandardCharsets.UTF_8));

    ByteKey thirdStart = ByteKey.copyFrom(("c00000").getBytes(StandardCharsets.UTF_8));
    ByteKey thirdEnd =
        ByteKey.copyFrom(
            ("c" + String.format("%05d", (MINI_BATCH_ROW_LIMIT / 2 - 1)))
                .getBytes(StandardCharsets.UTF_8));

    when(mockBigtableSource.getRanges())
        .thenReturn(
            Arrays.asList(
                ByteKeyRange.of(firstStart, firstEnd),
                ByteKeyRange.of(secondStart, secondEnd),
                ByteKeyRange.of(thirdStart, thirdEnd)));
    when(mockBigtableDataClient.readFlatRows(any(ReadRowsRequest.class), any()))
        .thenAnswer(mockReadRowsAnswer(rowsInRowRanges.subList(0, MINI_BATCH_ROW_LIMIT)))
        .thenAnswer(
            mockReadRowsAnswer(
                rowsInRowRanges.subList(
                    MINI_BATCH_ROW_LIMIT, MINI_BATCH_ROW_LIMIT + MINI_BATCH_ROW_LIMIT / 2)))
        .thenAnswer(mockReadRowsAnswer(new ArrayList<FlatRow>()));

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(mockSession, mockBigtableSource);

    underTest.start();
    verify(mockBigtableDataClient, times(1))
        .readFlatRows(requestCaptor.capture(), any(StreamObserver.class));
    Assert.assertEquals(3, requestCaptor.getValue().getRows().getRowRangesCount());
    Assert.assertEquals(
        FlatRowConverter.convert(rowsInRowRanges.get(0)), underTest.getCurrentRow());
    for (int i = 0; i < MINI_BATCH_ROW_LIMIT; i++) {
      underTest.advance();
    }
    verify(mockBigtableDataClient, times(2))
        .readFlatRows(requestCaptor.capture(), any(StreamObserver.class));
    for (int i = 0; i < MINI_BATCH_ROW_LIMIT; i++) {
      underTest.advance();
    }
    Assert.assertEquals(
        FlatRowConverter.convert(rowsInRowRanges.get(rowsInRowRanges.size() - 1)),
        underTest.getCurrentRow());
    Assert.assertEquals(1, requestCaptor.getValue().getRows().getRowRangesCount());
    Assert.assertFalse(underTest.advance());

    underTest.close();
    verifyMetricWasSet("google.bigtable.v2.ReadRows", "ok", 2);
  }

  /**
   * This test checks that the buffer will not fill up once the byte limit is reached. It will
   * cancel the ScanHandler after reached the limit. This test completes one fill and contains one
   * Row after the first buffer has been completed. The test cheaks the current free memory in the
   * JVM and uses a percent of it to mock the original behavior.
   *
   * @throws IOException
   */
  @Test
  public void testReadByteLimitBuffer() throws IOException {
    List<FlatRow> expectedFirstRangeRows = new ArrayList<>();
    // Max amount of memory allowed in a Row (256MB)
    byte[] largeMemory = new byte[(int) DEFAULT_ROW_SIZE];
    long currentByteLimit =
        (long) (Runtime.getRuntime().freeMemory() * DEFAULT_BYTE_LIMIT_PERCENTAGE);
    int numOfRowsInsideBuffer = (int) (currentByteLimit / DEFAULT_ROW_SIZE) + 1;
    System.out.println("CurrentByteLimit inside of test method: " + currentByteLimit);
    System.out.println("Num Of Rows Inside Of Buffer (start):" + numOfRowsInsideBuffer);
    FlatRow.Builder largeRow =
        FlatRow.newBuilder()
            .addCell(
                "Family",
                ByteString.copyFromUtf8("LargeMemoryRow"),
                System.currentTimeMillis(),
                ByteString.copyFrom(largeMemory));

    for (int i = 0; i < numOfRowsInsideBuffer + 1; i++) {
      expectedFirstRangeRows.add(
          largeRow.withRowKey(ByteString.copyFromUtf8("b" + String.format("%05d", i))).build());
    }
    expectedFirstRangeRows.set(
        0, FlatRow.newBuilder().withRowKey(ByteString.copyFromUtf8("a")).build());

    ByteKey start = ByteKey.copyFrom("a".getBytes(StandardCharsets.UTF_8));
    ByteKey end = ByteKey.copyFrom("c".getBytes(StandardCharsets.UTF_8));

    when(mockBigtableSource.getRanges()).thenReturn(Arrays.asList(ByteKeyRange.of(start, end)));

    when(mockBigtableDataClient.readFlatRows(any(ReadRowsRequest.class), any()))
        .thenAnswer(mockReadRowsAnswer(expectedFirstRangeRows.subList(0, numOfRowsInsideBuffer)))
        .thenAnswer(
            mockReadRowsAnswer(
                expectedFirstRangeRows.subList(
                    numOfRowsInsideBuffer, expectedFirstRangeRows.size())));

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(mockSession, mockBigtableSource);
    underTest.start();
    Assert.assertEquals(
        FlatRowConverter.convert(expectedFirstRangeRows.get(0)), underTest.getCurrentRow());
    for (int i = 0; i < expectedFirstRangeRows.size() - 1; i++) {
      System.out.println("Step: " + i);
      underTest.advance();
      Assert.assertEquals(
          FlatRowConverter.convert(expectedFirstRangeRows.get(i + 1)), underTest.getCurrentRow());
    }
    System.out.println("Asserting final row");
    Assert.assertEquals(
        FlatRowConverter.convert(expectedFirstRangeRows.get(expectedFirstRangeRows.size() - 1)),
        underTest.getCurrentRow());
    System.out.println("Asserting final advance");
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

  public Answer<ScanHandler> mockReadRowsAnswer(List<FlatRow> rows) {
    return new Answer<ScanHandler>() {
      @Override
      public ScanHandler answer(InvocationOnMock invocationOnMock) throws Throwable {
        StreamObserver<FlatRow> flatRowObserver = invocationOnMock.getArgument(1);
        new Thread() {
          @Override
          public void run() {
            for (int i = 0; i < rows.size(); i++) {
              flatRowObserver.onNext(rows.get(i));
            }
            flatRowObserver.onCompleted();
          }
        }.start();

        return scanHandler;
      }
    };
  }
}
