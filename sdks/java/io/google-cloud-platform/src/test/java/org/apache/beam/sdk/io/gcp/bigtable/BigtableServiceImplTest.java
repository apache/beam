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
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.runners.core.metrics.GcpResourceIdentifiers;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoMetricName;
import org.apache.beam.runners.core.metrics.ServiceCallMetric;
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
import org.mockito.stubbing.OngoingStubbing;

/** Unit tests of BigtableServiceImpl. */
@RunWith(JUnit4.class)
public class BigtableServiceImplTest {

  private static final String PROJECT_ID = "project";
  private static final String INSTANCE_ID = "instance";
  private static final String TABLE_ID = "table";

  private static final int SEGMENT_SIZE = 10;
  private static final long DEFAULT_ROW_SIZE = 1024 * 1024 * 25; // 25MB
  private static final long DEFAULT_BYTE_SEGMENT_SIZE = 1024 * 1024 * 1000;
  private static final String DEFAULT_PREFIX = "b";

  private static final BigtableTableName TABLE_NAME =
      new BigtableInstanceName(PROJECT_ID, INSTANCE_ID).toTableName(TABLE_ID);

  @Mock private BigtableSession mockSession;

  @Mock private BulkMutation mockBulkMutation;

  @Mock private BigtableDataClient mockBigtableDataClient;

  @Mock private BigtableSource mockBigtableSource;

  @Mock private ScanHandler scanHandler;

  @Mock private ServiceCallMetric mockCallMetric;

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
  public void testReadSingleRangeBelowSegmentLimit() throws Exception {
    RowSet.Builder ranges = RowSet.newBuilder();
    ranges.addRowRanges(
        generateRowRange(
            generateByteString(DEFAULT_PREFIX, 0), generateByteString(DEFAULT_PREFIX, 1)));

    FlatRow expectedRow = FlatRow.newBuilder().withRowKey(ByteString.copyFromUtf8("a")).build();

    when(mockBigtableDataClient.readFlatRows(any(ReadRowsRequest.class), any()))
        .thenAnswer(mockReadRowsAnswer(Arrays.asList(expectedRow)));

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(
            mockSession,
            TABLE_ID,
            ranges.build(),
            SEGMENT_SIZE,
            DEFAULT_BYTE_SEGMENT_SIZE,
            RowFilter.getDefaultInstance(),
            mockCallMetric);

    underTest.start();
    Assert.assertEquals(FlatRowConverter.convert(expectedRow), underTest.getCurrentRow());
    Assert.assertFalse(underTest.advance());
    underTest.close();

    Mockito.verify(mockCallMetric, Mockito.times(2)).call("ok");
  }

  /**
   * This test ensures that all the rows are properly added to the buffer and read. This example
   * uses a single range with SEGMENT_SIZE*2+1 rows. Range: [b00000, b00001, ... b00199, b00200)
   *
   * @throws IOException
   */
  @Test
  public void testReadSingleRangeAboveSegmentLimit() throws IOException {
    RowSet.Builder ranges = RowSet.newBuilder();
    ranges.addRowRanges(
        generateRowRange(
            generateByteString(DEFAULT_PREFIX, 0),
            generateByteString(DEFAULT_PREFIX, SEGMENT_SIZE * 2)));

    OngoingStubbing<?> stub =
        when(mockBigtableDataClient.readFlatRows(any(ReadRowsRequest.class), any()));
    List<List<FlatRow>> expectedResults =
        ImmutableList.of(
            generateSegmentResult(DEFAULT_PREFIX, 0, SEGMENT_SIZE),
            generateSegmentResult(DEFAULT_PREFIX, SEGMENT_SIZE, SEGMENT_SIZE),
            ImmutableList.of());
    expectRowResults(stub, expectedResults);

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(
            mockSession,
            TABLE_ID,
            ranges.build(),
            SEGMENT_SIZE,
            DEFAULT_BYTE_SEGMENT_SIZE,
            RowFilter.getDefaultInstance(),
            mockCallMetric);

    List<Row> actualResults = new ArrayList<>();
    Assert.assertTrue(underTest.start());
    do {
      actualResults.add(underTest.getCurrentRow());
    } while (underTest.advance());

    Assert.assertEquals(
        expectedResults.stream()
            .flatMap(Collection::stream)
            .map(i -> FlatRowConverter.convert(i))
            .collect(Collectors.toList()),
        actualResults);
    underTest.close();
    Mockito.verify(mockCallMetric, Mockito.times(3)).call("ok");
  }

  /**
   * This test ensures that all the rows are properly added to the buffer and read. This example
   * uses two ranges with SEGMENT_SIZE rows. The buffer should be refilled twice and ReadRowsAsync
   * should be called three times. The last rpc call should return zero rows. The following test
   * follows this example: FirstRange: [b00000,b00001,...,b00099,b00100) SecondRange:
   * [b00100,b00101,...,b00199,b00200)
   *
   * @throws IOException
   */
  @Test
  public void testReadMultipleRanges() throws IOException {
    RowSet.Builder ranges = RowSet.newBuilder();
    ranges.addRowRanges(
        generateRowRange(
            generateByteString(DEFAULT_PREFIX, 0),
            generateByteString(DEFAULT_PREFIX, SEGMENT_SIZE)));
    ranges.addRowRanges(
        generateRowRange(
            generateByteString(DEFAULT_PREFIX, SEGMENT_SIZE),
            generateByteString(DEFAULT_PREFIX, SEGMENT_SIZE * 2)));

    OngoingStubbing<?> stub =
        when(mockBigtableDataClient.readFlatRows(any(ReadRowsRequest.class), any()));
    List<List<FlatRow>> expectedResults =
        ImmutableList.of(
            generateSegmentResult(DEFAULT_PREFIX, 0, SEGMENT_SIZE),
            generateSegmentResult(DEFAULT_PREFIX, SEGMENT_SIZE, SEGMENT_SIZE),
            ImmutableList.of());
    expectRowResults(stub, expectedResults);

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(
            mockSession,
            TABLE_ID,
            ranges.build(),
            SEGMENT_SIZE,
            DEFAULT_BYTE_SEGMENT_SIZE,
            RowFilter.getDefaultInstance(),
            mockCallMetric);

    List<Row> actualResults = new ArrayList<>();
    Assert.assertTrue(underTest.start());
    do {
      actualResults.add(underTest.getCurrentRow());
    } while (underTest.advance());

    Assert.assertEquals(
        expectedResults.stream()
            .flatMap(Collection::stream)
            .map(i -> FlatRowConverter.convert(i))
            .collect(Collectors.toList()),
        actualResults);
    underTest.close();

    Mockito.verify(mockCallMetric, Mockito.times(3)).call("ok");
  }

  /**
   * This test ensures that all the rows are properly added to the buffer and read. This example
   * uses three overlapping ranges. The logic should remove all keys that were already added to the
   * buffer. The following test follows this example: FirstRange: [b00000,b00001,...,b00099,b00100)
   * SecondRange: [b00050,b00051...b00100,b00101,...,b00199,b00200) ThirdRange: [b00070, b00200)
   *
   * @throws IOException
   */
  @Test
  public void testReadMultipleRangesOverlappingKeys() throws IOException {
    RowSet.Builder ranges = RowSet.newBuilder();
    ranges.addRowRanges(
        generateRowRange(
            generateByteString(DEFAULT_PREFIX, 0),
            generateByteString(DEFAULT_PREFIX, SEGMENT_SIZE)));
    ranges.addRowRanges(
        generateRowRange(
            generateByteString(DEFAULT_PREFIX, SEGMENT_SIZE / 2),
            generateByteString(DEFAULT_PREFIX, SEGMENT_SIZE * 2)));
    ranges.addRowRanges(
        generateRowRange(
            generateByteString(DEFAULT_PREFIX, (int) (SEGMENT_SIZE * .7)),
            generateByteString(DEFAULT_PREFIX, SEGMENT_SIZE * 2)));

    OngoingStubbing<?> stub =
        when(mockBigtableDataClient.readFlatRows(any(ReadRowsRequest.class), any()));
    List<List<FlatRow>> expectedResults =
        ImmutableList.of(
            generateSegmentResult(DEFAULT_PREFIX, 0, SEGMENT_SIZE),
            generateSegmentResult(DEFAULT_PREFIX, SEGMENT_SIZE, SEGMENT_SIZE),
            ImmutableList.of());
    expectRowResults(stub, expectedResults);

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(
            mockSession,
            TABLE_ID,
            ranges.build(),
            SEGMENT_SIZE,
            DEFAULT_BYTE_SEGMENT_SIZE,
            RowFilter.getDefaultInstance(),
            mockCallMetric);

    List<Row> actualResults = new ArrayList<>();
    Assert.assertTrue(underTest.start());
    do {
      actualResults.add(underTest.getCurrentRow());
    } while (underTest.advance());

    Assert.assertEquals(
        expectedResults.stream()
            .flatMap(Collection::stream)
            .map(i -> FlatRowConverter.convert(i))
            .collect(Collectors.toList()),
        actualResults);
    underTest.close();

    Mockito.verify(mockCallMetric, Mockito.times(3)).call("ok");
  }

  /**
   * This test ensures that all the rows are properly added to the buffer and read. This example
   * uses an empty request to trigger a full table scan. RowRange: [b00000, b00001, ... b00300)
   *
   * @throws IOException
   */
  @Test
  public void testReadFullTableScan() throws IOException {
    OngoingStubbing<?> stub =
        when(mockBigtableDataClient.readFlatRows(any(ReadRowsRequest.class), any()));
    List<List<FlatRow>> expectedResults =
        ImmutableList.of(
            generateSegmentResult(DEFAULT_PREFIX, 0, SEGMENT_SIZE),
            generateSegmentResult(DEFAULT_PREFIX, SEGMENT_SIZE, SEGMENT_SIZE),
            generateSegmentResult(DEFAULT_PREFIX, SEGMENT_SIZE * 2, SEGMENT_SIZE),
            ImmutableList.of());
    expectRowResults(stub, expectedResults);

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(
            mockSession,
            TABLE_ID,
            RowSet.getDefaultInstance(),
            SEGMENT_SIZE,
            DEFAULT_BYTE_SEGMENT_SIZE,
            RowFilter.getDefaultInstance(),
            mockCallMetric);

    List<Row> actualResults = new ArrayList<>();
    Assert.assertTrue(underTest.start());
    do {
      actualResults.add(underTest.getCurrentRow());
    } while (underTest.advance());

    Assert.assertEquals(
        expectedResults.stream()
            .flatMap(Collection::stream)
            .map(i -> FlatRowConverter.convert(i))
            .collect(Collectors.toList()),
        actualResults);
    underTest.close();

    Mockito.verify(mockCallMetric, Mockito.times(4)).call("ok");
    ;
  }

  /**
   * This test compares the amount of RowRanges being requested after the buffer is refilled. After
   * reading the first buffer, the first two RowRanges should be removed and the RowRange containing
   * [b00100,b00200) should be requested.
   *
   * @throws IOException
   */
  @Test
  public void testReadFillBuffer() throws IOException {
    RowSet.Builder ranges = RowSet.newBuilder();
    ranges.addRowRanges(
        generateRowRange(
            generateByteString(DEFAULT_PREFIX, 0),
            generateByteString(DEFAULT_PREFIX, SEGMENT_SIZE / 2)));
    ranges.addRowRanges(
        generateRowRange(
            generateByteString(DEFAULT_PREFIX, SEGMENT_SIZE / 2),
            generateByteString(DEFAULT_PREFIX, SEGMENT_SIZE)));
    ranges.addRowRanges(
        generateRowRange(
            generateByteString(DEFAULT_PREFIX, SEGMENT_SIZE),
            generateByteString(DEFAULT_PREFIX, SEGMENT_SIZE * 2)));

    OngoingStubbing<?> stub =
        when(mockBigtableDataClient.readFlatRows(any(ReadRowsRequest.class), any()));
    List<List<FlatRow>> expectedResults =
        ImmutableList.of(
            generateSegmentResult(DEFAULT_PREFIX, 0, SEGMENT_SIZE),
            generateSegmentResult(DEFAULT_PREFIX, SEGMENT_SIZE, SEGMENT_SIZE),
            ImmutableList.of());
    expectRowResults(stub, expectedResults);

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(
            mockSession,
            TABLE_ID,
            ranges.build(),
            SEGMENT_SIZE,
            DEFAULT_BYTE_SEGMENT_SIZE,
            RowFilter.getDefaultInstance(),
            mockCallMetric);

    List<Row> actualResults = new ArrayList<>();
    Assert.assertTrue(underTest.start());
    verify(mockBigtableDataClient, times(1))
        .readFlatRows(requestCaptor.capture(), any(StreamObserver.class));
    Assert.assertEquals(3, requestCaptor.getValue().getRows().getRowRangesCount());
    do {
      actualResults.add(underTest.getCurrentRow());
    } while (underTest.advance());

    verify(mockBigtableDataClient, times(3))
        .readFlatRows(requestCaptor.capture(), any(StreamObserver.class));
    Assert.assertEquals(1, requestCaptor.getValue().getRows().getRowRangesCount());

    Assert.assertEquals(
        expectedResults.stream()
            .flatMap(Collection::stream)
            .map(i -> FlatRowConverter.convert(i))
            .collect(Collectors.toList()),
        actualResults);
    underTest.close();

    Mockito.verify(mockCallMetric, Mockito.times(3)).call("ok");
  }

  /**
   * This test checks that the buffer will stop filling up once the byte limit is reached. It will
   * cancel the ScanHandler after reached the limit. This test completes one fill and contains one
   * Row after the first buffer has been completed. The test cheaks the current available memory in
   * the JVM and uses a percent of it to mock the original behavior. Range: [b00000, b00010)
   *
   * @throws IOException
   */
  @Test
  public void testReadByteLimitBuffer() throws IOException {
    long segmentByteLimit = DEFAULT_ROW_SIZE * (SEGMENT_SIZE / 2);
    int numOfRowsInsideBuffer = (int) (segmentByteLimit / DEFAULT_ROW_SIZE) + 1;

    RowRange mockRowRange =
        generateRowRange(
            generateByteString(DEFAULT_PREFIX, 0),
            generateByteString(DEFAULT_PREFIX, SEGMENT_SIZE));

    OngoingStubbing<?> stub =
        when(mockBigtableDataClient.readFlatRows(any(ReadRowsRequest.class), any()));
    List<List<FlatRow>> expectedResults =
        ImmutableList.of(
            generateLargeSegmentResult(DEFAULT_PREFIX, 0, numOfRowsInsideBuffer),
            generateSegmentResult(
                DEFAULT_PREFIX, numOfRowsInsideBuffer, SEGMENT_SIZE - numOfRowsInsideBuffer),
            ImmutableList.of());
    expectRowResults(stub, expectedResults);

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(
            mockSession,
            TABLE_ID,
            RowSet.newBuilder().addRowRanges(mockRowRange).build(),
            SEGMENT_SIZE,
            segmentByteLimit,
            RowFilter.getDefaultInstance(),
            mockCallMetric);

    List<Row> actualResults = new ArrayList<>();
    Assert.assertTrue(underTest.start());
    do {
      actualResults.add(underTest.getCurrentRow());
    } while (underTest.advance());

    Assert.assertEquals(
        expectedResults.stream()
            .flatMap(Collection::stream)
            .map(i -> FlatRowConverter.convert(i))
            .collect(Collectors.toList()),
        actualResults);
    underTest.close();

    Mockito.verify(mockCallMetric, Mockito.times(3)).call("ok");
  }

  /**
   * This test ensures the Exception handling inside of the scanHandler. This test will check if a
   * StatusRuntimeException was thrown.
   *
   * @throws IOException
   */
  @Test
  public void testReadSegmentExceptionHandling() throws IOException {
    RowSet.Builder ranges = RowSet.newBuilder();
    ranges.addRowRanges(
        generateRowRange(
            generateByteString(DEFAULT_PREFIX, 0), generateByteString(DEFAULT_PREFIX, 1)));

    when(mockBigtableDataClient.readFlatRows(any(ReadRowsRequest.class), any()))
        .thenAnswer(
            new Answer<ScanHandler>() {
              @Override
              public ScanHandler answer(InvocationOnMock invocationOnMock) throws Throwable {
                StreamObserver<FlatRow> flatRowObserver = invocationOnMock.getArgument(1);
                new Thread() {
                  @Override
                  public void run() {
                    flatRowObserver.onError(Status.INVALID_ARGUMENT.asRuntimeException());
                  }
                }.start();

                return scanHandler;
              }
            });
    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(
            mockSession,
            TABLE_ID,
            ranges.build(),
            SEGMENT_SIZE,
            DEFAULT_BYTE_SEGMENT_SIZE,
            RowFilter.getDefaultInstance(),
            mockCallMetric);

    IOException returnedError = null;
    try {
      underTest.start();
    } catch (IOException e) {
      returnedError = e;
    }
    Assert.assertTrue(returnedError.getCause() instanceof StatusRuntimeException);

    Mockito.verify(mockCallMetric, Mockito.times(1))
        .call(Status.INVALID_ARGUMENT.getCode().toString());
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
    when(mockBigtableSource.getTableId()).thenReturn(StaticValueProvider.of(TABLE_ID));
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

  private Answer<ScanHandler> mockReadRowsAnswer(List<FlatRow> rows) {
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

  private static RowRange generateRowRange(ByteString start, ByteString end) {
    return RowRange.newBuilder().setStartKeyClosed(start).setEndKeyOpen(end).build();
  }

  private static List<FlatRow> generateSegmentResult(String prefix, int startIndex, int count) {
    return generateSegmentResult(prefix, startIndex, count, false);
  }

  private static List<FlatRow> generateLargeSegmentResult(
      String prefix, int startIndex, int count) {
    return generateSegmentResult(prefix, startIndex, count, true);
  }

  private static List<FlatRow> generateSegmentResult(
      String prefix, int startIndex, int count, boolean largeRow) {
    byte[] largeMemory = new byte[(int) DEFAULT_ROW_SIZE];
    return IntStream.range(startIndex, startIndex + count)
        .mapToObj(
            i -> {
              FlatRow.Builder builder = FlatRow.newBuilder();
              if (!largeRow) {
                builder.withRowKey(generateByteString(prefix, i));
              } else {
                builder
                    .withRowKey(generateByteString(prefix, i))
                    .addCell(
                        "Family",
                        ByteString.copyFromUtf8("LargeMemoryRow"),
                        System.currentTimeMillis(),
                        ByteString.copyFrom(largeMemory));
              }
              return builder.build();
            })
        .collect(Collectors.toList());
  }

  private <T> OngoingStubbing<T> expectRowResults(
      OngoingStubbing<T> stub, List<List<FlatRow>> results) {
    for (List<FlatRow> result : results) {
      stub = stub.thenAnswer(mockReadRowsAnswer(result));
    }
    return stub;
  }

  private static ByteString generateByteString(String prefix, int index) {
    return ByteString.copyFromUtf8(prefix + String.format("%05d", index));
  }
}
