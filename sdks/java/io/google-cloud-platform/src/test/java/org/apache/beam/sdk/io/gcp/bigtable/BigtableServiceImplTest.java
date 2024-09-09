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
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.batching.Batcher;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.api.gax.rpc.StreamController;
import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.RowAdapter;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStub;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
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

  private static final int SEGMENT_SIZE = 10;
  private static final long DEFAULT_ROW_SIZE = 1024 * 1024 * 25; // 25MB
  private static final long DEFAULT_BYTE_SEGMENT_SIZE = 1024 * 1024 * 1000;
  private static final String DEFAULT_PREFIX = "b";

  private static RequestContext requestContext =
      RequestContext.create(PROJECT_ID, INSTANCE_ID, "default");

  @Mock private Batcher<RowMutationEntry, Void> mockBatcher;

  @Mock private BigtableDataClient mockBigtableDataClient;

  @Mock private EnhancedBigtableStub mockStub;

  private BigtableDataSettings bigtableDataSettings;

  @Mock private BigtableSource mockBigtableSource;

  @Mock private ServiceCallMetric mockCallMetric;

  @Captor private ArgumentCaptor<Query> queryCaptor;

  private static AtomicBoolean cancelled = new AtomicBoolean(false);

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    this.bigtableDataSettings =
        BigtableDataSettings.newBuilder()
            .setProjectId(PROJECT_ID)
            .setInstanceId(INSTANCE_ID)
            .setAppProfileId("default")
            .build();
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
  @SuppressWarnings("unchecked")
  public void testRead() throws IOException {
    ByteKey start = ByteKey.copyFrom("a".getBytes(StandardCharsets.UTF_8));
    ByteKey end = ByteKey.copyFrom("b".getBytes(StandardCharsets.UTF_8));
    when(mockBigtableSource.getRanges()).thenReturn(Arrays.asList(ByteKeyRange.of(start, end)));
    when(mockBigtableSource.getTableId()).thenReturn(StaticValueProvider.of(TABLE_ID));

    Row expectedRow = Row.newBuilder().setKey(ByteString.copyFromUtf8("a")).build();

    // Set up iterator to be returned by ServerStream.iterator()
    Iterator<Row> mockIterator = Mockito.mock(Iterator.class);
    when(mockIterator.next()).thenReturn(expectedRow).thenReturn(null);
    when(mockIterator.hasNext()).thenReturn(true).thenReturn(false);
    // Set up ServerStream to be returned by callable.call(Query)
    ServerStream<Row> mockRows = Mockito.mock(ServerStream.class);
    when(mockRows.iterator()).thenReturn(mockIterator);
    // Set up Callable to be returned by stub.createReadRowsCallable()
    ServerStreamingCallable<Query, Row> mockCallable = Mockito.mock(ServerStreamingCallable.class);
    when(mockCallable.call(
            any(com.google.cloud.bigtable.data.v2.models.Query.class), any(ApiCallContext.class)))
        .thenReturn(mockRows);
    when(mockStub.createReadRowsCallable(any(RowAdapter.class))).thenReturn(mockCallable);
    ServerStreamingCallable<Query, Row> callable =
        mockStub.createReadRowsCallable(new BigtableServiceImpl.BigtableRowProtoAdapter());
    // Set up client to return callable
    when(mockBigtableDataClient.readRowsCallable(any(RowAdapter.class))).thenReturn(callable);

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableReaderImpl(
            mockBigtableDataClient,
            bigtableDataSettings.getProjectId(),
            bigtableDataSettings.getInstanceId(),
            mockBigtableSource.getTableId().get(),
            mockBigtableSource.getRanges(),
            null);

    underTest.start();
    Assert.assertEquals(expectedRow, underTest.getCurrentRow());
    Assert.assertFalse(underTest.advance());

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

    Row expectedRow = Row.newBuilder().setKey(ByteString.copyFromUtf8("a")).build();

    // Set up Callable to be returned by stub.createReadRowsCallable()
    ServerStreamingCallable<Query, Row> mockCallable = Mockito.mock(ServerStreamingCallable.class);
    // Set up ResponseObserver to return expectedRow
    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocation) throws Throwable {
                ((ResponseObserver) invocation.getArgument(1)).onResponse(expectedRow);
                ((ResponseObserver) invocation.getArgument(1)).onComplete();
                return null;
              }
            })
        .when(mockCallable)
        .call(
            any(com.google.cloud.bigtable.data.v2.models.Query.class),
            any(ResponseObserver.class),
            any(ApiCallContext.class));
    when(mockStub.createReadRowsCallable(any(RowAdapter.class))).thenReturn(mockCallable);
    ServerStreamingCallable<Query, Row> callable =
        mockStub.createReadRowsCallable(new BigtableServiceImpl.BigtableRowProtoAdapter());
    // Set up client to return callable
    when(mockBigtableDataClient.readRowsCallable(any(RowAdapter.class))).thenReturn(callable);
    when(mockBigtableSource.getTableId()).thenReturn(StaticValueProvider.of(TABLE_ID));

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(
            mockBigtableDataClient,
            bigtableDataSettings.getProjectId(),
            bigtableDataSettings.getInstanceId(),
            mockBigtableSource.getTableId().get(),
            ranges.build(),
            RowFilter.getDefaultInstance(),
            SEGMENT_SIZE,
            DEFAULT_BYTE_SEGMENT_SIZE,
            mockCallMetric);

    underTest.start();
    Assert.assertEquals(expectedRow, underTest.getCurrentRow());
    Assert.assertFalse(underTest.advance());

    Mockito.verify(mockCallMetric, Mockito.times(2)).call("ok");
  }

  /**
   * This test ensures that protobuf creation and interactions with {@link BigtableDataClient} work
   * as expected. This test checks that a single row is returned from the future.
   *
   * @throws IOException
   */
  @Test
  public void testReadSingleRangeAtSegmentLimit() throws Exception {
    RowSet.Builder ranges = RowSet.newBuilder();
    ranges.addRowRanges(
        generateRowRange(
            generateByteString(DEFAULT_PREFIX, SEGMENT_SIZE),
            generateByteString(DEFAULT_PREFIX, SEGMENT_SIZE - 1)));

    // Set up Callable to be returned by stub.createReadRowsCallable()
    ServerStreamingCallable<Query, Row> mockCallable = Mockito.mock(ServerStreamingCallable.class);
    List<List<Row>> expectedResults =
        ImmutableList.of(
            generateSegmentResult(DEFAULT_PREFIX, 0, SEGMENT_SIZE), ImmutableList.of());

    // Return multiple answers when mockCallable is called
    doAnswer(
            new MultipleAnswer<Row>(
                ImmutableList.of(
                    generateSegmentResult(DEFAULT_PREFIX, 0, SEGMENT_SIZE),
                    generateSegmentResult(DEFAULT_PREFIX, SEGMENT_SIZE, SEGMENT_SIZE * 2),
                    ImmutableList.of())))
        .when(mockCallable)
        .call(any(Query.class), any(ResponseObserver.class), any(ApiCallContext.class));

    when(mockStub.createReadRowsCallable(any(RowAdapter.class))).thenReturn(mockCallable);
    ServerStreamingCallable<Query, Row> callable =
        mockStub.createReadRowsCallable(new BigtableServiceImpl.BigtableRowProtoAdapter());
    // Set up client to return callable
    when(mockBigtableDataClient.readRowsCallable(any(RowAdapter.class))).thenReturn(callable);
    when(mockBigtableSource.getTableId()).thenReturn(StaticValueProvider.of(TABLE_ID));

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(
            mockBigtableDataClient,
            bigtableDataSettings.getProjectId(),
            bigtableDataSettings.getInstanceId(),
            mockBigtableSource.getTableId().get(),
            ranges.build(),
            RowFilter.getDefaultInstance(),
            SEGMENT_SIZE,
            DEFAULT_BYTE_SEGMENT_SIZE,
            mockCallMetric);

    List<Row> actualResults = new ArrayList<>();
    Assert.assertTrue(underTest.start());
    do {
      actualResults.add(underTest.getCurrentRow());
    } while (underTest.advance());

    Assert.assertEquals(
        expectedResults.stream().flatMap(Collection::stream).collect(Collectors.toList()),
        actualResults);

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

    // Set up Callable to be returned by stub.createReadRowsCallable()
    ServerStreamingCallable<Query, Row> mockCallable = Mockito.mock(ServerStreamingCallable.class);

    List<List<Row>> expectedResults =
        ImmutableList.of(
            generateSegmentResult(DEFAULT_PREFIX, 0, SEGMENT_SIZE),
            generateSegmentResult(DEFAULT_PREFIX, SEGMENT_SIZE, SEGMENT_SIZE),
            ImmutableList.of());

    // Return multiple answers when mockCallable is called
    doAnswer(new MultipleAnswer<Row>(expectedResults))
        .when(mockCallable)
        .call(any(Query.class), any(ResponseObserver.class), any(ApiCallContext.class));

    when(mockStub.createReadRowsCallable(any(RowAdapter.class))).thenReturn(mockCallable);
    ServerStreamingCallable<Query, Row> callable =
        mockStub.createReadRowsCallable(new BigtableServiceImpl.BigtableRowProtoAdapter());
    // Set up client to return callable
    when(mockBigtableDataClient.readRowsCallable(any(RowAdapter.class))).thenReturn(callable);

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(
            mockBigtableDataClient,
            bigtableDataSettings.getProjectId(),
            bigtableDataSettings.getInstanceId(),
            TABLE_ID,
            ranges.build(),
            RowFilter.getDefaultInstance(),
            SEGMENT_SIZE,
            DEFAULT_BYTE_SEGMENT_SIZE,
            mockCallMetric);

    List<Row> actualResults = new ArrayList<>();
    Assert.assertTrue(underTest.start());
    do {
      actualResults.add(underTest.getCurrentRow());
    } while (underTest.advance());

    Assert.assertEquals(
        expectedResults.stream().flatMap(Collection::stream).collect(Collectors.toList()),
        actualResults);
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

    // Set up Callable to be returned by stub.createReadRowsCallable()
    ServerStreamingCallable<Query, Row> mockCallable = Mockito.mock(ServerStreamingCallable.class);

    List<List<Row>> expectedResults =
        ImmutableList.of(
            generateSegmentResult(DEFAULT_PREFIX, 0, SEGMENT_SIZE),
            generateSegmentResult(DEFAULT_PREFIX, SEGMENT_SIZE, SEGMENT_SIZE),
            ImmutableList.of());

    // Return multiple answers when mockCallable is called
    doAnswer(new MultipleAnswer<Row>(expectedResults))
        .when(mockCallable)
        .call(any(Query.class), any(ResponseObserver.class), any(ApiCallContext.class));

    when(mockStub.createReadRowsCallable(any(RowAdapter.class))).thenReturn(mockCallable);
    ServerStreamingCallable<Query, Row> callable =
        mockStub.createReadRowsCallable(new BigtableServiceImpl.BigtableRowProtoAdapter());

    // Set up client to return callable
    when(mockBigtableDataClient.readRowsCallable(any(RowAdapter.class))).thenReturn(callable);

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(
            mockBigtableDataClient,
            bigtableDataSettings.getProjectId(),
            bigtableDataSettings.getInstanceId(),
            TABLE_ID,
            ranges.build(),
            RowFilter.getDefaultInstance(),
            SEGMENT_SIZE,
            DEFAULT_BYTE_SEGMENT_SIZE,
            mockCallMetric);

    List<Row> actualResults = new ArrayList<>();
    Assert.assertTrue(underTest.start());
    do {
      actualResults.add(underTest.getCurrentRow());
    } while (underTest.advance());

    Assert.assertEquals(
        expectedResults.stream().flatMap(Collection::stream).collect(Collectors.toList()),
        actualResults);

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

    List<List<Row>> expectedResults =
        ImmutableList.of(
            generateSegmentResult(DEFAULT_PREFIX, 0, SEGMENT_SIZE),
            generateSegmentResult(DEFAULT_PREFIX, SEGMENT_SIZE, SEGMENT_SIZE),
            ImmutableList.of());

    // Set up Callable to be returned by stub.createReadRowsCallable()
    ServerStreamingCallable<Query, Row> mockCallable = Mockito.mock(ServerStreamingCallable.class);
    // Return multiple answers when mockCallable is called
    doAnswer(new MultipleAnswer<Row>(expectedResults))
        .when(mockCallable)
        .call(any(Query.class), any(ResponseObserver.class), any(ApiCallContext.class));
    when(mockStub.createReadRowsCallable(any(RowAdapter.class))).thenReturn(mockCallable);
    ServerStreamingCallable<Query, Row> callable =
        mockStub.createReadRowsCallable(new BigtableServiceImpl.BigtableRowProtoAdapter());
    // Set up client to return callable
    when(mockBigtableDataClient.readRowsCallable(any(RowAdapter.class))).thenReturn(callable);

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(
            mockBigtableDataClient,
            bigtableDataSettings.getProjectId(),
            bigtableDataSettings.getInstanceId(),
            TABLE_ID,
            ranges.build(),
            RowFilter.getDefaultInstance(),
            SEGMENT_SIZE,
            DEFAULT_BYTE_SEGMENT_SIZE,
            mockCallMetric);

    List<Row> actualResults = new ArrayList<>();
    Assert.assertTrue(underTest.start());
    do {
      actualResults.add(underTest.getCurrentRow());
    } while (underTest.advance());

    Assert.assertEquals(
        expectedResults.stream().flatMap(Collection::stream).collect(Collectors.toList()),
        actualResults);

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
    List<List<Row>> expectedResults =
        ImmutableList.of(
            generateSegmentResult(DEFAULT_PREFIX, 0, SEGMENT_SIZE),
            generateSegmentResult(DEFAULT_PREFIX, SEGMENT_SIZE, SEGMENT_SIZE),
            generateSegmentResult(DEFAULT_PREFIX, SEGMENT_SIZE * 2, SEGMENT_SIZE),
            ImmutableList.of());

    // Set up Callable to be returned by stub.createReadRowsCallable()
    ServerStreamingCallable<Query, Row> mockCallable = Mockito.mock(ServerStreamingCallable.class);
    // Return multiple answers when mockCallable is called
    doAnswer(new MultipleAnswer<Row>(expectedResults))
        .when(mockCallable)
        .call(any(Query.class), any(ResponseObserver.class), any(ApiCallContext.class));
    when(mockStub.createReadRowsCallable(any(RowAdapter.class))).thenReturn(mockCallable);
    ServerStreamingCallable<Query, Row> callable =
        mockStub.createReadRowsCallable(new BigtableServiceImpl.BigtableRowProtoAdapter());
    // Set up client to return callable
    when(mockBigtableDataClient.readRowsCallable(any(RowAdapter.class))).thenReturn(callable);

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(
            mockBigtableDataClient,
            bigtableDataSettings.getProjectId(),
            bigtableDataSettings.getInstanceId(),
            TABLE_ID,
            RowSet.getDefaultInstance(),
            RowFilter.getDefaultInstance(),
            SEGMENT_SIZE,
            DEFAULT_BYTE_SEGMENT_SIZE,
            mockCallMetric);

    List<Row> actualResults = new ArrayList<>();
    Assert.assertTrue(underTest.start());
    do {
      actualResults.add(underTest.getCurrentRow());
    } while (underTest.advance());

    Assert.assertEquals(
        expectedResults.stream().flatMap(Collection::stream).collect(Collectors.toList()),
        actualResults);

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

    List<List<Row>> expectedResults =
        ImmutableList.of(
            generateSegmentResult(DEFAULT_PREFIX, 0, SEGMENT_SIZE),
            generateSegmentResult(DEFAULT_PREFIX, SEGMENT_SIZE, SEGMENT_SIZE),
            ImmutableList.of());

    // Set up Callable to be returned by stub.createReadRowsCallable()
    ServerStreamingCallable<Query, Row> mockCallable = Mockito.mock(ServerStreamingCallable.class);
    // Return multiple answers when mockCallable is called
    doAnswer(new MultipleAnswer<Row>(expectedResults))
        .when(mockCallable)
        .call(any(Query.class), any(ResponseObserver.class), any(ApiCallContext.class));
    when(mockStub.createReadRowsCallable(any(RowAdapter.class))).thenReturn(mockCallable);
    ServerStreamingCallable<Query, Row> callable =
        mockStub.createReadRowsCallable(new BigtableServiceImpl.BigtableRowProtoAdapter());
    // Set up client to return callable
    when(mockBigtableDataClient.readRowsCallable(any(RowAdapter.class))).thenReturn(callable);

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(
            mockBigtableDataClient,
            bigtableDataSettings.getProjectId(),
            bigtableDataSettings.getInstanceId(),
            TABLE_ID,
            ranges.build(),
            RowFilter.getDefaultInstance(),
            SEGMENT_SIZE,
            DEFAULT_BYTE_SEGMENT_SIZE,
            mockCallMetric);

    List<Row> actualResults = new ArrayList<>();
    Assert.assertTrue(underTest.start());

    verify(callable, times(1))
        .call(queryCaptor.capture(), any(ResponseObserver.class), any(ApiCallContext.class));
    Assert.assertEquals(
        3, queryCaptor.getValue().toProto(requestContext).getRows().getRowRangesCount());
    do {
      actualResults.add(underTest.getCurrentRow());
    } while (underTest.advance());

    verify(callable, times(3))
        .call(queryCaptor.capture(), any(ResponseObserver.class), any(ApiCallContext.class));
    Assert.assertEquals(
        1, queryCaptor.getValue().toProto(requestContext).getRows().getRowRangesCount());

    Assert.assertEquals(
        expectedResults.stream().flatMap(Collection::stream).collect(Collectors.toList()),
        actualResults);

    Mockito.verify(mockCallMetric, Mockito.times(3)).call("ok");
  }

  /** This test verifies that a refill request is sent before we read all the rows in the buffer. */
  @Test
  public void testRefillOnLowWatermark() throws IOException {
    int segmentSize = 30;
    RowSet.Builder ranges = RowSet.newBuilder();
    // generate 3 pages of rows
    ranges.addRowRanges(
        generateRowRange(
            generateByteString(DEFAULT_PREFIX, 0),
            generateByteString(DEFAULT_PREFIX, segmentSize * 3)));

    List<List<Row>> expectedResults =
        ImmutableList.of(
            generateSegmentResult(DEFAULT_PREFIX, 0, segmentSize),
            generateSegmentResult(DEFAULT_PREFIX, segmentSize, segmentSize),
            generateSegmentResult(DEFAULT_PREFIX, segmentSize * 2, segmentSize),
            ImmutableList.of());

    // Set up Callable to be returned by stub.createReadRowsCallable()
    ServerStreamingCallable<Query, Row> mockCallable = Mockito.mock(ServerStreamingCallable.class);
    // Return multiple answers when mockCallable is called
    doAnswer(new MultipleAnswer<Row>(expectedResults))
        .when(mockCallable)
        .call(any(Query.class), any(ResponseObserver.class), any(ApiCallContext.class));
    when(mockStub.createReadRowsCallable(any(RowAdapter.class))).thenReturn(mockCallable);
    ServerStreamingCallable<Query, Row> callable =
        mockStub.createReadRowsCallable(new BigtableServiceImpl.BigtableRowProtoAdapter());
    // Set up client to return callable
    when(mockBigtableDataClient.readRowsCallable(any(RowAdapter.class))).thenReturn(callable);

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(
            mockBigtableDataClient,
            bigtableDataSettings.getProjectId(),
            bigtableDataSettings.getInstanceId(),
            TABLE_ID,
            ranges.build(),
            RowFilter.getDefaultInstance(),
            segmentSize,
            DEFAULT_BYTE_SEGMENT_SIZE,
            mockCallMetric);

    Assert.assertTrue(underTest.start());

    int refillWatermark = Math.max(1, (int) (segmentSize * 0.1));

    Assert.assertTrue(refillWatermark > 1);

    // Make sure refill happens on the second page. At this point, there
    // should be 3 calls made on the callable.
    for (int i = 0; i < segmentSize * 2 - refillWatermark + 1; i++) {
      underTest.getCurrentRow();
      underTest.advance();
    }

    verify(callable, times(3))
        .call(queryCaptor.capture(), any(ResponseObserver.class), any(ApiCallContext.class));
  }

  /**
   * This test checks that the buffer will stop filling up once the byte limit is reached. It will
   * cancel the controller after reached the limit. This test completes one fill and contains one
   * Row after the first buffer has been completed. The test checks the current available memory in
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

    List<List<Row>> expectedResults =
        ImmutableList.of(
            generateLargeSegmentResult(DEFAULT_PREFIX, 0, numOfRowsInsideBuffer),
            generateSegmentResult(
                DEFAULT_PREFIX, numOfRowsInsideBuffer, SEGMENT_SIZE - numOfRowsInsideBuffer),
            ImmutableList.of());

    // Set up Callable to be returned by stub.createReadRowsCallable()
    ServerStreamingCallable<Query, Row> mockCallable = Mockito.mock(ServerStreamingCallable.class);

    // Create a mock stream controller
    StreamController mockController = Mockito.mock(StreamController.class);
    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocation) throws Throwable {
                cancelled.set(true);
                return null;
              }
            })
        .when(mockController)
        .cancel();
    // Return multiple answers when mockCallable is called
    doAnswer(new MultipleAnswer<Row>(expectedResults, mockController))
        .when(mockCallable)
        .call(any(Query.class), any(ResponseObserver.class), any(ApiCallContext.class));
    when(mockStub.createReadRowsCallable(any(RowAdapter.class))).thenReturn(mockCallable);
    ServerStreamingCallable<Query, Row> callable =
        mockStub.createReadRowsCallable(new BigtableServiceImpl.BigtableRowProtoAdapter());
    // Set up client to return callable
    when(mockBigtableDataClient.readRowsCallable(any(RowAdapter.class))).thenReturn(callable);

    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(
            mockBigtableDataClient,
            bigtableDataSettings.getProjectId(),
            bigtableDataSettings.getInstanceId(),
            TABLE_ID,
            RowSet.newBuilder().addRowRanges(mockRowRange).build(),
            RowFilter.getDefaultInstance(),
            SEGMENT_SIZE,
            segmentByteLimit,
            mockCallMetric);

    List<Row> actualResults = new ArrayList<>();
    Assert.assertTrue(underTest.start());
    do {
      actualResults.add(underTest.getCurrentRow());
    } while (underTest.advance());

    Assert.assertEquals(
        expectedResults.stream().flatMap(Collection::stream).collect(Collectors.toList()),
        actualResults);

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

    // Set up Callable to be returned by stub.createReadRowsCallable()
    ServerStreamingCallable<Query, Row> mockCallable = Mockito.mock(ServerStreamingCallable.class);
    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                new Thread() {
                  @Override
                  public void run() {
                    ((ResponseObserver) invocationOnMock.getArgument(1))
                        .onError(Status.INVALID_ARGUMENT.asRuntimeException());
                  }
                }.start();

                return null;
              }
            })
        .when(mockCallable)
        .call(any(Query.class), any(ResponseObserver.class), any(ApiCallContext.class));
    when(mockStub.createReadRowsCallable(any(RowAdapter.class))).thenReturn(mockCallable);
    ServerStreamingCallable<Query, Row> callable =
        mockStub.createReadRowsCallable(new BigtableServiceImpl.BigtableRowProtoAdapter());
    // Set up client to return callable
    when(mockBigtableDataClient.readRowsCallable(any(RowAdapter.class))).thenReturn(callable);
    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableSegmentReaderImpl(
            mockBigtableDataClient,
            bigtableDataSettings.getProjectId(),
            bigtableDataSettings.getInstanceId(),
            TABLE_ID,
            ranges.build(),
            RowFilter.getDefaultInstance(),
            SEGMENT_SIZE,
            DEFAULT_BYTE_SEGMENT_SIZE,
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
   * This test ensures that protobuf creation and interactions with {@link Batcher} work as
   * expected.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testWrite() throws IOException {
    doReturn(mockBatcher).when(mockBigtableDataClient).newBulkMutationBatcher((String) any());
    SettableApiFuture<Void> fakeFuture = SettableApiFuture.create();
    when(mockBatcher.closeAsync()).thenReturn(fakeFuture);
    ArgumentCaptor<RowMutationEntry> captor = ArgumentCaptor.forClass(RowMutationEntry.class);
    ApiFuture<Void> fakeResponse = SettableApiFuture.create();
    when(mockBatcher.add(any(RowMutationEntry.class)))
        .thenAnswer(
            invocation -> {
              fakeFuture.set(null);
              return fakeResponse;
            });

    BigtableService.Writer underTest =
        new BigtableServiceImpl.BigtableWriterImpl(
            mockBigtableDataClient,
            bigtableDataSettings.getProjectId(),
            bigtableDataSettings.getInstanceId(),
            TABLE_ID,
            Duration.millis(60000));

    ByteString key = ByteString.copyFromUtf8("key");
    Mutation mutation =
        Mutation.newBuilder()
            .setSetCell(
                Mutation.SetCell.newBuilder()
                    .setFamilyName("Family")
                    .setColumnQualifier(ByteString.copyFromUtf8("q"))
                    .setValue(ByteString.copyFromUtf8("value"))
                    .build())
            .build();

    underTest.writeRecord(KV.of(key, ImmutableList.of(mutation)));

    verify(mockBatcher).add(captor.capture());

    assertEquals(key, captor.getValue().toProto().getRowKey());
    assertTrue(captor.getValue().toProto().getMutations(0).hasSetCell());
    assertEquals(
        "Family", captor.getValue().toProto().getMutations(0).getSetCell().getFamilyName());
    underTest.close();
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

  private static RowRange generateRowRange(ByteString start, ByteString end) {
    return RowRange.newBuilder().setStartKeyClosed(start).setEndKeyOpen(end).build();
  }

  private static List<Row> generateSegmentResult(String prefix, int startIndex, int count) {
    return generateSegmentResult(prefix, startIndex, count, false);
  }

  private static List<Row> generateLargeSegmentResult(String prefix, int startIndex, int count) {
    return generateSegmentResult(prefix, startIndex, count, true);
  }

  private static List<Row> generateSegmentResult(
      String prefix, int startIndex, int count, boolean largeRow) {
    byte[] largeMemory = new byte[(int) DEFAULT_ROW_SIZE];
    return IntStream.range(startIndex, startIndex + count)
        .mapToObj(
            i -> {
              Row.Builder builder = Row.newBuilder();
              if (!largeRow) {
                builder.setKey(generateByteString(prefix, i));
              } else {
                builder
                    .setKey(generateByteString(prefix, i))
                    .addFamilies(
                        Family.newBuilder()
                            .setName("Family")
                            .addColumns(
                                Column.newBuilder()
                                    .setQualifier(ByteString.copyFromUtf8("LargeMemoryRow"))
                                    .addCells(
                                        Cell.newBuilder()
                                            .setValue(ByteString.copyFrom(largeMemory))
                                            .setTimestampMicros(System.currentTimeMillis())
                                            .build())
                                    .build())
                            .build());
              }
              return builder.build();
            })
        .collect(Collectors.toList());
  }

  private static ByteString generateByteString(String prefix, int index) {
    return ByteString.copyFromUtf8(prefix + String.format("%05d", index));
  }

  private static class MultipleAnswer<T> implements Answer<T> {

    private final List<Row> rows = new ArrayList<>();
    private StreamController streamController;

    MultipleAnswer(List<List<Row>> expectedRows) {
      this(expectedRows, null);
    }

    MultipleAnswer(List<List<Row>> expectedRows, StreamController streamController) {
      rows.addAll(expectedRows.stream().flatMap(row -> row.stream()).collect(Collectors.toList()));
      this.streamController = streamController;
    }

    @Override
    public T answer(InvocationOnMock invocation) throws Throwable {
      if (streamController != null) {
        ((ResponseObserver) invocation.getArgument(1)).onStart(streamController);
      }
      long rowLimit = ((Query) invocation.getArgument(0)).toProto(requestContext).getRowsLimit();
      for (long i = 0; i < rowLimit; i++) {
        if (rows.isEmpty()) {
          break;
        }
        Row row = rows.remove(0);
        ((ResponseObserver) invocation.getArgument(1)).onResponse(row);
        if (cancelled.get()) {
          ((ResponseObserver) invocation.getArgument(1)).onComplete();
          cancelled.set(false);
          return null;
        }
      }
      ((ResponseObserver) invocation.getArgument(1)).onComplete();
      return null;
    }
  }
}
