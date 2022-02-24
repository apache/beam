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
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Queue;
import org.apache.beam.runners.core.metrics.GcpResourceIdentifiers;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoMetricName;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO.BigtableSource;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.io.range.ByteKeyRangeTracker;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.Default.Byte;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
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
   * as expected. This test checks that the proper Rows are returned when the buffer is not filled.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testReadBelowMiniBatchLimit() throws IOException {
    ByteKey start = ByteKey.copyFrom("a".getBytes(StandardCharsets.UTF_8));
    ByteKey end = ByteKey.copyFrom("b".getBytes(StandardCharsets.UTF_8));
    when(mockBigtableSource.getRanges()).thenReturn(Arrays.asList(ByteKeyRange.of(start, end)));
    when(mockBigtableSource.getTableId()).thenReturn(StaticValueProvider.of(TABLE_ID));
    @SuppressWarnings("unchecked")
    ResultScanner<Row> mockResultScanner = Mockito.mock(ResultScanner.class);
    Row[] expectedRows = new Row[] {Row.newBuilder().setKey(ByteString.copyFromUtf8("a")).build()};
    when(mockResultScanner.next(MINI_BATCH_ROW_LIMIT)).thenReturn(expectedRows).thenReturn(null);
    when(mockResultScanner.available()).thenReturn(1).thenReturn(0);
    when(mockBigtableDataClient.readRows(any(ReadRowsRequest.class))).thenReturn(mockResultScanner);
    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableReaderImpl(mockSession, mockBigtableSource);

    underTest.start();
    Assert.assertEquals(expectedRows[0], underTest.getCurrentRow());
    Assert.assertFalse(underTest.advance());
    underTest.close();

    verify(mockResultScanner, times(1)).close();
    verifyMetricWasSet("google.bigtable.v2.ReadRows", "ok", 1);
  }

  /**
   * This test ensures that protobuf creation and interactions with {@link BigtableDataClient} work
   * as expected. This test checks that the proper Rows are returned when the buffer is filled.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testReadAboveMiniBatchLimit() throws IOException {
    ByteKey start = ByteKey.copyFrom("a".getBytes(StandardCharsets.UTF_8));
    //ByteKey filler = ByteKey.copyFrom("z".getBytes(StandardCharsets.UTF_8));
    ByteKey end = ByteKey.copyFrom("b".getBytes(StandardCharsets.UTF_8));

    //mockBigtableSource.

    //ByteKeyRange How do I make a ByteKeyRange of 101 size?

    when(mockBigtableSource.getRanges()).thenReturn(Arrays.asList(ByteKeyRange.of(start, end)));
    when(mockBigtableSource.getTableId()).thenReturn(StaticValueProvider.of(TABLE_ID));
    @SuppressWarnings("unchecked")
    ResultScanner<Row> mockResultScanner = Mockito.mock(ResultScanner.class);
    Row[] expectedRows = new Row[] {Row.newBuilder().setKey(ByteString.copyFromUtf8("a")).build(),
        Row.newBuilder().setKey(ByteString.copyFromUtf8("b")).build()};
    when(mockResultScanner.next(MINI_BATCH_ROW_LIMIT)).thenReturn(expectedRows).thenReturn(null);
    when(mockResultScanner.available()).thenReturn(2).thenReturn(0);
    when(mockBigtableDataClient.readRows(any(ReadRowsRequest.class))).thenReturn(mockResultScanner);
    BigtableService.Reader underTest =
        new BigtableServiceImpl.BigtableReaderImpl(mockSession, mockBigtableSource);

    underTest.start();
    Assert.assertEquals(expectedRows[0], underTest.getCurrentRow());
    Assert.assertTrue(underTest.advance());
    Assert.assertEquals(expectedRows[1], underTest.getCurrentRow());
    Assert.assertFalse(underTest.advance());
    underTest.close();

    verify(mockResultScanner, times(1)).close();
    verifyMetricWasSet("google.bigtable.v2.ReadRows", "ok", 1);
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
