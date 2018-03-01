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
package org.apache.beam.sdk.io.gcp.spanner;

import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.spanner.FakeBatchTransactionId;
import com.google.cloud.spanner.FakePartitionFactory;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Unit tests for {@link SpannerIO}. */
@RunWith(JUnit4.class) public class SpannerIOReadTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create()
      .enableAbandonedNodeEnforcement(false);
  @Rule public final transient ExpectedException thrown = ExpectedException.none();

  private FakeServiceFactory serviceFactory;
  private BatchReadOnlyTransaction mockBatchTx;

  private static final Type FAKE_TYPE = Type
      .struct(Type.StructField.of("id", Type.int64()), Type.StructField.of("name", Type.string()));

  private static final List<Struct> FAKE_ROWS = Arrays.asList(
      Struct.newBuilder().add("id", Value.int64(1)).add("name", Value.string("Alice")).build(),
      Struct.newBuilder().add("id", Value.int64(2)).add("name", Value.string("Bob")).build(),
      Struct.newBuilder().add("id", Value.int64(3)).add("name", Value.string("Carl")).build(),
      Struct.newBuilder().add("id", Value.int64(4)).add("name", Value.string("Dan")).build(),
      Struct.newBuilder().add("id", Value.int64(5)).add("name", Value.string("Evan")).build(),
      Struct.newBuilder().add("id", Value.int64(6)).add("name", Value.string("Floyd")).build());

  @Before
  @SuppressWarnings("unchecked"
  ) public void setUp() throws Exception {
    serviceFactory = new FakeServiceFactory();
    mockBatchTx = Mockito.mock(BatchReadOnlyTransaction.class);
  }

  @Test
  public void runQuery() throws Exception {
    SpannerIO.Read read = SpannerIO.read().withProjectId("test").withInstanceId("123")
        .withDatabaseId("aaa").withQuery("SELECT * FROM users").withServiceFactory(serviceFactory);

    List<Partition> fakePartitions = Arrays
        .asList(mock(Partition.class), mock(Partition.class), mock(Partition.class));

    BatchTransactionId id = mock(BatchTransactionId.class);
    Transaction tx = Transaction.create(id);
    PCollectionView<Transaction> txView = pipeline.apply(Create.of(tx))
        .apply(View.<Transaction>asSingleton());

    BatchSpannerRead.GeneratePartitionsFn fn = new BatchSpannerRead.GeneratePartitionsFn(
        read.getSpannerConfig(), txView);
    DoFnTester<ReadOperation, Partition> fnTester = DoFnTester.of(fn);
    fnTester.setSideInput(txView, GlobalWindow.INSTANCE, tx);

    when(serviceFactory.mockBatchClient().batchReadOnlyTransaction(id)).thenReturn(mockBatchTx);
    when(mockBatchTx.partitionQuery(any(PartitionOptions.class), any(Statement.class)))
        .thenReturn(fakePartitions);

    List<Partition> result = fnTester.processBundle(read.getReadOperation());
    assertThat(result, Matchers.containsInAnyOrder(fakePartitions.toArray()));

    verify(serviceFactory.mockBatchClient()).batchReadOnlyTransaction(id);
    verify(mockBatchTx)
        .partitionQuery(any(PartitionOptions.class), eq(Statement.of("SELECT * " + "FROM users")));
  }

  @Test
  public void runRead() throws Exception {
    SpannerIO.Read read = SpannerIO.read().withProjectId("test").withInstanceId("123")
        .withDatabaseId("aaa").withTable("users").withColumns("id", "name")
        .withServiceFactory(serviceFactory);

    List<Partition> fakePartitions = Arrays
        .asList(mock(Partition.class), mock(Partition.class), mock(Partition.class));

    BatchTransactionId id = mock(BatchTransactionId.class);
    Transaction tx = Transaction.create(id);
    PCollectionView<Transaction> txView = pipeline.apply(Create.of(tx))
        .apply(View.<Transaction>asSingleton());

    BatchSpannerRead.GeneratePartitionsFn fn = new BatchSpannerRead.GeneratePartitionsFn(
        read.getSpannerConfig(), txView);
    DoFnTester<ReadOperation, Partition> fnTester = DoFnTester.of(fn);
    fnTester.setSideInput(txView, GlobalWindow.INSTANCE, tx);

    when(serviceFactory.mockBatchClient().batchReadOnlyTransaction(id)).thenReturn(mockBatchTx);
    when(mockBatchTx.partitionRead(any(PartitionOptions.class), eq("users"), eq(KeySet.all()),
        eq(Arrays.asList("id", "name")))).thenReturn(fakePartitions);

    List<Partition> result = fnTester.processBundle(read.getReadOperation());
    assertThat(result, Matchers.containsInAnyOrder(fakePartitions.toArray()));

    verify(serviceFactory.mockBatchClient()).batchReadOnlyTransaction(id);
    verify(mockBatchTx).partitionRead(any(PartitionOptions.class), eq("users"), eq(KeySet.all()),
        eq(Arrays.asList("id", "name")));
  }

  @Test
  public void runReadUsingIndex() throws Exception {
    SpannerIO.Read read = SpannerIO.read().withProjectId("test").withInstanceId("123")
        .withDatabaseId("aaa").withTimestamp(Timestamp.now()).withTable("users")
        .withColumns("id", "name").withIndex("theindex").withServiceFactory(serviceFactory);

    List<Partition> fakePartitions = Arrays
        .asList(mock(Partition.class), mock(Partition.class), mock(Partition.class));

    FakeBatchTransactionId id = new FakeBatchTransactionId("one");
    Transaction tx = Transaction.create(id);
    PCollectionView<Transaction> txView = pipeline.apply(Create.of(tx))
        .apply(View.<Transaction>asSingleton());

    BatchSpannerRead.GeneratePartitionsFn fn = new BatchSpannerRead.GeneratePartitionsFn(
        read.getSpannerConfig(), txView);
    DoFnTester<ReadOperation, Partition> fnTester = DoFnTester.of(fn);
    fnTester.setSideInput(txView, GlobalWindow.INSTANCE, tx);

    when(serviceFactory.mockBatchClient().batchReadOnlyTransaction(id)).thenReturn(mockBatchTx);
    when(mockBatchTx
        .partitionReadUsingIndex(any(PartitionOptions.class), eq("users"), eq("theindex"),
            eq(KeySet.all()), eq(Arrays.asList("id", "name")))).thenReturn(fakePartitions);

    List<Partition> result = fnTester.processBundle(read.getReadOperation());
    assertThat(result, Matchers.containsInAnyOrder(fakePartitions.toArray()));

    verify(serviceFactory.mockBatchClient()).batchReadOnlyTransaction(id);
    verify(mockBatchTx)
        .partitionReadUsingIndex(any(PartitionOptions.class), eq("users"), eq("theindex"),
            eq(KeySet.all()), eq(Arrays.asList("id", "name")));
  }

  @Test
  public void readPipeline() throws Exception {
    Timestamp timestamp = Timestamp.ofTimeMicroseconds(12345);
    TimestampBound timestampBound = TimestampBound.ofReadTimestamp(timestamp);

    SpannerConfig spannerConfig = SpannerConfig.create().withProjectId("test").withInstanceId("123")
        .withDatabaseId("aaa").withServiceFactory(serviceFactory);

    PCollection<Struct> one = pipeline.apply("read q",
        SpannerIO.read().withSpannerConfig(spannerConfig).withQuery("SELECT * FROM users")
            .withTimestampBound(timestampBound));
    FakeBatchTransactionId txId = new FakeBatchTransactionId("readPipelineTest");
    when(mockBatchTx.getBatchTransactionId()).thenReturn(txId);

    when(serviceFactory.mockBatchClient().batchReadOnlyTransaction(timestampBound))
        .thenReturn(mockBatchTx);

    when(serviceFactory.mockBatchClient().batchReadOnlyTransaction(any(BatchTransactionId.class)))
        .thenReturn(mockBatchTx);

    Partition fakePartition = FakePartitionFactory.createFakeQueryPartition(ByteString
        .copyFromUtf8("one"));
    when(mockBatchTx
        .partitionQuery(any(PartitionOptions.class), eq(Statement.of("SELECT * FROM users"))))
        .thenReturn(Arrays.asList(fakePartition, fakePartition));

    when(mockBatchTx.execute(any(Partition.class)))
        .thenReturn(ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(0, 2)),
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(2, 6)));

    PAssert.that(one).containsInAnyOrder(FAKE_ROWS);

    pipeline.run();
  }

  @Test
  public void readAllPipeline() throws Exception {
    Timestamp timestamp = Timestamp.ofTimeMicroseconds(12345);
    TimestampBound timestampBound = TimestampBound.ofReadTimestamp(timestamp);

    SpannerConfig spannerConfig = SpannerConfig.create().withProjectId("test").withInstanceId("123")
        .withDatabaseId("aaa").withServiceFactory(serviceFactory);

    PCollectionView<Transaction> tx = pipeline.apply("tx",
        SpannerIO.createTransaction().withSpannerConfig(spannerConfig)
            .withTimestampBound(timestampBound));

    PCollection<ReadOperation> reads = pipeline.apply(Create
        .of(ReadOperation.create().withQuery("SELECT * FROM users"),
            ReadOperation.create().withTable("users").withColumns("id", "name")));

    PCollection<Struct> one = reads.apply("read all",
        SpannerIO.readAll().withSpannerConfig(spannerConfig).withTransaction(tx));

    BatchTransactionId txId = new FakeBatchTransactionId("tx");
    when(mockBatchTx.getBatchTransactionId()).thenReturn(txId);

    when(serviceFactory.mockBatchClient().batchReadOnlyTransaction(timestampBound))
        .thenReturn(mockBatchTx);

    when(serviceFactory.mockBatchClient().batchReadOnlyTransaction(any(BatchTransactionId.class)))
        .thenReturn(mockBatchTx);

    Partition fakePartition = FakePartitionFactory.createFakeReadPartition(ByteString
        .copyFromUtf8("partition"));
    when(mockBatchTx
        .partitionQuery(any(PartitionOptions.class), eq(Statement.of("SELECT * FROM users"))))
        .thenReturn(Arrays.asList(fakePartition, fakePartition));
    when(mockBatchTx.partitionRead(any(PartitionOptions.class), eq("users"), eq(KeySet.all()),
        eq(Arrays.asList("id", "name")))).thenReturn(Arrays.asList(fakePartition));

    when(mockBatchTx.execute(any(Partition.class)))
        .thenReturn(ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(0, 2)),
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(2, 4)),
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(4, 6)));

    PAssert.that(one).containsInAnyOrder(FAKE_ROWS);

    pipeline.run();
  }
}
