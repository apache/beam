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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
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
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Unit tests for {@link SpannerIO}. */
@RunWith(JUnit4.class)
public class SpannerIOReadTest implements Serializable {

  @Rule
  public final transient TestPipeline pipeline =
      TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Rule public final transient ExpectedException thrown = ExpectedException.none();

  private FakeServiceFactory serviceFactory;
  private BatchReadOnlyTransaction mockBatchTx;

  private static final Type FAKE_TYPE =
      Type.struct(
          Type.StructField.of("id", Type.int64()), Type.StructField.of("name", Type.string()));

  private static final List<Struct> FAKE_ROWS =
      Arrays.asList(
          Struct.newBuilder().set("id").to(Value.int64(1)).set("name").to("Alice").build(),
          Struct.newBuilder().set("id").to(Value.int64(2)).set("name").to("Bob").build(),
          Struct.newBuilder().set("id").to(Value.int64(3)).set("name").to("Carl").build(),
          Struct.newBuilder().set("id").to(Value.int64(4)).set("name").to("Dan").build(),
          Struct.newBuilder().set("id").to(Value.int64(5)).set("name").to("Evan").build(),
          Struct.newBuilder().set("id").to(Value.int64(6)).set("name").to("Floyd").build());

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    serviceFactory = new FakeServiceFactory();
    mockBatchTx = Mockito.mock(BatchReadOnlyTransaction.class);
  }

  @Test
  public void runQuery() throws Exception {
    Timestamp timestamp = Timestamp.ofTimeMicroseconds(12345);
    TimestampBound timestampBound = TimestampBound.ofReadTimestamp(timestamp);

    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId("test")
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withServiceFactory(serviceFactory);

    PCollection<Struct> one =
        pipeline.apply(
            "read q",
            SpannerIO.read()
                .withSpannerConfig(spannerConfig)
                .withQuery("SELECT * FROM users")
                .withTimestampBound(timestampBound));

    FakeBatchTransactionId id = new FakeBatchTransactionId("runQueryTest");
    when(mockBatchTx.getBatchTransactionId()).thenReturn(id);

    when(serviceFactory.mockBatchClient().batchReadOnlyTransaction(timestampBound))
        .thenReturn(mockBatchTx);
    when(serviceFactory.mockBatchClient().batchReadOnlyTransaction(any(BatchTransactionId.class)))
        .thenReturn(mockBatchTx);

    Partition fakePartition =
        FakePartitionFactory.createFakeQueryPartition(ByteString.copyFromUtf8("one"));

    when(mockBatchTx.partitionQuery(
            any(PartitionOptions.class), eq(Statement.of("SELECT * FROM users"))))
        .thenReturn(Arrays.asList(fakePartition, fakePartition));
    when(mockBatchTx.execute(any(Partition.class)))
        .thenReturn(
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(0, 2)),
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(2, 6)));

    PAssert.that(one).containsInAnyOrder(FAKE_ROWS);

    pipeline.run();
  }

  @Test
  public void runRead() throws Exception {
    Timestamp timestamp = Timestamp.ofTimeMicroseconds(12345);
    TimestampBound timestampBound = TimestampBound.ofReadTimestamp(timestamp);

    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId("test")
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withServiceFactory(serviceFactory);

    PCollection<Struct> one =
        pipeline.apply(
            "read q",
            SpannerIO.read()
                .withSpannerConfig(spannerConfig)
                .withTable("users")
                .withColumns("id", "name")
                .withTimestampBound(timestampBound));

    FakeBatchTransactionId id = new FakeBatchTransactionId("runReadTest");
    when(mockBatchTx.getBatchTransactionId()).thenReturn(id);

    when(serviceFactory.mockBatchClient().batchReadOnlyTransaction(timestampBound))
        .thenReturn(mockBatchTx);
    when(serviceFactory.mockBatchClient().batchReadOnlyTransaction(any(BatchTransactionId.class)))
        .thenReturn(mockBatchTx);

    Partition fakePartition =
        FakePartitionFactory.createFakeReadPartition(ByteString.copyFromUtf8("one"));

    when(mockBatchTx.partitionRead(
            any(PartitionOptions.class),
            eq("users"),
            eq(KeySet.all()),
            eq(Arrays.asList("id", "name"))))
        .thenReturn(Arrays.asList(fakePartition, fakePartition, fakePartition));
    when(mockBatchTx.execute(any(Partition.class)))
        .thenReturn(
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(0, 2)),
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(2, 4)),
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(4, 6)));

    PAssert.that(one).containsInAnyOrder(FAKE_ROWS);

    pipeline.run();
  }

  @Test
  public void runReadUsingIndex() throws Exception {
    Timestamp timestamp = Timestamp.ofTimeMicroseconds(12345);
    TimestampBound timestampBound = TimestampBound.ofReadTimestamp(timestamp);

    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId("test")
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withServiceFactory(serviceFactory);

    PCollection<Struct> one =
        pipeline.apply(
            "read q",
            SpannerIO.read()
                .withTimestamp(Timestamp.now())
                .withSpannerConfig(spannerConfig)
                .withTable("users")
                .withColumns("id", "name")
                .withIndex("theindex")
                .withTimestampBound(timestampBound));

    FakeBatchTransactionId id = new FakeBatchTransactionId("runReadUsingIndexTest");
    when(mockBatchTx.getBatchTransactionId()).thenReturn(id);

    when(serviceFactory.mockBatchClient().batchReadOnlyTransaction(timestampBound))
        .thenReturn(mockBatchTx);
    when(serviceFactory.mockBatchClient().batchReadOnlyTransaction(any(BatchTransactionId.class)))
        .thenReturn(mockBatchTx);

    Partition fakePartition =
        FakePartitionFactory.createFakeReadPartition(ByteString.copyFromUtf8("one"));

    when(serviceFactory.mockBatchClient().batchReadOnlyTransaction(id)).thenReturn(mockBatchTx);
    when(mockBatchTx.partitionReadUsingIndex(
            any(PartitionOptions.class),
            eq("users"),
            eq("theindex"),
            eq(KeySet.all()),
            eq(Arrays.asList("id", "name"))))
        .thenReturn(Arrays.asList(fakePartition, fakePartition, fakePartition));

    when(mockBatchTx.execute(any(Partition.class)))
        .thenReturn(
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(0, 2)),
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(2, 4)),
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(4, 6)));

    PAssert.that(one).containsInAnyOrder(FAKE_ROWS);

    pipeline.run();
  }

  @Test
  public void readPipeline() throws Exception {
    Timestamp timestamp = Timestamp.ofTimeMicroseconds(12345);
    TimestampBound timestampBound = TimestampBound.ofReadTimestamp(timestamp);

    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId("test")
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withServiceFactory(serviceFactory);

    PCollection<Struct> one =
        pipeline.apply(
            "read q",
            SpannerIO.read()
                .withSpannerConfig(spannerConfig)
                .withQuery("SELECT * FROM users")
                .withTimestampBound(timestampBound));
    FakeBatchTransactionId txId = new FakeBatchTransactionId("readPipelineTest");
    when(mockBatchTx.getBatchTransactionId()).thenReturn(txId);

    when(serviceFactory.mockBatchClient().batchReadOnlyTransaction(timestampBound))
        .thenReturn(mockBatchTx);

    when(serviceFactory.mockBatchClient().batchReadOnlyTransaction(any(BatchTransactionId.class)))
        .thenReturn(mockBatchTx);

    Partition fakePartition =
        FakePartitionFactory.createFakeQueryPartition(ByteString.copyFromUtf8("one"));
    when(mockBatchTx.partitionQuery(
            any(PartitionOptions.class), eq(Statement.of("SELECT * FROM users"))))
        .thenReturn(Arrays.asList(fakePartition, fakePartition));

    when(mockBatchTx.execute(any(Partition.class)))
        .thenReturn(
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(0, 2)),
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(2, 6)));

    PAssert.that(one).containsInAnyOrder(FAKE_ROWS);

    pipeline.run();
  }

  @Test
  public void readAllPipeline() throws Exception {
    Timestamp timestamp = Timestamp.ofTimeMicroseconds(12345);
    TimestampBound timestampBound = TimestampBound.ofReadTimestamp(timestamp);

    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId("test")
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withServiceFactory(serviceFactory);

    PCollectionView<Transaction> tx =
        pipeline.apply(
            "tx",
            SpannerIO.createTransaction()
                .withSpannerConfig(spannerConfig)
                .withTimestampBound(timestampBound));

    PCollection<ReadOperation> reads =
        pipeline.apply(
            Create.of(
                ReadOperation.create().withQuery("SELECT * FROM users"),
                ReadOperation.create().withTable("users").withColumns("id", "name")));

    PCollection<Struct> one =
        reads.apply(
            "read all", SpannerIO.readAll().withSpannerConfig(spannerConfig).withTransaction(tx));

    BatchTransactionId txId = new FakeBatchTransactionId("tx");
    when(mockBatchTx.getBatchTransactionId()).thenReturn(txId);

    when(serviceFactory.mockBatchClient().batchReadOnlyTransaction(timestampBound))
        .thenReturn(mockBatchTx);

    when(serviceFactory.mockBatchClient().batchReadOnlyTransaction(any(BatchTransactionId.class)))
        .thenReturn(mockBatchTx);

    Partition fakePartition =
        FakePartitionFactory.createFakeReadPartition(ByteString.copyFromUtf8("partition"));
    when(mockBatchTx.partitionQuery(
            any(PartitionOptions.class), eq(Statement.of("SELECT * FROM users"))))
        .thenReturn(Arrays.asList(fakePartition, fakePartition));
    when(mockBatchTx.partitionRead(
            any(PartitionOptions.class),
            eq("users"),
            eq(KeySet.all()),
            eq(Arrays.asList("id", "name"))))
        .thenReturn(Arrays.asList(fakePartition));

    when(mockBatchTx.execute(any(Partition.class)))
        .thenReturn(
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(0, 2)),
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(2, 4)),
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(4, 6)));

    PAssert.that(one).containsInAnyOrder(FAKE_ROWS);

    pipeline.run();
  }
}
