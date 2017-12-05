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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Unit tests for {@link SpannerIO}. */
@RunWith(JUnit4.class)
public class SpannerIOReadTest implements Serializable {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule
  public final transient ExpectedException thrown = ExpectedException.none();

  private FakeServiceFactory serviceFactory;
  private ReadOnlyTransaction mockTx;

  private static final Type FAKE_TYPE =
      Type.struct(
          Type.StructField.of("id", Type.int64()), Type.StructField.of("name", Type.string()));

  private static final List<Struct> FAKE_ROWS =
      Arrays.asList(
          Struct.newBuilder().add("id", Value.int64(1)).add("name", Value.string("Alice")).build(),
          Struct.newBuilder().add("id", Value.int64(2)).add("name", Value.string("Bob")).build(),
          Struct.newBuilder().add("id", Value.int64(3)).add("name", Value.string("Carl")).build(),
          Struct.newBuilder().add("id", Value.int64(4)).add("name", Value.string("Dan")).build());

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    serviceFactory = new FakeServiceFactory();
    mockTx = Mockito.mock(ReadOnlyTransaction.class);
  }

  @Test
  public void runQuery() throws Exception {
    SpannerIO.Read read =
        SpannerIO.read()
            .withProjectId("test")
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withQuery("SELECT * FROM users")
            .withServiceFactory(serviceFactory);

    NaiveSpannerReadFn readFn = new NaiveSpannerReadFn(read.getSpannerConfig());
    DoFnTester<ReadOperation, Struct> fnTester = DoFnTester.of(readFn);

    when(serviceFactory.mockDatabaseClient().readOnlyTransaction(any(TimestampBound.class)))
        .thenReturn(mockTx);
    when(mockTx.executeQuery(any(Statement.class)))
        .thenReturn(ResultSets.forRows(FAKE_TYPE, FAKE_ROWS));

    List<Struct> result = fnTester.processBundle(read.getReadOperation());
    assertThat(result, Matchers.containsInAnyOrder(FAKE_ROWS.toArray()));

    verify(serviceFactory.mockDatabaseClient()).readOnlyTransaction(TimestampBound
        .strong());
    verify(mockTx).executeQuery(Statement.of("SELECT * FROM users"));
  }

  @Test
  public void runRead() throws Exception {
    SpannerIO.Read read =
        SpannerIO.read()
            .withProjectId("test")
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withTable("users")
            .withColumns("id", "name")
            .withServiceFactory(serviceFactory);

    NaiveSpannerReadFn readFn = new NaiveSpannerReadFn(read.getSpannerConfig());
    DoFnTester<ReadOperation, Struct> fnTester = DoFnTester.of(readFn);

    when(serviceFactory.mockDatabaseClient().readOnlyTransaction(any(TimestampBound.class)))
        .thenReturn(mockTx);
    when(mockTx.read("users", KeySet.all(), Arrays.asList("id", "name")))
        .thenReturn(ResultSets.forRows(FAKE_TYPE, FAKE_ROWS));

    List<Struct> result = fnTester.processBundle(read.getReadOperation());
    assertThat(result, Matchers.containsInAnyOrder(FAKE_ROWS.toArray()));

    verify(serviceFactory.mockDatabaseClient()).readOnlyTransaction(TimestampBound.strong());
    verify(mockTx).read("users", KeySet.all(), Arrays.asList("id", "name"));
  }

  @Test
  public void runReadUsingIndex() throws Exception {
    SpannerIO.Read read =
        SpannerIO.read()
            .withProjectId("test")
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withTimestamp(Timestamp.now())
            .withTable("users")
            .withColumns("id", "name")
            .withIndex("theindex")
            .withServiceFactory(serviceFactory);

    NaiveSpannerReadFn readFn = new NaiveSpannerReadFn(read.getSpannerConfig());
    DoFnTester<ReadOperation, Struct> fnTester = DoFnTester.of(readFn);

    when(serviceFactory.mockDatabaseClient().readOnlyTransaction(any(TimestampBound.class)))
        .thenReturn(mockTx);
    when(mockTx.readUsingIndex("users", "theindex", KeySet.all(), Arrays.asList("id", "name")))
        .thenReturn(ResultSets.forRows(FAKE_TYPE, FAKE_ROWS));

    List<Struct> result = fnTester.processBundle(read.getReadOperation());
    assertThat(result, Matchers.containsInAnyOrder(FAKE_ROWS.toArray()));

    verify(serviceFactory.mockDatabaseClient()).readOnlyTransaction(TimestampBound.strong());
    verify(mockTx).readUsingIndex("users", "theindex", KeySet.all(), Arrays.asList("id", "name"));
  }

  @Test
  @Category(NeedsRunner.class)
  public void readPipeline() throws Exception {
    Timestamp timestamp = Timestamp.ofTimeMicroseconds(12345);

    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId("test")
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withServiceFactory(serviceFactory);

    PCollectionView<Transaction> tx =
        pipeline.apply("tx", SpannerIO.createTransaction().withSpannerConfig(spannerConfig));

    PCollection<Struct> one =
        pipeline.apply(
            "read q",
            SpannerIO.read()
                .withSpannerConfig(spannerConfig)
                .withQuery("SELECT * FROM users")
                .withTransaction(tx));
    PCollection<Struct> two =
        pipeline.apply(
            "read r",
            SpannerIO.read()
                .withSpannerConfig(spannerConfig)
                .withTimestamp(Timestamp.now())
                .withTable("users")
                .withColumns("id", "name")
                .withTransaction(tx));

    when(serviceFactory.mockDatabaseClient().readOnlyTransaction(any(TimestampBound.class)))
        .thenReturn(mockTx);

    when(mockTx.executeQuery(Statement.of("SELECT 1"))).thenReturn(ResultSets.forRows(Type.struct(),
        Collections.<Struct>emptyList()));

    when(mockTx.executeQuery(Statement.of("SELECT * FROM users")))
        .thenReturn(ResultSets.forRows(FAKE_TYPE, FAKE_ROWS));
    when(mockTx.read("users", KeySet.all(), Arrays.asList("id", "name")))
        .thenReturn(ResultSets.forRows(FAKE_TYPE, FAKE_ROWS));
    when(mockTx.getReadTimestamp()).thenReturn(timestamp);

    PAssert.that(one).containsInAnyOrder(FAKE_ROWS);
    PAssert.that(two).containsInAnyOrder(FAKE_ROWS);

    pipeline.run();

    verify(serviceFactory.mockDatabaseClient(), times(2))
        .readOnlyTransaction(TimestampBound.ofReadTimestamp(timestamp));
  }

  @Test
  @Category(NeedsRunner.class)
  public void readAllPipeline() throws Exception {
    Timestamp timestamp = Timestamp.ofTimeMicroseconds(12345);

    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId("test")
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withServiceFactory(serviceFactory);

    PCollectionView<Transaction> tx =
        pipeline.apply("tx", SpannerIO.createTransaction().withSpannerConfig(spannerConfig));

    PCollection<ReadOperation> reads =
        pipeline.apply(
            Create.of(
                ReadOperation.create().withQuery("SELECT * FROM users"),
                ReadOperation.create().withTable("users").withColumns("id", "name")));

    PCollection<Struct> one =
        reads.apply(
            "read all", SpannerIO.readAll().withSpannerConfig(spannerConfig).withTransaction(tx));

    when(serviceFactory.mockDatabaseClient().readOnlyTransaction(any(TimestampBound.class)))
        .thenReturn(mockTx);

    when(mockTx.executeQuery(Statement.of("SELECT 1")))
        .thenReturn(ResultSets.forRows(Type.struct(), Collections.<Struct>emptyList()));

    when(mockTx.executeQuery(Statement.of("SELECT * FROM users")))
        .thenReturn(ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(0, 2)));
    when(mockTx.read("users", KeySet.all(), Arrays.asList("id", "name")))
        .thenReturn(ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(2, 4)));
    when(mockTx.getReadTimestamp()).thenReturn(timestamp);

    PAssert.that(one).containsInAnyOrder(FAKE_ROWS);

    pipeline.run();

    verify(serviceFactory.mockDatabaseClient(), times(2))
        .readOnlyTransaction(TimestampBound.ofReadTimestamp(timestamp));
  }
}
