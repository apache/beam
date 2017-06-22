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

  private Type fakeType = Type.struct(Type.StructField.of("id", Type.int64()),
      Type.StructField.of("name", Type.string()));

  private List<Struct> fakeRows = Arrays.asList(
      Struct.newBuilder().add("id", Value.int64(1)).add("name", Value.string("Alice")).build(),
      Struct.newBuilder().add("id", Value.int64(2)).add("name", Value.string("Bob")).build());

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    serviceFactory = new FakeServiceFactory();
    mockTx = Mockito.mock(ReadOnlyTransaction.class);
  }

  @Test
  public void emptyTransform() throws Exception {
    SpannerIO.Read read = SpannerIO.read();
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("requires instance id to be set with");
    read.validate(null);
  }

  @Test
  public void emptyInstanceId() throws Exception {
    SpannerIO.Read read = SpannerIO.read().withDatabaseId("123");
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("requires instance id to be set with");
    read.validate(null);
  }

  @Test
  public void emptyDatabaseId() throws Exception {
    SpannerIO.Read read = SpannerIO.read().withInstanceId("123");
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("requires database id to be set with");
    read.validate(null);
  }

  @Test
  public void emptyQuery() throws Exception {
    SpannerIO.Read read =
        SpannerIO.read().withInstanceId("123").withDatabaseId("aaa").withTimestamp(Timestamp.now());
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("requires configuring query or read operation");
    read.validate(null);
  }

  @Test
  public void emptyColumns() throws Exception {
    SpannerIO.Read read =
        SpannerIO.read()
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withTimestamp(Timestamp.now())
            .withTable("users");
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("requires a list of columns");
    read.validate(null);
  }

  @Test
  public void validRead() throws Exception {
    SpannerIO.Read read =
        SpannerIO.read()
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withTimestamp(Timestamp.now())
            .withTable("users")
            .withColumns("id", "name", "email");
    read.validate(null);
  }

  @Test
  public void validQuery() throws Exception {
    SpannerIO.Read read =
        SpannerIO.read()
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withTimestamp(Timestamp.now())
            .withQuery("SELECT * FROM users");
    read.validate(null);
  }

  @Test
  public void runQuery() throws Exception {
    SpannerIO.Read read =
        SpannerIO.read()
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withTimestamp(Timestamp.now())
            .withQuery("SELECT * FROM users")
            .withServiceFactory(serviceFactory);

    NaiveSpannerReadFn readFn = new NaiveSpannerReadFn(read);
    DoFnTester<Object, Struct> fnTester = DoFnTester.of(readFn);

    when(serviceFactory.mockDatabaseClient().readOnlyTransaction(any(TimestampBound.class)))
        .thenReturn(mockTx);
    when(mockTx.executeQuery(any(Statement.class)))
        .thenReturn(ResultSets.forRows(fakeType, fakeRows));

    List<Struct> result = fnTester.processBundle(1);
    assertThat(result, Matchers.<Struct>iterableWithSize(2));

    verify(serviceFactory.mockDatabaseClient()).readOnlyTransaction(TimestampBound
        .strong());
    verify(mockTx).executeQuery(Statement.of("SELECT * FROM users"));
  }

  @Test
  public void runRead() throws Exception {
    SpannerIO.Read read =
        SpannerIO.read()
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withTimestamp(Timestamp.now())
            .withTable("users")
            .withColumns("id", "name")
            .withServiceFactory(serviceFactory);

    NaiveSpannerReadFn readFn = new NaiveSpannerReadFn(read);
    DoFnTester<Object, Struct> fnTester = DoFnTester.of(readFn);

    when(serviceFactory.mockDatabaseClient().readOnlyTransaction(any(TimestampBound.class)))
        .thenReturn(mockTx);
    when(mockTx.read("users", KeySet.all(), Arrays.asList("id", "name")))
        .thenReturn(ResultSets.forRows(fakeType, fakeRows));

    List<Struct> result = fnTester.processBundle(1);
    assertThat(result, Matchers.<Struct>iterableWithSize(2));

    verify(serviceFactory.mockDatabaseClient()).readOnlyTransaction(TimestampBound.strong());
    verify(mockTx).read("users", KeySet.all(), Arrays.asList("id", "name"));
  }

  @Test
  public void runReadUsingIndex() throws Exception {
    SpannerIO.Read read =
        SpannerIO.read()
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withTimestamp(Timestamp.now())
            .withTable("users")
            .withColumns("id", "name")
            .withIndex("theindex")
            .withServiceFactory(serviceFactory);

    NaiveSpannerReadFn readFn = new NaiveSpannerReadFn(read);
    DoFnTester<Object, Struct> fnTester = DoFnTester.of(readFn);

    when(serviceFactory.mockDatabaseClient().readOnlyTransaction(any(TimestampBound.class)))
        .thenReturn(mockTx);
    when(mockTx.readUsingIndex("users", "theindex", KeySet.all(), Arrays.asList("id", "name")))
        .thenReturn(ResultSets.forRows(fakeType, fakeRows));

    List<Struct> result = fnTester.processBundle(1);
    assertThat(result, Matchers.<Struct>iterableWithSize(2));

    verify(serviceFactory.mockDatabaseClient()).readOnlyTransaction(TimestampBound.strong());
    verify(mockTx).readUsingIndex("users", "theindex", KeySet.all(), Arrays.asList("id", "name"));
  }

  @Test
  @Category(NeedsRunner.class)
  public void readPipeline() throws Exception {
    Timestamp timestamp = Timestamp.ofTimeMicroseconds(12345);

    PCollectionView<Transaction> tx = pipeline
        .apply("tx", SpannerIO.createTransaction()
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withServiceFactory(serviceFactory));

    PCollection<Struct> one = pipeline.apply("read q", SpannerIO.read()
        .withInstanceId("123")
        .withDatabaseId("aaa")
        .withTimestamp(Timestamp.now())
        .withQuery("SELECT * FROM users")
        .withServiceFactory(serviceFactory)
        .withTransaction(tx));
    PCollection<Struct> two = pipeline.apply("read r", SpannerIO.read()
        .withInstanceId("123")
        .withDatabaseId("aaa")
        .withTimestamp(Timestamp.now())
        .withTable("users")
        .withColumns("id", "name")
        .withServiceFactory(serviceFactory)
        .withTransaction(tx));

    when(serviceFactory.mockDatabaseClient().readOnlyTransaction(any(TimestampBound.class)))
        .thenReturn(mockTx);

    when(mockTx.executeQuery(Statement.of("SELECT 1"))).thenReturn(ResultSets.forRows(Type.struct(),
        Collections.<Struct>emptyList()));

    when(mockTx.executeQuery(Statement.of("SELECT * FROM users")))
        .thenReturn(ResultSets.forRows(fakeType, fakeRows));
    when(mockTx.read("users", KeySet.all(), Arrays.asList("id", "name")))
        .thenReturn(ResultSets.forRows(fakeType, fakeRows));
    when(mockTx.getReadTimestamp()).thenReturn(timestamp);

    PAssert.that(one).containsInAnyOrder(fakeRows);
    PAssert.that(two).containsInAnyOrder(fakeRows);

    pipeline.run();

    verify(serviceFactory.mockDatabaseClient(), times(2))
        .readOnlyTransaction(TimestampBound.ofReadTimestamp(timestamp));
  }
}
