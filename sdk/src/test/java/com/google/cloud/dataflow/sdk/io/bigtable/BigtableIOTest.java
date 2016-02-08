/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io.bigtable;

import static com.google.common.base.Preconditions.checkArgument;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.bigtable.v1.Mutation;
import com.google.bigtable.v1.Mutation.SetCell;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.dataflow.sdk.Pipeline.PipelineExecutionException;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.testing.ExpectedLogs;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * Unit tests for {@link BigtableIO}.
 */
@RunWith(JUnit4.class)
public class BigtableIOTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ExpectedLogs logged = ExpectedLogs.none(BigtableIO.class);

  /**
   * These tests requires a static instance of the {@link FakeBigtableService} because the writers
   * go through a serialization step when executing the test and would not affect passed-in objects
   * otherwise.
   */
  private static FakeBigtableService service;
  private static final BigtableOptions BIGTABLE_OPTIONS =
      new BigtableOptions.Builder()
          .setProjectId("project")
          .setClusterId("cluster")
          .setZoneId("zone")
          .build();
  private Coder<KV<ByteString, Iterable<Mutation>>> bigtableCoder;
  private static final TypeDescriptor<KV<ByteString, Iterable<Mutation>>> BIGTABLE_WRITE_TYPE =
      new TypeDescriptor<KV<ByteString, Iterable<Mutation>>>() {};

  @Before
  public void setup() throws Exception {
    service = new FakeBigtableService();
    bigtableCoder = TestPipeline.create().getCoderRegistry().getCoder(BIGTABLE_WRITE_TYPE);
  }

  @Test
  public void testWriteBuildsCorrectly() {
    BigtableIO.Write write =
        BigtableIO.write().withBigtableOptions(BIGTABLE_OPTIONS).withTableId("table");
    assertEquals("table", write.getTableId());
    assertEquals("project", write.getBigtableOptions().getProjectId());
    assertEquals("zone", write.getBigtableOptions().getZoneId());
    assertEquals("cluster", write.getBigtableOptions().getClusterId());
  }

  @Test
  public void testWriteBuildsCorrectlyInDifferentOrder() {
    BigtableIO.Write write =
        BigtableIO.write().withTableId("table").withBigtableOptions(BIGTABLE_OPTIONS);
    assertEquals("cluster", write.getBigtableOptions().getClusterId());
    assertEquals("project", write.getBigtableOptions().getProjectId());
    assertEquals("zone", write.getBigtableOptions().getZoneId());
    assertEquals("table", write.getTableId());
  }

  @Test
  public void testWriteValidationFailsMissingTable() {
    BigtableIO.Write write = BigtableIO.write().withBigtableOptions(BIGTABLE_OPTIONS);

    thrown.expect(IllegalArgumentException.class);

    write.validate(null);
  }

  @Test
  public void testSinkValidationFailsMissingOptions() {
    BigtableIO.Write write = BigtableIO.write().withTableId("table");

    thrown.expect(IllegalArgumentException.class);

    write.validate(null);
  }

  /** Helper function to make a single row mutation to be written. */
  private static KV<ByteString, Iterable<Mutation>> makeRowWithSetCell(String key, String value) {
    ByteString rowKey = ByteString.copyFromUtf8(key);
    Iterable<Mutation> mutations =
        ImmutableList.of(
            Mutation.newBuilder()
                .setSetCell(SetCell.newBuilder().setValue(ByteString.copyFromUtf8(value)))
                .build());
    return KV.of(rowKey, mutations);
  }

  /** Helper function to make a single bad row mutation (no set cell). */
  private static KV<ByteString, Iterable<Mutation>> makeBadRow(String key) {
    Iterable<Mutation> mutations = ImmutableList.of(Mutation.newBuilder().build());
    return KV.of(ByteString.copyFromUtf8(key), mutations);
  }

  /** Tests that a record gets written to the service and messages are logged. */
  @Test
  public void testWriting() throws Exception {
    final String table = "table";
    final String key = "key";
    final String value = "value";

    service.createTable(table);

    BigtableIO.Write write =
        BigtableIO.write()
            .withBigtableOptions(BIGTABLE_OPTIONS)
            .withTableId(table)
            .withBigtableService(service);

    TestPipeline p = TestPipeline.create();
    p.apply("single row", Create.of(makeRowWithSetCell(key, value)).withCoder(bigtableCoder))
        .apply("write", write);
    p.run();

    logged.verifyInfo("Wrote 1 records");

    assertEquals(1, service.tables.size());
    assertNotNull(service.getTable(table));
    Map<ByteString, ByteString> rows = service.getTable(table);
    assertEquals(1, rows.size());
    assertEquals(ByteString.copyFromUtf8(value), rows.get(ByteString.copyFromUtf8(key)));
  }

  /** Tests that when writing to a non-existent table, the write fails. */
  @Test
  public void testWritingFailsTableDoesNotExist() throws Exception {
    final String table = "TEST-TABLE";

    BigtableIO.Write write =
        BigtableIO.write()
            .withBigtableOptions(BIGTABLE_OPTIONS)
            .withTableId(table)
            .withBigtableService(service);

    PCollection<KV<ByteString, Iterable<Mutation>>> emptyInput =
        TestPipeline.create().apply(Create.<KV<ByteString, Iterable<Mutation>>>of());

    // Exception will be thrown by write.validate() when write is applied.
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("Table %s does not exist", table));

    emptyInput.apply("write", write);
  }

  /** Tests that when writing an element fails, the write fails. */
  @Test
  public void testWritingFailsBadElement() throws Exception {
    final String table = "TEST-TABLE";
    final String key = "KEY";
    service.createTable(table);

    BigtableIO.Write write =
        BigtableIO.write()
            .withBigtableOptions(BIGTABLE_OPTIONS)
            .withTableId(table)
            .withBigtableService(service);

    TestPipeline p = TestPipeline.create();
    p.apply(Create.of(makeBadRow(key)).withCoder(bigtableCoder)).apply(write);

    thrown.expect(PipelineExecutionException.class);
    thrown.expectCause(Matchers.<Throwable>instanceOf(IOException.class));
    thrown.expectMessage("At least 1 errors occurred writing to Bigtable. First 1 errors:");
    thrown.expectMessage("Error mutating row " + key + " with mutations []: cell value missing");
    p.run();
  }

  /**
   * A {@link BigtableService} implementation that stores tables and their contents in memory.
   */
  private static class FakeBigtableService implements BigtableService {
    private final Map<String, Map<ByteString, ByteString>> tables = new HashMap<>();

    @Nullable
    public Map<ByteString, ByteString> getTable(String tableId) {
      return tables.get(tableId);
    }

    public void createTable(String tableId) {
      tables.put(tableId, new HashMap<ByteString, ByteString>());
    }

    @Override
    public boolean tableExists(String tableId) {
      return tables.containsKey(tableId);
    }

    @Override
    public FakeBigtableWriter openForWriting(String tableId) {
      checkArgument(tableExists(tableId), "Table %s does not exist", tableId);
      return new FakeBigtableWriter(tableId);
    }
  }

  /**
   * A {@link BigtableService.Writer} implementation that writes to the static instance of
   * {@link FakeBigtableService} stored in {@link #service}.
   *
   * <p>This writer only supports {@link Mutation Mutations} that consist only of {@link SetCell}
   * entries. The column family in the {@link SetCell} is ignored; only the value is used.
   *
   * <p>When no {@link SetCell} is provided, the write will fail and this will be exposed via an
   * exception on the returned {@link ListenableFuture}.
   */
  private static class FakeBigtableWriter implements BigtableService.Writer {
    private final String tableId;

    public FakeBigtableWriter(String tableId) {
      this.tableId = tableId;
    }

    @Override
    public ListenableFuture<Empty> writeRecord(KV<ByteString, Iterable<Mutation>> record) {
      Map<ByteString, ByteString> table = service.getTable(tableId);
      ByteString key = record.getKey();
      for (Mutation m : record.getValue()) {
        SetCell cell = m.getSetCell();
        if (cell.getValue().isEmpty()) {
          return Futures.immediateFailedCheckedFuture(new IOException("cell value missing"));
        }
        table.put(key, cell.getValue());
      }
      return Futures.immediateFuture(Empty.getDefaultInstance());
    }

    @Override
    public void close() {}
  }
}
