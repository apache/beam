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
package org.apache.beam.sdk.io.hbase;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.lang.reflect.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * {@link HBaseIO} used to release its resources as consecutive, unguarded statements, so a throwing
 * earlier {@code close()} skipped everything after it.
 *
 * <p>The sharpest case is {@link HBaseIO.WriteRowMutations.WriteRowMutationsFn#tearDown()}: the
 * skipped call there is {@link HBaseSharedConnection#close(Configuration)}, which is a
 * reference-count decrement rather than an ordinary close. Missing it strands the entry in a {@code
 * static} pool for the lifetime of the JVM, so that test asserts the real count rather than a mock
 * interaction.
 */
@RunWith(JUnit4.class)
public class HBaseIOCloseTest {

  private final Configuration configuration = HBaseConfiguration.create();

  @After
  public void resetConnectionPool() throws IOException {
    HBaseSharedConnection.closeAll();
  }

  // ---------------------------------------------------------------- failure collection

  @Test
  public void appendSuppressedKeepsTheFirstFailure() {
    IOException first = new IOException("first");
    IOException second = new IOException("second");

    assertSame(first, HBaseIO.appendSuppressed(null, first));

    Throwable collected = HBaseIO.appendSuppressed(first, second);
    assertSame(first, collected);
    assertArrayEquals(new Throwable[] {second}, collected.getSuppressed());
  }

  @Test
  public void rethrowCloseFailurePreservesTheType() {
    IOException io = new IOException("io");
    assertSame(io, assertThrows(IOException.class, () -> HBaseIO.rethrowCloseFailure(io)));

    IllegalStateException unchecked = new IllegalStateException("unchecked");
    assertSame(
        unchecked,
        assertThrows(IllegalStateException.class, () -> HBaseIO.rethrowCloseFailure(unchecked)));

    // Anything else has to be wrapped, because the teardowns only declare IOException.
    Throwable checked = new Exception("checked");
    IOException wrapped =
        assertThrows(IOException.class, () -> HBaseIO.rethrowCloseFailure(checked));
    assertSame(checked, wrapped.getCause());
  }

  // ---------------------------------------------------------------- reader

  @Test
  public void readerClosesTheConnectionWhenTheScannerFailsToClose() throws Exception {
    HBaseIO.Read read = HBaseIO.read().withConfiguration(configuration).withTableId("some_table");
    HBaseIO.HBaseReader reader =
        new HBaseIO.HBaseReader(new HBaseIO.HBaseSource(read, null /* estimatedSizeBytes */));

    ResultScanner scanner = mock(ResultScanner.class);
    Connection connection = mock(Connection.class);
    IOException scannerFailure = new IOException("scanner close failed");
    doThrow(scannerFailure).when(scanner).close();
    // The reader only acquires these in start(), which would need a live cluster.
    set(reader, "scanner", scanner);
    set(reader, "connection", connection);

    assertSame(scannerFailure, assertThrows(IOException.class, reader::close));
    verify(connection).close();
  }

  // ---------------------------------------------------------------- mutation writer

  @Test
  public void writerClosesTheConnectionWhenTheFinalFlushFails() throws Exception {
    HBaseIO.Write write =
        HBaseIO.write().withConfiguration(configuration).withTableId("some_table");
    HBaseIO.Write.HBaseWriterFn fn = write.new HBaseWriterFn(write);

    BufferedMutator mutator = mock(BufferedMutator.class);
    Connection connection = mock(Connection.class);
    // BufferedMutator.close() performs a flush, so this is the expected failure mode.
    IOException flushFailure = new IOException("flush on close failed");
    doThrow(flushFailure).when(mutator).close();
    set(fn, "mutator", mutator);
    set(fn, "connection", connection);

    assertSame(flushFailure, assertThrows(IOException.class, fn::tearDown));
    verify(connection).close();
  }

  // ---------------------------------------------------------------- row-mutation writer

  @Test
  public void rowMutationWriterReleasesTheSharedConnectionWhenTheTableFailsToClose()
      throws Exception {
    HBaseSharedConnection.getOrCreate(configuration);
    assertEquals(1, HBaseSharedConnection.getConnectionCount(configuration));

    HBaseIO.WriteRowMutations write =
        HBaseIO.writeRowMutations().withConfiguration(configuration).withTableId("some_table");
    HBaseIO.WriteRowMutations.WriteRowMutationsFn fn = write.new WriteRowMutationsFn(write);

    Table table = mock(Table.class);
    IOException tableFailure = new IOException("table close failed");
    doThrow(tableFailure).when(table).close();
    set(fn, "table", table);

    assertSame(tableFailure, assertThrows(IOException.class, fn::tearDown));
    // The point of the fix: the reference count still went back down, so the pooled connection is
    // releasable instead of being stranded for the lifetime of the JVM.
    assertEquals(0, HBaseSharedConnection.getConnectionCount(configuration));
  }

  private static void set(Object target, String fieldName, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }
}
