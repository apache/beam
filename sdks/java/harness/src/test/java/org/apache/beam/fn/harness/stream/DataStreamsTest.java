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
package org.apache.beam.fn.harness.stream;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Iterators;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.SynchronousQueue;
import org.apache.beam.fn.harness.stream.DataStreams.BlockingIterator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DataStreams}. */
@RunWith(JUnit4.class)
public class DataStreamsTest {
  private static final ByteString BYTES_A = ByteString.copyFromUtf8("TestData");
  private static final ByteString BYTES_B = ByteString.copyFromUtf8("SomeOtherTestData");

  @Test
  public void testEmptyRead() throws Exception {
    assertEquals(ByteString.EMPTY, read());
    assertEquals(ByteString.EMPTY, read(ByteString.EMPTY));
    assertEquals(ByteString.EMPTY, read(ByteString.EMPTY, ByteString.EMPTY));
  }

  @Test
  public void testRead() throws Exception {
    assertEquals(BYTES_A.concat(BYTES_B), read(BYTES_A, BYTES_B));
    assertEquals(BYTES_A.concat(BYTES_B), read(BYTES_A, ByteString.EMPTY, BYTES_B));
    assertEquals(BYTES_A.concat(BYTES_B), read(BYTES_A, BYTES_B, ByteString.EMPTY));
  }

  @Test(timeout = 10_000)
  public void testBlockingIteratorWithoutBlocking() throws Exception {
    BlockingIterator<String> iterator = new BlockingIterator<>(new ArrayBlockingQueue<>(3));

    iterator.accept("A");
    iterator.accept("B");
    iterator.close();

    assertEquals(Arrays.asList("A", "B"),
        Arrays.asList(Iterators.toArray(iterator, String.class)));
  }

  @Test(timeout = 10_000)
  public void testBlockingIteratorWithBlocking() throws Exception {
    // The synchronous queue only allows for one element to transfer at a time and blocks
    // the sending/receiving parties until both parties are there.
    final BlockingIterator<String> iterator = new BlockingIterator<>(new SynchronousQueue<>());
    final CompletableFuture<List<String>> valuesFuture = new CompletableFuture<>();
    Thread appender = new Thread() {
      @Override
      public void run() {
        valuesFuture.complete(Arrays.asList(Iterators.toArray(iterator, String.class)));
      }
    };
    appender.start();
    iterator.accept("A");
    iterator.accept("B");
    iterator.close();
    assertEquals(Arrays.asList("A", "B"), valuesFuture.get());
    appender.join();
  }

  private static ByteString read(ByteString... bytes) throws IOException {
    return ByteString.readFrom(DataStreams.inbound(Arrays.asList(bytes).iterator()));
  }
}
