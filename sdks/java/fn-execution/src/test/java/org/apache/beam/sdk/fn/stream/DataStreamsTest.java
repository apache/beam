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
package org.apache.beam.sdk.fn.stream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.stream.DataStreams.BlockingQueueIterator;
import org.apache.beam.sdk.fn.stream.DataStreams.DataStreamDecoder;
import org.apache.beam.sdk.fn.stream.DataStreams.ElementDelimitedOutputStream;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.CountingOutputStream;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.SettableFuture;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DataStreams}. */
public class DataStreamsTest {

  /** Tests for {@link DataStreams.Inbound}. */
  @RunWith(JUnit4.class)
  public static class InboundTest {
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

    private static ByteString read(ByteString... bytes) throws IOException {
      return ByteString.readFrom(DataStreams.inbound(Arrays.asList(bytes).iterator()));
    }
  }

  /** Tests for {@link DataStreams.BlockingQueueIterator}. */
  @RunWith(JUnit4.class)
  public static class BlockingQueueIteratorTest {
    @Test(timeout = 10_000)
    public void testBlockingQueueIteratorWithoutBlocking() throws Exception {
      BlockingQueueIterator<String> iterator =
          new BlockingQueueIterator<>(new ArrayBlockingQueue<String>(3));

      iterator.accept("A");
      iterator.accept("B");
      iterator.close();

      assertEquals(
          Arrays.asList("A", "B"), Arrays.asList(Iterators.toArray(iterator, String.class)));
    }

    @Test(timeout = 10_000)
    public void testBlockingQueueIteratorWithBlocking() throws Exception {
      // The synchronous queue only allows for one element to transfer at a time and blocks
      // the sending/receiving parties until both parties are there.
      final BlockingQueueIterator<String> iterator =
          new BlockingQueueIterator<>(new SynchronousQueue<String>());
      final SettableFuture<List<String>> valuesFuture = SettableFuture.create();
      Thread appender =
          new Thread(
              () -> valuesFuture.set(Arrays.asList(Iterators.toArray(iterator, String.class))));
      appender.start();
      iterator.accept("A");
      iterator.accept("B");
      iterator.close();
      assertEquals(Arrays.asList("A", "B"), valuesFuture.get());
      appender.join();
    }
  }

  /** Tests for {@link DataStreams.DataStreamDecoder}. */
  @RunWith(JUnit4.class)
  public static class DataStreamDecoderTest {
    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testEmptyInputStream() throws Exception {
      testDecoderWith(StringUtf8Coder.of());
    }

    @Test
    public void testNonEmptyInputStream() throws Exception {
      testDecoderWith(StringUtf8Coder.of(), "A", "BC", "DEF", "GHIJ");
    }

    @Test
    public void testNonEmptyInputStreamWithZeroLengthCoder() throws Exception {
      CountingOutputStream countingOutputStream =
          new CountingOutputStream(ByteStreams.nullOutputStream());
      GlobalWindow.Coder.INSTANCE.encode(GlobalWindow.INSTANCE, countingOutputStream);
      assumeTrue(countingOutputStream.getCount() == 0);

      testDecoderWith(GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE, GlobalWindow.INSTANCE);
    }

    private <T> void testDecoderWith(Coder<T> coder, T... expected) throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      for (T value : expected) {
        int size = baos.size();
        coder.encode(value, baos);
        // Pad an arbitrary byte when values encode to zero bytes
        if (baos.size() - size == 0) {
          baos.write(0);
        }
      }

      Iterator<T> decoder =
          new DataStreamDecoder<>(coder, new ByteArrayInputStream(baos.toByteArray()));

      Object[] actual = Iterators.toArray(decoder, Object.class);
      assertArrayEquals(expected, actual);

      assertFalse(decoder.hasNext());
      assertFalse(decoder.hasNext());

      thrown.expect(NoSuchElementException.class);
      decoder.next();
    }
  }

  /** Tests for {@link ElementDelimitedOutputStream delimited streams}. */
  @RunWith(JUnit4.class)
  public static class ElementDelimitedOutputStreamTest {
    @Test
    public void testNothingWritten() throws Exception {
      List<ByteString> output = new ArrayList<>();
      ElementDelimitedOutputStream outputStream = new ElementDelimitedOutputStream(output::add, 3);
      outputStream.close();
      assertThat(output, hasSize(0));
    }

    @Test
    public void testEmptyElementsArePadded() throws Exception {
      List<ByteString> output = new ArrayList<>();
      ElementDelimitedOutputStream outputStream = new ElementDelimitedOutputStream(output::add, 3);
      outputStream.delimitElement();
      outputStream.delimitElement();
      outputStream.delimitElement();
      outputStream.delimitElement();
      outputStream.delimitElement();
      outputStream.close();
      assertThat(
          output, contains(ByteString.copyFrom(new byte[3]), ByteString.copyFrom(new byte[2])));
    }

    @Test
    public void testNonEmptyElementsAreChunked() throws Exception {
      List<ByteString> output = new ArrayList<>();
      ElementDelimitedOutputStream outputStream = new ElementDelimitedOutputStream(output::add, 3);
      outputStream.write(new byte[] {0x01, 0x02});
      outputStream.delimitElement();
      outputStream.write(new byte[] {0x03, 0x04, 0x05, 0x06, 0x07, 0x08});
      outputStream.delimitElement();
      outputStream.write(0x09);
      outputStream.delimitElement();
      outputStream.close();
      assertThat(
          output,
          contains(
              ByteString.copyFrom(new byte[] {0x01, 0x02, 0x03}),
              ByteString.copyFrom(new byte[] {0x04, 0x05, 0x06}),
              ByteString.copyFrom(new byte[] {0x07, 0x08, 0x09})));
    }
  }
}
