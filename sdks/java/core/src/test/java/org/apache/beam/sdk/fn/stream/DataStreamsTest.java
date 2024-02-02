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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.WeightedList;
import org.apache.beam.sdk.fn.stream.DataStreams.DataStreamDecoder;
import org.apache.beam.sdk.fn.stream.DataStreams.ElementDelimitedOutputStream;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.CountingOutputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DataStreams}. */
@RunWith(Enclosed.class)
public class DataStreamsTest {

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
    public void testNonEmptyInputStreamWithZeroLengthEncoding() throws Exception {
      CountingOutputStream countingOutputStream =
          new CountingOutputStream(ByteStreams.nullOutputStream());
      GlobalWindow.Coder.INSTANCE.encode(GlobalWindow.INSTANCE, countingOutputStream);
      assumeTrue(countingOutputStream.getCount() == 0);

      testDecoderWith(GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE, GlobalWindow.INSTANCE);
    }

    @Test
    public void testPrefetch() throws Exception {
      List<ByteString> encodings = new ArrayList<>();
      encodings.add(encode("A", "BC"));
      encodings.add(ByteString.EMPTY);
      encodings.add(encode("DEF", "GHIJ"));

      PrefetchableIteratorsTest.ReadyAfterPrefetchUntilNext<ByteString> iterator =
          new PrefetchableIteratorsTest.ReadyAfterPrefetchUntilNext<>(encodings.iterator());
      PrefetchableIterator<String> decoder =
          new DataStreamDecoder<>(StringUtf8Coder.of(), iterator);
      assertFalse(decoder.isReady());
      decoder.prefetch();
      assertTrue(decoder.isReady());
      assertEquals(1, iterator.getNumPrefetchCalls());

      decoder.next();
      // Now we will have moved off of the empty byte array that we start with so prefetch will
      // do nothing since we are ready
      assertTrue(decoder.isReady());
      decoder.prefetch();
      assertEquals(1, iterator.getNumPrefetchCalls());

      decoder.next();
      // Now we are at the end of the first ByteString so we expect a prefetch to pass through
      assertFalse(decoder.isReady());
      decoder.prefetch();
      assertEquals(2, iterator.getNumPrefetchCalls());
      // We also expect the decoder to not be ready since the next byte string is empty which
      // would require us to move to the next page. This typically wouldn't happen in practice
      // though because we expect non empty pages.
      assertFalse(decoder.isReady());

      // Prefetching will allow us to move to the third ByteString
      decoder.prefetch();
      assertEquals(3, iterator.getNumPrefetchCalls());
      assertTrue(decoder.isReady());
    }

    @Test
    public void testDecodeFromChunkBoundaryToChunkBoundary() throws Exception {
      ByteString multipleElementsToSplit = encode("B", "BigElementC");
      ByteString singleElementToSplit = encode("BigElementG");
      DataStreamDecoder<String> decoder =
          new DataStreamDecoder<>(
              StringUtf8Coder.of(),
              new PrefetchableIteratorsTest.ReadyAfterPrefetchUntilNext<>(
                  Iterators.forArray(
                      encode("A"),
                      multipleElementsToSplit.substring(0, multipleElementsToSplit.size() - 1),
                      multipleElementsToSplit.substring(multipleElementsToSplit.size() - 1),
                      encode("D"),
                      encode(),
                      encode("E", "F"),
                      singleElementToSplit.substring(0, singleElementToSplit.size() - 1),
                      singleElementToSplit.substring(singleElementToSplit.size() - 1))));

      WeightedList<String> weightedListA = decoder.decodeFromChunkBoundaryToChunkBoundary();
      assertThat(weightedListA.getBacking(), contains("A"));
      assertThat(weightedListA.getWeight(), equalTo(10L)); // 2 + 8

      WeightedList<String> weightedListBC = decoder.decodeFromChunkBoundaryToChunkBoundary();
      assertThat(weightedListBC.getBacking(), contains("B", "BigElementC"));
      assertThat(weightedListBC.getWeight(), equalTo(29L)); // 2 + 8 + 11 + 8

      assertThat(decoder.decodeFromChunkBoundaryToChunkBoundary().getBacking(), contains("D"));
      assertThat(decoder.decodeFromChunkBoundaryToChunkBoundary().getBacking(), is(empty()));
      assertThat(decoder.decodeFromChunkBoundaryToChunkBoundary().getBacking(), contains("E", "F"));
      assertThat(
          decoder.decodeFromChunkBoundaryToChunkBoundary().getBacking(), contains("BigElementG"));
      assertFalse(decoder.hasNext());
    }

    private ByteString encode(String... values) throws IOException {
      ByteStringOutputStream out = new ByteStringOutputStream();
      for (String value : values) {
        StringUtf8Coder.of().encode(value, out);
      }
      return out.toByteString();
    }

    private <T> void testDecoderWith(Coder<T> coder, T... expected) throws IOException {
      ByteStringOutputStream output = new ByteStringOutputStream();
      for (T value : expected) {
        int size = output.size();
        coder.encode(value, output);
        // Pad an arbitrary byte when values encode to zero bytes
        if (output.size() - size == 0) {
          output.write(0);
        }
      }
      testDecoderWith(coder, expected, Arrays.asList(output.toByteString()));
      testDecoderWith(coder, expected, Arrays.asList(ByteString.EMPTY, output.toByteString()));
      testDecoderWith(coder, expected, Arrays.asList(output.toByteString(), ByteString.EMPTY));
    }

    private <T> void testDecoderWith(Coder<T> coder, T[] expected, List<ByteString> encoded) {
      DataStreamDecoder<T> decoder =
          new DataStreamDecoder<>(
              coder, PrefetchableIterators.maybePrefetchable(encoded.iterator()));

      Object[] actual = Iterators.toArray(decoder, Object.class);
      assertArrayEquals(expected, actual);

      // Ensure that we are consistent on hasNext at end of stream
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
