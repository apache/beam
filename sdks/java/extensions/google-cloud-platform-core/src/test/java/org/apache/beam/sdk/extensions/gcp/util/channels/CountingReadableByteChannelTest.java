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
package org.apache.beam.sdk.extensions.gcp.util.channels;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.apache.beam.repackaged.core.org.apache.commons.compress.utils.SeekableInMemoryByteChannel;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class CountingReadableByteChannelTest {

  private ByteBuffer byteBuffer;
  private byte[] testData;
  private SeekableByteChannel delegate;
  private final BiFunction<SeekableByteChannel, Consumer<Integer>, ? extends ReadableByteChannel>
      channelUnderTestProvider;

  public CountingReadableByteChannelTest(
      BiFunction<SeekableByteChannel, Consumer<Integer>, ReadableByteChannel>
          channelUnderTestProvider,
      @SuppressWarnings("unused") Class<? extends ReadableByteChannel> testLabel) {
    this.channelUnderTestProvider = channelUnderTestProvider;
  }

  @Parameterized.Parameters(name = "{1}")
  public static Iterable<Object[]> testParams() {
    ImmutableList.Builder<Object[]> builder = new ImmutableList.Builder<>();
    BiFunction<SeekableByteChannel, Consumer<Integer>, CountingReadableByteChannel>
        countingReadableByteChannelProvider = CountingReadableByteChannel::new;
    builder.add(
        new Object[] {countingReadableByteChannelProvider, CountingReadableByteChannel.class});

    BiFunction<SeekableByteChannel, Consumer<Integer>, CountingSeekableByteChannel>
        countingSeekableByteChannelProvider =
            (delegate, bytesReadConsumer) ->
                new CountingSeekableByteChannel(delegate, bytesReadConsumer, __ -> {});
    builder.add(
        new Object[] {countingSeekableByteChannelProvider, CountingSeekableByteChannel.class});

    return builder.build();
  }

  @Before
  public void before() {
    testData = "This is some test data".getBytes(StandardCharsets.UTF_8);
    byteBuffer = ByteBuffer.allocate(1024);
    delegate = new SeekableInMemoryByteChannel(testData);
  }

  @Test
  public void testCounting() throws IOException {
    AtomicInteger counter = new AtomicInteger();
    ReadableByteChannel countingChannel =
        channelUnderTestProvider.apply(delegate, counter::addAndGet);

    while (countingChannel.read(byteBuffer) != -1) {}

    assertEquals(
        testData.length - 1,
        counter.get()); // the counter will subtract the final -1, that's expected
  }

  @Test
  public void testRead() throws IOException {
    ReadableByteChannel channel = channelUnderTestProvider.apply(delegate, __ -> {});
    int totalBytesRead = 0;
    for (int bytesRead = channel.read(byteBuffer);
        bytesRead != -1;
        bytesRead = channel.read(byteBuffer)) {
      totalBytesRead += bytesRead;
    }
    byteBuffer.rewind();

    byte[] actual = new byte[totalBytesRead];
    byteBuffer.get(actual);

    assertArrayEquals(testData, actual);
  }
}
