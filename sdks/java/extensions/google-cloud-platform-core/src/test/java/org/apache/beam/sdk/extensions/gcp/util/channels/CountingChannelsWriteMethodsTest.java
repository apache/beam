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
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.beam.repackaged.core.org.apache.commons.compress.utils.SeekableInMemoryByteChannel;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class CountingChannelsWriteMethodsTest<ChannelT extends WritableByteChannel> {
  private ByteBuffer byteBuffer;
  private byte[] testData;
  private byte[] writtenData;
  private SeekableByteChannel delegate;
  private final ChannelUnderTestProvider<ChannelT> channelUnderTestProvider;

  public interface ChannelUnderTestProvider<T extends WritableByteChannel> {
    /**
     * Should create a delegating and counting {@link WritableByteChannel} whose write methods will
     * be tested.
     *
     * @param delegate a delegate to be used by the channel
     * @param bytesWrittenConsumer a consumer that is expected to receive the number of bytes
     *     written by the channel
     * @return the channel object to be tested
     */
    T create(SeekableByteChannel delegate, Consumer<Integer> bytesWrittenConsumer);
  }

  public CountingChannelsWriteMethodsTest(
      ChannelUnderTestProvider<ChannelT> channelUnderTestProvider,
      @SuppressWarnings("unused") String testLabel) {
    this.channelUnderTestProvider = channelUnderTestProvider;
  }

  private static <T extends WritableByteChannel> Object[] createTestCase(
      ChannelUnderTestProvider<T> channelUnderTestProvider, Class<T> channelClass) {
    return new Object[] {channelUnderTestProvider, channelClass.getSimpleName()};
  }

  private static Object[] createCountingSeekableByteChannelTestCase() {
    return createTestCase(
        CountingSeekableByteChannel::createWithBytesWrittenConsumer,
        CountingSeekableByteChannel.class);
  }

  private static Object[] createCountingWritableByteChannelTestCase() {
    return createTestCase(CountingWritableByteChannel::new, CountingWritableByteChannel.class);
  }

  @Parameterized.Parameters(name = "{1}")
  public static Iterable<Object[]> testCases() {
    ImmutableList.Builder<Object[]> builder = new ImmutableList.Builder<>();
    builder.add(createCountingSeekableByteChannelTestCase());
    builder.add(createCountingWritableByteChannelTestCase());
    return builder.build();
  }

  @Before
  public void before() {
    testData = "This is some test data".getBytes(StandardCharsets.UTF_8);
    byteBuffer = ByteBuffer.wrap(testData);
    writtenData = new byte[testData.length];
    delegate = new SeekableInMemoryByteChannel(writtenData);
  }

  @Test
  public void testCounting() throws IOException {
    AtomicInteger counter = new AtomicInteger();
    WritableByteChannel countingChannel =
        channelUnderTestProvider.create(delegate, counter::addAndGet);

    while (byteBuffer.remaining() != 0) {
      countingChannel.write(byteBuffer);
    }

    assertEquals(testData.length, counter.get());
  }

  @Test
  public void testWrite() throws IOException {
    WritableByteChannel channel = channelUnderTestProvider.create(delegate, __ -> {});

    while (byteBuffer.remaining() != 0) {
      channel.write(byteBuffer);
    }

    assertArrayEquals(testData, writtenData);
  }
}
