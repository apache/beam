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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.channels.Channel;
import java.nio.channels.SeekableByteChannel;
import org.apache.beam.repackaged.core.org.apache.commons.compress.utils.SeekableInMemoryByteChannel;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class CountingChannelsIsOpenCloseMethodsTest<ChannelT extends Channel> {

  private final ChannelUnderTestProvider<ChannelT> channelUnderTestProvider;
  private Channel channelUnderTest;
  private SeekableByteChannel delegate;

  public interface ChannelUnderTestProvider<T extends Channel> {
    /**
     * Should create a {@link Channel} that delegates its open/closed state management to the
     * provided delegate.
     *
     * @param delegate a delegate channel whose state is to be managed
     * @return an instance of the channel class to be tested
     */
    T create(SeekableByteChannel delegate);
  }

  public CountingChannelsIsOpenCloseMethodsTest(
      ChannelUnderTestProvider<ChannelT> channelUnderTestProvider,
      @SuppressWarnings("unused") String testLabel) {
    this.channelUnderTestProvider = channelUnderTestProvider;
  }

  @Before
  public void before() {
    delegate = new SeekableInMemoryByteChannel();
    channelUnderTest = channelUnderTestProvider.create(delegate);
  }

  private static <T extends Channel> Object[] createTestCase(
      ChannelUnderTestProvider<T> channelUnderTestProvider, Class<T> channelClass) {
    return new Object[] {channelUnderTestProvider, channelClass.getSimpleName()};
  }

  private static Object[] createCountingSeekableByteChannelTestCase() {
    return createTestCase(
        CountingSeekableByteChannel::createWithNoOpConsumer, CountingSeekableByteChannel.class);
  }

  private static Object[] createCountingReadableByteChannelTestCase() {
    return createTestCase(
        CountingReadableByteChannel::createWithNoOpConsumer, CountingReadableByteChannel.class);
  }

  private static Object[] createCountingWritableByteChannelTestCase() {
    return createTestCase(
        CountingWritableByteChannel::createWithNoOpConsumer, CountingWritableByteChannel.class);
  }

  @Parameterized.Parameters(name = "{1}")
  public static Iterable<Object[]> testCases() {
    ImmutableList.Builder<Object[]> builder = ImmutableList.builder();
    builder.add(createCountingReadableByteChannelTestCase());
    builder.add(createCountingWritableByteChannelTestCase());
    builder.add(createCountingSeekableByteChannelTestCase());
    return builder.build();
  }

  @Test
  public void testIsOpen() throws IOException {
    assertTrue(channelUnderTest.isOpen());
    delegate.close();
    assertFalse(channelUnderTest.isOpen());
  }

  @Test
  public void testClose() throws IOException {
    channelUnderTest.close();
    assertFalse(channelUnderTest.isOpen());
    assertFalse(delegate.isOpen());
  }
}
