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
import java.util.function.Function;
import org.apache.beam.repackaged.core.org.apache.commons.compress.utils.SeekableInMemoryByteChannel;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class CountingChannelsTest {

  private final Function<SeekableByteChannel, Channel> channelUnderTestProvider;
  private Channel channelUnderTest;
  private SeekableByteChannel delegate;

  public CountingChannelsTest(
      Function<SeekableByteChannel, Channel> channelUnderTestProvider,
      @SuppressWarnings("unused") Class<? extends Channel> testLabel) {
    this.channelUnderTestProvider = channelUnderTestProvider;
  }

  @Before
  public void before() {
    delegate = new SeekableInMemoryByteChannel();
    channelUnderTest = channelUnderTestProvider.apply(delegate);
  }

  @Parameterized.Parameters(name = "{1}")
  public static Iterable<Object[]> testParams() {

    ImmutableList.Builder<Object[]> paramBuilder = ImmutableList.builder();

    Function<SeekableByteChannel, Channel> countingReadableByteChannelProvider =
        delegate -> new CountingReadableByteChannel(delegate, __ -> {});

    paramBuilder.add(
        new Object[] {countingReadableByteChannelProvider, CountingReadableByteChannel.class});

    Function<SeekableByteChannel, Channel> countingWritableByteChannelProvider =
        delegate -> new CountingWritableByteChannel(delegate, __ -> {});
    paramBuilder.add(
        new Object[] {countingWritableByteChannelProvider, CountingWritableByteChannel.class});

    Function<SeekableByteChannel, Channel> countingSeekableByteChannelProvider =
        delegate -> new CountingSeekableByteChannel(delegate, __ -> {}, __ -> {});
    paramBuilder.add(
        new Object[] {countingSeekableByteChannelProvider, CountingSeekableByteChannel.class});

    return paramBuilder.build();
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
