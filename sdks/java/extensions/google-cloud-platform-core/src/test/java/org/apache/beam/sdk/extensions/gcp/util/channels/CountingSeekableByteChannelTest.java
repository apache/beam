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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import org.apache.beam.repackaged.core.org.apache.commons.compress.utils.SeekableInMemoryByteChannel;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test {@link SeekableByteChannel} delegation. Reading, writing and consumer interactions are
 * tested along with {@link CountingReadableByteChannel} and {@link CountingWritableByteChannel} in
 * the {@link CountingChannelsReadMethodsTest} and {@link CountingChannelsWriteMethodsTest}
 * respectively.
 */
@RunWith(JUnit4.class)
public class CountingSeekableByteChannelTest {

  private SeekableByteChannel delegate;
  private CountingSeekableByteChannel channelUnderTest;

  @Before
  public void before() {
    delegate = new SeekableInMemoryByteChannel(new byte[16]);
    channelUnderTest = CountingSeekableByteChannel.createWithNoOpConsumer(delegate);
  }

  @Test
  public void testPosition() throws IOException {
    int newPosition = 5;
    channelUnderTest.position(newPosition);
    assertEquals(newPosition, delegate.position());
    assertEquals(newPosition, channelUnderTest.position());
  }

  @Test
  public void testTruncate() throws IOException {
    int newSize = 5;
    channelUnderTest.truncate(newSize);
    assertEquals(newSize, delegate.size());
    assertEquals(newSize, channelUnderTest.size());
  }

  @Test
  public void testSize() throws IOException {
    assertEquals(delegate.size(), channelUnderTest.size());
  }
}
