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

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CountingSeekableByteChannelTest {
  @Mock private SeekableByteChannel delegate;

  @Mock private ByteBuffer byteBuffer;

  @Mock private Consumer<Integer> bytesReadConsumer;

  @Mock private Consumer<Integer> bytesWrittenConsumer;

  private CountingSeekableByteChannel countingSeekableByteChannel;

  @Before
  public void before() {
    countingSeekableByteChannel =
        new CountingSeekableByteChannel(delegate, bytesReadConsumer, bytesWrittenConsumer);
  }

  @Test
  public void delegateMethodsAreCalled() throws Exception {
    countingSeekableByteChannel.isOpen();
    Mockito.verify(delegate).isOpen();

    countingSeekableByteChannel.close();
    Mockito.verify(delegate).close();

    countingSeekableByteChannel.read(byteBuffer);
    Mockito.verify(delegate).read(byteBuffer);

    countingSeekableByteChannel.position();
    Mockito.verify(delegate).position();

    countingSeekableByteChannel.position(-10);
    Mockito.verify(delegate).position(-10);

    countingSeekableByteChannel.size();
    Mockito.verify(delegate).size();

    countingSeekableByteChannel.truncate(-3);
    Mockito.verify(delegate).truncate(-3);
  }

  @Test
  public void bytesReadAreReportedToConsumer() throws Exception {
    int bytesRead = 42;
    Mockito.when(delegate.read(Mockito.any())).thenReturn(bytesRead);
    countingSeekableByteChannel.read(byteBuffer);
    Mockito.verify(bytesReadConsumer).accept(bytesRead);
  }

  @Test
  public void bytesWrittenAreReportedToConsumer() throws Exception {
    int bytesWritten = 42;
    Mockito.when(delegate.write(Mockito.any())).thenReturn(bytesWritten);
    countingSeekableByteChannel.write(byteBuffer);
    Mockito.verify(bytesWrittenConsumer).accept(bytesWritten);
  }
}
