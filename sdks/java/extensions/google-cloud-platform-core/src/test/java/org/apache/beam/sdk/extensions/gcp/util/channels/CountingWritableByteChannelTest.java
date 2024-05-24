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
import java.nio.channels.WritableByteChannel;
import java.util.function.Consumer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CountingWritableByteChannelTest {
  @Mock private WritableByteChannel delegate;

  @Mock private ByteBuffer byteBuffer;

  @Mock private Consumer<Integer> consumer;

  @InjectMocks private CountingWritableByteChannel countingReadableByteChannel;

  @Test
  public void delegateMethodsAreCalled() throws Exception {
    countingReadableByteChannel.isOpen();
    Mockito.verify(delegate).isOpen();

    countingReadableByteChannel.close();
    Mockito.verify(delegate).close();

    countingReadableByteChannel.write(byteBuffer);
    Mockito.verify(delegate).write(byteBuffer);
  }

  @Test
  public void bytesWrittenAreReportedToConsumer() throws Exception {
    int bytesWritten = 42;
    Mockito.when(delegate.write(Mockito.any())).thenReturn(bytesWritten);
    countingReadableByteChannel.write(byteBuffer);
    Mockito.verify(consumer).accept(bytesWritten);
  }
}
