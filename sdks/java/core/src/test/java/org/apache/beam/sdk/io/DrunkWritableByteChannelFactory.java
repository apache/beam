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
package org.apache.beam.sdk.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import org.apache.beam.sdk.io.FileBasedSink.WritableByteChannelFactory;
import org.apache.beam.sdk.util.MimeTypes;

/**
 * {@link WritableByteChannelFactory} implementation useful for testing that creates a {@link
 * WritableByteChannel} that writes everything twice.
 */
public class DrunkWritableByteChannelFactory implements WritableByteChannelFactory {
  @Override
  public WritableByteChannel create(WritableByteChannel channel) throws IOException {
    return new DrunkWritableByteChannel(channel);
  }

  @Override
  public String getMimeType() {
    return MimeTypes.TEXT;
  }

  @Override
  public String getSuggestedFilenameSuffix() {
    return ".drunk";
  }

  @Override
  public String toString() {
    return "DRUNK";
  }

  /** WritableByteChannel that writes everything twice. */
  private static class DrunkWritableByteChannel implements WritableByteChannel {
    protected final WritableByteChannel channel;

    public DrunkWritableByteChannel(final WritableByteChannel channel) {
      this.channel = channel;
    }

    @Override
    public boolean isOpen() {
      return channel.isOpen();
    }

    @Override
    public void close() throws IOException {
      channel.close();
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      final int w1 = channel.write(src);
      src.rewind();
      final int w2 = channel.write(src);
      return w1 + w2;
    }
  }
}
