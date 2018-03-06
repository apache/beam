/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.vfs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileSystemException;

/**
 * A VFS outputstream wrapped to be compatible with Beam fileystem.
 */
public class VfsWritableByteChannel implements WritableByteChannel {
  private final FileContent content;
  private OutputStream outputStream;

  public VfsWritableByteChannel(final FileContent content) {
    this.content = content;
    try {
      this.outputStream = content.getOutputStream(true);
    } catch (final FileSystemException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public int write(final ByteBuffer src) throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }

    int totalBytesWritten = 0;
    while (src.hasRemaining()) {
      final int bytesWritten = src.remaining();
      totalBytesWritten += bytesWritten;

      final byte[] copyBuffer = new byte[bytesWritten];
      src.get(copyBuffer);
      outputStream.write(copyBuffer);
    }
    return totalBytesWritten;
  }

  @Override
  public boolean isOpen() {
    return content.isOpen();
  }

  @Override
  public void close() throws IOException {
    content.close();
  }
}
