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
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileSystemException;

/**
 * The ReadableByteChannel backed by a VFS InputStream.
 */
public class VfsReadableByteChannel implements ReadableByteChannel {
  private final FileContent content;
  private ReadableByteChannel channel;

  public VfsReadableByteChannel(final FileContent content) {
    this.content = content;
    try {
      this.channel = Channels.newChannel(content.getInputStream());
    } catch (final FileSystemException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public int read(final ByteBuffer dst) throws IOException {
    return channel.read(dst);
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
