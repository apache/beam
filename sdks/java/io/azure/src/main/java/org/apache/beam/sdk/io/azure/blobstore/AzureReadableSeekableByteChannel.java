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
package org.apache.beam.sdk.io.azure.blobstore;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.specialized.BlobInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;

class AzureReadableSeekableByteChannel implements SeekableByteChannel {

  private final BlobInputStream inputStream;
  private boolean closed;
  private final long contentLength;
  private long position = 0;

  public AzureReadableSeekableByteChannel(BlobClient blobClient) {
    inputStream = blobClient.openInputStream();
    contentLength = blobClient.getProperties().getBlobSize();
    closed = false;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    if (closed) {
      throw new ClosedChannelException();
    }
    if (!dst.hasRemaining()) {
      return 0;
    }

    int read = 0;
    if (dst.hasArray()) {
      // Stores up to dst.remaining() bytes into dst.array() starting at dst.position().
      // But dst can have an offset with its backing array, hence the + dst.arrayOffset().
      read = inputStream.read(dst.array(), dst.position() + dst.arrayOffset(), dst.remaining());
    } else {
      byte[] myarray = new byte[dst.remaining()];
      read = inputStream.read(myarray, 0, myarray.length);
      dst.put(myarray);
    }

    if (read > 0) {
      position += read;
    }
    return read;
  }

  @Override
  public int write(ByteBuffer src) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long position() throws IOException {
    if (closed) {
      throw new ClosedChannelException();
    }
    return position;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    if (closed) {
      throw new ClosedChannelException();
    }
    checkArgument(newPosition >= 0, "newPosition too low");
    checkArgument(newPosition < contentLength, "new position too high");

    Long bytesToSkip = newPosition - position;
    Long n = inputStream.skip(bytesToSkip);
    position += n;
    return this;
  }

  @Override
  public long size() throws IOException {
    if (closed) {
      throw new ClosedChannelException();
    }
    return contentLength;
  }

  @Override
  public SeekableByteChannel truncate(long size) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isOpen() {
    return !closed;
  }

  @Override
  public void close() throws IOException {
    closed = true;
    inputStream.close();
  }
}
