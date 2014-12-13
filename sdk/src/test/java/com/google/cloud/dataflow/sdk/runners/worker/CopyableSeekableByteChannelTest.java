/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.api.client.util.Preconditions.checkArgument;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;

/** Unit tests for {@link CopyableSeekableByteChannel}. */
@RunWith(JUnit4.class)
public final class CopyableSeekableByteChannelTest {
  @Test
  public void copiedChannelShouldMaintainIndependentPosition()
      throws IOException {
    ByteBuffer dst = ByteBuffer.allocate(6);
    SeekableByteChannel base =
        new FakeSeekableByteChannel("Hello, world! :-)".getBytes());
    base.position(1);

    CopyableSeekableByteChannel chan = new CopyableSeekableByteChannel(base);
    assertThat(chan.position(), equalTo((long) 1));

    CopyableSeekableByteChannel copy = chan.copy();
    assertThat(copy.position(), equalTo((long) 1));

    assertThat(chan.read(dst), equalTo(6));
    assertThat(chan.position(), equalTo((long) 7));
    assertThat(new String(dst.array()), equalTo("ello, "));
    dst.rewind();

    assertThat(copy.position(), equalTo((long) 1));
    copy.position(3);
    assertThat(copy.read(dst), equalTo(6));
    assertThat(copy.position(), equalTo((long) 9));
    assertThat(new String(dst.array()), equalTo("lo, wo"));
    dst.rewind();

    assertThat(chan.read(dst), equalTo(6));
    assertThat(chan.position(), equalTo((long) 13));
    assertThat(new String(dst.array()), equalTo("world!"));
    dst.rewind();

    assertThat(chan.read(dst), equalTo(4));
    assertThat(chan.position(), equalTo((long) 17));
    assertThat(new String(dst.array()), equalTo(" :-)d!"));
    dst.rewind();

    assertThat(copy.position(), equalTo((long) 9));
    assertThat(copy.read(dst), equalTo(6));
    assertThat(new String(dst.array()), equalTo("rld! :"));
  }

  private static final class FakeSeekableByteChannel
      implements SeekableByteChannel {
    private boolean closed = false;
    private ByteBuffer data;

    public FakeSeekableByteChannel(byte[] data) {
      this.data = ByteBuffer.wrap(data);
    }

    @Override
    public long position() throws IOException {
      checkClosed();
      return data.position();
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
      checkArgument(newPosition >= 0);
      checkClosed();
      data.position((int) newPosition);
      return this;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      checkClosed();
      if (!data.hasRemaining()) {
        return -1;
      }
      int count = Math.min(data.remaining(), dst.remaining());
      ByteBuffer src = data.slice();
      src.limit(count);
      dst.put(src);
      data.position(data.position() + count);
      return count;
    }

    @Override
    public long size() throws IOException {
      checkClosed();
      return data.limit();
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
      checkClosed();
      data.limit((int) size);
      return this;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      checkClosed();
      int count = Math.min(data.remaining(), src.remaining());
      ByteBuffer copySrc = src.slice();
      copySrc.limit(count);
      data.put(copySrc);
      return count;
    }

    @Override
    public boolean isOpen() {
      return !closed;
    }

    @Override
    public void close() {
      closed = true;
    }

    private void checkClosed() throws ClosedChannelException {
      if (closed) {
        throw new ClosedChannelException();
      }
    }
  }
}
