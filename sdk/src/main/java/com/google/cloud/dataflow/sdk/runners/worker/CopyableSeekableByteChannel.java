/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.api.client.util.Preconditions.checkNotNull;
import static com.google.api.client.util.Preconditions.checkState;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;

import javax.annotation.concurrent.GuardedBy;

/**
 * A {@link SeekableByteChannel} that adds copy semantics.
 *
 * <p>This implementation uses a lock to ensure that only one thread accesses
 * the underlying {@code SeekableByteChannel} at any given time.
 *
 * <p>{@link SeekableByteChannel#close} is called on the underlying channel once
 * all {@code CopyableSeekableByteChannel} objects copied from the initial
 * {@code CopyableSeekableByteChannel} are closed.
 *
 * <p>The implementation keeps track of the position of each
 * {@code CopyableSeekableByteChannel}; on access, it synchronizes with the
 * other {@code CopyableSeekableByteChannel} instances accessing the underlying
 * channel, seeks to its own position, performs the operation, updates its local
 * position, and returns the result.
 */
final class CopyableSeekableByteChannel implements SeekableByteChannel {
  /** This particular stream's position in the base stream. */
  private long pos;

  /**
   * The synchronization object keeping track of the base
   * {@link SeekableByteChannel}, its reference count, and its current position.
   * This also doubles as the lock shared by all
   * {@link CopyableSeekableByteChannel} instances derived from some original
   * instance.
   */
  private final Sync sync;

  /**
   * Indicates whether this {@link CopyableSeekableByteChannel} is closed.
   *
   * <p>Invariant: Unclosed channels own a reference to the base channel,
   * allowing us to make {@link #close} idempotent.
   *
   * <p>This is only modified under the sync lock.
   */
  private boolean closed;

  /**
   * Constructs a new {@link CopyableSeekableByteChannel}.  The supplied base
   * channel will be closed when this channel and all derived channels are
   * closed.
   */
  public CopyableSeekableByteChannel(SeekableByteChannel base) throws IOException {
    this(new Sync(base), 0);

    // Update the position to match the original stream's position.
    //
    // This doesn't actually need to be synchronized, but it's a little more
    // obviously correct to always access sync.position while holding sync's
    // internal monitor.
    synchronized (sync) {
      sync.position = base.position();
      pos = sync.position;
    }
  }

  /**
   * The internal constructor used when deriving a new
   * {@link CopyableSeekableByteChannel}.
   *
   * <p>N.B. This signature is deliberately incompatible with the public
   * constructor.
   *
   * <p>Ordinarily, one would implement copy using a copy constructor, and pass
   * the object being copied -- but that signature would be compatible with the
   * public constructor creating a new set of
   * {@code CopyableSeekableByteChannel} objects for some base channel.  The
   * copy constructor would still be the one called, since its type is more
   * specific, but that's fragile; it'd be easy to tweak the signature of the
   * constructor used for copies without changing callers, which would silently
   * fall back to using the public constructor.  So instead, we're careful to
   * give this internal constructor its own unique signature.
   */
  private CopyableSeekableByteChannel(Sync sync, long pos) {
    this.sync = checkNotNull(sync);
    checkState(sync.base.isOpen(),
        "the base SeekableByteChannel is not open");
    synchronized (sync) {
      sync.refCount++;
    }
    this.pos = pos;
    this.closed = false;
  }

  /**
   * Creates a new {@link CopyableSeekableByteChannel} derived from an existing
   * channel, referencing the same base channel.
   */
  public CopyableSeekableByteChannel copy() throws IOException {
    synchronized (sync) {
      if (closed) {
        throw new ClosedChannelException();
      }
      return new CopyableSeekableByteChannel(sync, pos);
    }
  }

  // SeekableByteChannel implementation

  @Override
  public long position() throws IOException {
    synchronized (sync) {
      if (closed) {
        throw new ClosedChannelException();
      }
      return pos;
    }
  }

  @Override
  public CopyableSeekableByteChannel position(long newPosition)
      throws IOException {
    synchronized (sync) {
      if (closed) {
        throw new ClosedChannelException();
      }
      // Verify that the position is valid for the base channel.
      sync.base.position(newPosition);
      this.pos = newPosition;
      this.sync.position = newPosition;
    }
    return this;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    synchronized (sync) {
      if (closed) {
        throw new ClosedChannelException();
      }
      reposition();
      int bytesRead = sync.base.read(dst);
      notePositionAdded(bytesRead);
      return bytesRead;
    }
  }

  @Override
  public long size() throws IOException {
    synchronized (sync) {
      if (closed) {
        throw new ClosedChannelException();
      }
      return sync.base.size();
    }
  }

  @Override
  public CopyableSeekableByteChannel truncate(long size) throws IOException {
    synchronized (sync) {
      if (closed) {
        throw new ClosedChannelException();
      }
      sync.base.truncate(size);
      return this;
    }
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    synchronized (sync) {
      if (closed) {
        throw new ClosedChannelException();
      }
      reposition();
      int bytesWritten = sync.base.write(src);
      notePositionAdded(bytesWritten);
      return bytesWritten;
    }
  }

  @Override
  public boolean isOpen() {
    synchronized (sync) {
      if (closed) {
        return false;
      }
      return sync.base.isOpen();
    }
  }

  @Override
  public void close() throws IOException {
    synchronized (sync) {
      if (closed) {
        return;
      }
      closed = true;
      sync.refCount--;
      if (sync.refCount == 0) {
        sync.base.close();
      }
    }
  }

  /**
   * Updates the base stream's position to match the position required by this
   * {@link CopyableSeekableByteChannel}.
   */
  @GuardedBy("sync")
  private void reposition() throws IOException {
    if (pos != sync.position) {
      sync.base.position(pos);
      sync.position = pos;
    }
  }

  /**
   * Notes that the specified amount has been logically added to the current
   * stream's position.
   */
  @GuardedBy("sync")
  private void notePositionAdded(int amount) {
    if (amount < 0) {
      return;  // Handles EOF indicators.
    }
    pos += amount;
    sync.position += amount;
  }

  /**
   * A simple value type used to synchronize a set of
   * {@link CopyableSeekableByteChannel} instances referencing a single
   * underlying channel.
   */
  private static final class Sync {
    // N.B. Another way to do this would be to implement something like a
    // RefcountingForwardingSeekableByteChannel.  Doing so would have the
    // advantage of clearly isolating the mutable state, at the cost of a lot
    // more code.
    public final SeekableByteChannel base;
    @GuardedBy("this") public long refCount = 0;
    @GuardedBy("this") public long position = 0;

    public Sync(SeekableByteChannel base) throws IOException {
      this.base = checkNotNull(base);
      position = base.position();
    }
  }
}
