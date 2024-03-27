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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Enumeration;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CancellationException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An InputStream that can be dynamically extended with additional InputStreams. */
@SuppressWarnings("JdkObsolete")
final class AppendableInputStream extends InputStream {
  private static final Logger LOG = LoggerFactory.getLogger(AppendableInputStream.class);
  private static final int QUEUE_MAX_CAPACITY = 10;
  private static final InputStream POISON_PILL = ByteString.EMPTY.newInput();

  private final AtomicBoolean cancelled;
  private final AtomicBoolean complete;
  private final AtomicLong blockedStartMs;
  private final BlockingDeque<InputStream> queue;
  private final InputStream stream;

  AppendableInputStream() {
    this.cancelled = new AtomicBoolean(false);
    this.complete = new AtomicBoolean(false);
    this.blockedStartMs = new AtomicLong();
    this.queue = new LinkedBlockingDeque<>(QUEUE_MAX_CAPACITY);
    this.stream = new SequenceInputStream(new InputStreamEnumeration());
  }

  long getBlockedStartMs() {
    return blockedStartMs.get();
  }

  boolean isComplete() {
    return complete.get();
  }

  boolean isCancelled() {
    return cancelled.get();
  }

  int size() {
    return queue.size();
  }

  /** Appends a new InputStream to the tail of this stream. */
  synchronized void append(InputStream chunk) {
    try {
      queue.put(chunk);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.debug("interrupted append");
    }
  }

  /** Cancels the stream. Future calls to InputStream methods will throw CancellationException. */
  synchronized void cancel() {
    cancelled.set(true);
    try {
      // Put the poison pill at the head of the queue to cancel as quickly as possible.
      queue.clear();
      queue.putFirst(POISON_PILL);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.debug("interrupted cancel");
    }
  }

  /** Signals that no new InputStreams will be added to this stream. */
  synchronized void complete() {
    complete.set(true);
    try {
      queue.put(POISON_PILL);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.debug("interrupted complete");
    }
  }

  @Override
  public int read() throws IOException {
    if (cancelled.get()) {
      throw new CancellationException();
    }
    return stream.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (cancelled.get()) {
      throw new CancellationException();
    }
    return stream.read(b, off, len);
  }

  @Override
  public int available() throws IOException {
    if (cancelled.get()) {
      throw new CancellationException();
    }
    return stream.available();
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }

  @SuppressWarnings("NullableProblems")
  private class InputStreamEnumeration implements Enumeration<InputStream> {
    // The first stream is eagerly read on SequenceInputStream creation. For this reason
    // we use an empty element as the first input to avoid blocking from the queue when
    // creating the AppendableInputStream.
    private @Nullable InputStream current = POISON_PILL;

    @Override
    public boolean hasMoreElements() {
      if (current != null) {
        return true;
      }

      try {
        blockedStartMs.set(Instant.now().getMillis());
        current = queue.poll(180, TimeUnit.SECONDS);
        if (current != null && current != POISON_PILL) {
          return true;
        }
        if (cancelled.get()) {
          throw new CancellationException();
        }
        if (complete.get()) {
          return false;
        }
        throw new IllegalStateException("Got poison pill or timeout but stream is not done.");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new CancellationException();
      }
    }

    @SuppressWarnings("return")
    @Override
    public InputStream nextElement() {
      if (!hasMoreElements()) {
        throw new NoSuchElementException();
      }
      blockedStartMs.set(0);
      InputStream next = current;
      current = null;
      return next;
    }
  }
}
