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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;

public class CountingSeekableByteChannel implements SeekableByteChannel {
  private final SeekableByteChannel delegate;
  private final WritableByteChannel writableDelegate;
  private final ReadableByteChannel readableDelegate;

  public static CountingSeekableByteChannel createWithBytesReadConsumer(
      SeekableByteChannel delegate, Consumer<Integer> bytesReadConsumer) {
    return new CountingSeekableByteChannel(delegate, bytesReadConsumer, null);
  }

  public static CountingSeekableByteChannel createWithBytesWrittenConsumer(
      SeekableByteChannel delegate, Consumer<Integer> bytesWrittenConsumer) {
    return new CountingSeekableByteChannel(delegate, null, bytesWrittenConsumer);
  }

  // since it's equivalent to directly using the delegate, there's no point in using this variant
  // elsewhere than in tests
  @VisibleForTesting
  public static CountingSeekableByteChannel createWithNoOpConsumer(SeekableByteChannel delegate) {
    Consumer<Integer> noop = __ -> {};
    return new CountingSeekableByteChannel(delegate, noop, noop);
  }

  public CountingSeekableByteChannel(
      SeekableByteChannel delegate,
      @Nullable Consumer<Integer> bytesReadConsumer,
      @Nullable Consumer<Integer> bytesWrittenConsumer) {

    this.delegate = Preconditions.checkNotNull(delegate);
    Preconditions.checkArgument(
        bytesReadConsumer != null || bytesWrittenConsumer != null,
        "At least one of bytesReadConsumer or bytesWrittenConsumer must be non-null");
    this.readableDelegate =
        Optional.ofNullable(bytesReadConsumer)
            .<ReadableByteChannel>map(
                consumer -> new CountingReadableByteChannel(delegate, consumer))
            .orElse(delegate);
    this.writableDelegate =
        Optional.ofNullable(bytesWrittenConsumer)
            .<WritableByteChannel>map(
                consumer -> new CountingWritableByteChannel(delegate, consumer))
            .orElse(delegate);
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    return readableDelegate.read(dst);
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    return writableDelegate.write(src);
  }

  @Override
  public long position() throws IOException {
    return delegate.position();
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    delegate.position(newPosition);
    return this;
  }

  @Override
  public long size() throws IOException {
    return delegate.size();
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    delegate.truncate(size);
    return this;
  }

  @Override
  public boolean isOpen() {
    return delegate.isOpen();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
