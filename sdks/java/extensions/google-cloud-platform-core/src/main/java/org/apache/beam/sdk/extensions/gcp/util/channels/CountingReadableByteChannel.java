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
import java.util.function.Consumer;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;

public class CountingReadableByteChannel implements ReadableByteChannel {

  private final ReadableByteChannel delegate;
  private final Consumer<Integer> bytesReadConsumer;

  // since it's equivalent to directly using the delegate, there's no point in using this variant
  // elsewhere than in tests
  @VisibleForTesting
  public static CountingReadableByteChannel createWithNoOpConsumer(ReadableByteChannel delegate) {
    return new CountingReadableByteChannel(delegate, __ -> {});
  }

  public CountingReadableByteChannel(
      ReadableByteChannel delegate, Consumer<Integer> bytesReadConsumer) {
    this.delegate = Preconditions.checkNotNull(delegate);
    this.bytesReadConsumer = Preconditions.checkNotNull(bytesReadConsumer);
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    int bytesRead = delegate.read(dst);
    if (bytesRead > 0) {
      bytesReadConsumer.accept(bytesRead);
    }
    return bytesRead;
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
