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
package org.apache.beam.sdk.fn.windowing;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.joda.time.Instant;

/**
 * An encoded {@link BoundedWindow} used within Runners to track window information without needing
 * to decode the window.
 *
 * <p>This allows for Runners to not need to know window format during execution.
 */
@AutoValue
public abstract class EncodedBoundedWindow extends BoundedWindow {
  public static EncodedBoundedWindow forEncoding(ByteString encodedWindow) {
    return new AutoValue_EncodedBoundedWindow(encodedWindow);
  }

  public abstract ByteString getEncodedWindow();

  @Override
  public Instant maxTimestamp() {
    throw new UnsupportedOperationException(
        "TODO: Add support for reading the timestamp from " + "the encoded window.");
  }

  /**
   * An {@link Coder} for {@link EncodedBoundedWindow}s.
   *
   * <p>This is a copy of {@code ByteStringCoder} to prevent a dependency on {@code
   * beam-java-sdk-extensions-protobuf}.
   */
  public static class Coder extends AtomicCoder<EncodedBoundedWindow> {
    public static final Coder INSTANCE = new Coder();

    // prevent instantiation
    private Coder() {}

    @Override
    public void encode(EncodedBoundedWindow value, OutputStream outStream)
        throws CoderException, IOException {
      VarInt.encode(value.getEncodedWindow().size(), outStream);
      value.getEncodedWindow().writeTo(outStream);
    }

    @Override
    public EncodedBoundedWindow decode(InputStream inStream) throws CoderException, IOException {
      int size = VarInt.decodeInt(inStream);
      ByteString encodedWindow = ByteString.readFrom(ByteStreams.limit(inStream, size));
      return EncodedBoundedWindow.forEncoding(encodedWindow);
    }

    @Override
    public boolean consistentWithEquals() {
      return true;
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(EncodedBoundedWindow value) {
      return true;
    }

    @Override
    protected long getEncodedElementByteSize(EncodedBoundedWindow value) throws Exception {
      return (long) VarInt.getLength(value.getEncodedWindow().size())
          + value.getEncodedWindow().size();
    }
  }
}
