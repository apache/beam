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
package org.apache.beam.runners.spark.translation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;

/**
 * A holder object that lets you serialize an element with a Coder with minimal wasted space.
 * Supports both Kryo and Java serialization.
 *
 * <p>There are two different representations: a deserialized representation and a serialized
 * representation.
 *
 * <p>The deserialized representation stores a Coder and the value. To serialize the value, we write
 * a length-prefixed encoding of value, but do NOT write the Coder used.
 *
 * <p>The serialized representation just reads a byte array - the value is not deserialized fully.
 * In order to get at the deserialized value, the caller must pass the Coder used to create this
 * instance via getOrDecode(Coder). This reverts the representation back to the deserialized
 * representation.
 *
 * @param <T> element type
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public final class ValueAndCoderLazySerializable<T> implements Serializable {
  private T value;
  // Re-use a field to save space in-memory. This is either a byte[] or a Coder, depending on
  // which representation we are in.
  private Object coderOrBytes;

  private ValueAndCoderLazySerializable(T value, Coder<T> currentCoder) {
    this.value = value;
    this.coderOrBytes = currentCoder;
  }

  @SuppressWarnings("unused") // for serialization
  ValueAndCoderLazySerializable() {}

  public static <T> ValueAndCoderLazySerializable<T> of(T value, Coder<T> coder) {
    return new ValueAndCoderLazySerializable<>(value, coder);
  }

  public T getOrDecode(Coder<T> coder) {
    if (!(coderOrBytes instanceof Coder)) {
      ByteArrayInputStream bais = new ByteArrayInputStream((byte[]) this.coderOrBytes);
      try {
        value = coder.decode(bais);
      } catch (IOException e) {
        throw new IllegalStateException("Error decoding bytes for coder: " + coder, e);
      }
      this.coderOrBytes = coder;
    }

    return value;
  }

  private static class ByteSizeObserver extends ElementByteSizeObserver {
    private long observedSize = 0;

    @Override
    protected void reportElementSize(long elementByteSize) {
      observedSize += elementByteSize;
    }
  }

  void writeCommon(OutputStream out) throws IOException {
    if (!(coderOrBytes instanceof Coder)) {
      byte[] bytes = (byte[]) coderOrBytes;
      VarInt.encode(bytes.length, out);
      out.write(bytes);
    } else {
      @SuppressWarnings("unchecked")
      Coder<T> coder = (Coder<T>) coderOrBytes;
      int bufferSize = 1024;
      if (coder.isRegisterByteSizeObserverCheap(value)) {
        try {
          ByteSizeObserver observer = new ByteSizeObserver();
          coder.registerByteSizeObserver(value, observer);
          bufferSize = (int) observer.observedSize;
        } catch (Exception e) {
          Throwables.throwIfUnchecked(e);
          throw new RuntimeException(e);
        }
      }

      ByteArrayOutputStream bytes = new ByteArrayOutputStream(bufferSize);
      try {
        coder.encode(value, bytes);
        VarInt.encode(bytes.size(), out);
        bytes.writeTo(out);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  void readCommon(InputStream in) throws IOException {
    int length = VarInt.decodeInt(in);
    byte[] bytes = new byte[length];
    ByteStreams.readFully(in, bytes);
    this.coderOrBytes = bytes;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    writeCommon(out);
  }

  private void readObject(ObjectInputStream in) throws IOException {
    readCommon(in);
  }
}
