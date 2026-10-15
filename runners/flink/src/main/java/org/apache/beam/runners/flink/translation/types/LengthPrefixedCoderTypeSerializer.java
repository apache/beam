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
package org.apache.beam.runners.flink.translation.types;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link CoderTypeSerializer} that prefixes every record with its encoded length.
 *
 * <p>Beam coders do not delimit their own output, so {@link CoderTypeSerializer#copy(DataInputView,
 * DataOutputView)} has to decode and re-encode a record to find where it ends. Flink calls that
 * method for every record when its batch sorter spills to disk. With a length prefix, copying is a
 * plain byte copy.
 *
 * <p>The format differs from {@link CoderTypeSerializer}, so this serializer must only be used for
 * data that is never persisted across job versions, i.e. in batch execution.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class LengthPrefixedCoderTypeSerializer<T> extends CoderTypeSerializer<T> {

  private static final long serialVersionUID = 1L;

  /** Scratch buffers above this size are released after use instead of being kept around. */
  private static final int MAX_RETAINED_BUFFER_SIZE = 1 << 20;

  private final Coder<T> coder;

  private final boolean fasterCopy;

  private transient @Nullable ExposedByteArrayOutputStream encodeBuffer;

  private transient byte @Nullable [] decodeBuffer;

  public LengthPrefixedCoderTypeSerializer(Coder<T> coder, boolean fasterCopy) {
    super(coder, fasterCopy);
    this.coder = coder;
    this.fasterCopy = fasterCopy;
  }

  @Override
  public LengthPrefixedCoderTypeSerializer<T> duplicate() {
    return new LengthPrefixedCoderTypeSerializer<>(coder, fasterCopy);
  }

  @Override
  public void serialize(T t, DataOutputView dataOutputView) throws IOException {
    ExposedByteArrayOutputStream buffer = encodeBuffer;
    if (buffer == null) {
      buffer = new ExposedByteArrayOutputStream();
      encodeBuffer = buffer;
    }
    buffer.reset();
    coder.encode(t, buffer);
    int length = buffer.size();
    dataOutputView.writeInt(length);
    dataOutputView.write(buffer.buffer(), 0, length);
    if (buffer.capacity() > MAX_RETAINED_BUFFER_SIZE) {
      encodeBuffer = null;
    }
  }

  @Override
  public T deserialize(DataInputView dataInputView) throws IOException {
    int length = dataInputView.readInt();
    byte[] buffer = decodeBuffer;
    if (buffer == null || buffer.length < length) {
      buffer = new byte[Math.max(length, 64)];
    }
    dataInputView.readFully(buffer, 0, length);
    decodeBuffer = buffer.length > MAX_RETAINED_BUFFER_SIZE ? null : buffer;
    try {
      return coder.decode(new ByteArrayInputStream(buffer, 0, length));
    } catch (CoderException e) {
      Throwable cause = e.getCause();
      if (cause instanceof EOFException) {
        throw (EOFException) cause;
      } else {
        throw e;
      }
    }
  }

  @Override
  public void copy(DataInputView dataInputView, DataOutputView dataOutputView) throws IOException {
    int length = dataInputView.readInt();
    dataOutputView.writeInt(length);
    dataOutputView.write(dataInputView, length);
  }

  @Override
  public boolean equals(@Nullable Object o) {
    // CoderTypeSerializer#equals compares isLengthPrefixed(), which this class overrides, so a
    // length-prefixed serializer never equals one that writes records without the prefix.
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  boolean isLengthPrefixed() {
    return true;
  }

  @Override
  public String toString() {
    return "LengthPrefixedCoderTypeSerializer{" + "coder=" + coder + '}';
  }

  /** A {@link ByteArrayOutputStream} that exposes its buffer to avoid copying it. */
  private static class ExposedByteArrayOutputStream extends ByteArrayOutputStream {
    ExposedByteArrayOutputStream() {
      super(256);
    }

    byte[] buffer() {
      return buf;
    }

    int capacity() {
      return buf.length;
    }
  }
}
