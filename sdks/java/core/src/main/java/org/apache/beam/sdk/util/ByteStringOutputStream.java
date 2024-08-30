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
package org.apache.beam.sdk.util;

import java.io.OutputStream;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.UnsafeByteOperations;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;

/**
 * An {@link OutputStream} that produces {@link ByteString}s.
 *
 * <p>Closing this output stream does nothing.
 *
 * <p>This class is not thread safe and expects appropriate locking to be used in a thread-safe
 * manner. This differs from {@link ByteString.Output} which synchronizes its writes.
 */
@NotThreadSafe
public final class ByteStringOutputStream extends OutputStream {

  // This constant was chosen based upon Protobufs ByteString#CONCATENATE_BY_COPY which
  // isn't public to prevent copying the bytes again when concatenating ByteStrings instead
  // of appending.
  private static final int DEFAULT_CAPACITY = 128;

  // ByteStringOutputStreamBenchmark.NewVsCopy shows that we actually are faster
  // creating a 4 new arrays that are 256k vs one that is 1024k by almost a factor
  // of 2.
  //
  // This number should be tuned periodically as hardware changes.
  private static final int MAX_CHUNK_SIZE = 256 * 1024;

  // ByteString to be concatenated to create the result
  private ByteString result;

  // Current buffer to which we are writing
  private byte[] buffer;

  // Location in buffer[] to which we write the next byte.
  private int bufferPos;

  /** Creates a new output stream with a default capacity. */
  public ByteStringOutputStream() {
    this(DEFAULT_CAPACITY);
  }

  /**
   * Creates a new output stream with the specified initial capacity.
   *
   * @param initialCapacity the initial capacity of the output stream.
   */
  public ByteStringOutputStream(int initialCapacity) {
    if (initialCapacity < 0) {
      throw new IllegalArgumentException("Initial capacity < 0");
    }
    this.buffer = new byte[initialCapacity];
    this.result = ByteString.EMPTY;
  }

  @Override
  public void write(int b) {
    if (bufferPos == buffer.length) {
      // We want to increase our total capacity by 50% but not larger than the max chunk size.
      result = result.concat(UnsafeByteOperations.unsafeWrap(buffer));
      buffer = new byte[Math.min(Math.max(1, result.size()), MAX_CHUNK_SIZE)];
      bufferPos = 0;
    }
    buffer[bufferPos++] = (byte) b;
  }

  @Override
  public void write(byte[] b, int offset, int length) {
    int remainingSpaceInBuffer = buffer.length - bufferPos;
    while (length > remainingSpaceInBuffer) {
      // Use up the current buffer
      System.arraycopy(b, offset, buffer, bufferPos, remainingSpaceInBuffer);
      offset += remainingSpaceInBuffer;
      length -= remainingSpaceInBuffer;

      result = result.concat(UnsafeByteOperations.unsafeWrap(buffer));
      // We want to increase our total capacity but not larger than the max chunk size.
      remainingSpaceInBuffer = Math.min(Math.max(length, result.size()), MAX_CHUNK_SIZE);
      buffer = new byte[remainingSpaceInBuffer];
      bufferPos = 0;
    }

    System.arraycopy(b, offset, buffer, bufferPos, length);
    bufferPos += length;
  }

  /**
   * Creates a byte string with the size and contents of this output stream.
   *
   * <p>Note that the caller must no longer use this object after this method. The internal buffer
   * is wrapped and thus mutations made by future {@link #write} or other methods may mutate {@link
   * ByteString}s returned in the past.
   */
  public ByteString toByteString() {
    // We specifically choose to concatenate here since the user won't be re-using the buffer.
    return result.concat(UnsafeByteOperations.unsafeWrap(buffer, 0, bufferPos));
  }

  private static boolean shouldCopy(int bufferLength, int bytesToCopy) {
    // These thresholds are from the results of ByteStringOutputStreamBenchmark.CopyVewNew
    // which show that at these thresholds we should copy the bytes instead to re-use
    // the existing buffer since creating a new one is more expensive.
    if (bufferLength <= 128) {
      // Always copy small byte arrays to prevent large chunks of wasted space
      // when dealing with very small amounts of data.
      return true;
    } else if (bufferLength <= 1024) {
      return bytesToCopy <= bufferLength * 0.875;
    } else if (bufferLength <= 8192) {
      return bytesToCopy <= bufferLength * 0.75;
    } else {
      return bytesToCopy <= bufferLength * 0.4375;
    }
  }

  /**
   * Creates a byte string with the size and contents of this output stream and resets the output
   * stream to be re-used possibly re-using any existing buffers.
   */
  public ByteString toByteStringAndReset() {
    ByteString rval;
    if (bufferPos > 0) {
      final boolean copy = shouldCopy(buffer.length, bufferPos);
      if (copy) {
        rval = result.concat(ByteString.copyFrom(buffer, 0, bufferPos));
      } else {
        rval = result.concat(UnsafeByteOperations.unsafeWrap(buffer, 0, bufferPos));
        buffer = new byte[Math.min(rval.size(), MAX_CHUNK_SIZE)];
      }
      bufferPos = 0;
    } else {
      rval = result;
    }
    result = ByteString.EMPTY;
    return rval;
  }

  /**
   * Creates a byte string with the given size containing the prefix of the contents of this output
   * stream.
   */
  public ByteString consumePrefixToByteString(int prefixSize) {
    ByteString rval;
    Preconditions.checkArgument(prefixSize <= size());
    if (prefixSize == size()) {
      return toByteStringAndReset();
    }
    int bytesFromBuffer = prefixSize - result.size();
    if (bytesFromBuffer == 0) {
      rval = result;
      result = ByteString.EMPTY;
      return rval;
    }
    if (bytesFromBuffer < 0) {
      rval = result.substring(0, prefixSize);
      result = result.substring(prefixSize);
      return rval;
    }
    // Copy or reference the kept bytes for the rval.
    if (shouldCopy(buffer.length, bytesFromBuffer)) {
      rval = result.concat(ByteString.copyFrom(buffer, 0, bytesFromBuffer));
    } else {
      rval = result.concat(UnsafeByteOperations.unsafeWrap(buffer, 0, bytesFromBuffer));
    }
    // Copy the remaining bytes to a new buffer.
    int remainingBytes = bufferPos - bytesFromBuffer;
    if (shouldCopy(buffer.length, remainingBytes)) {
      result = ByteString.copyFrom(buffer, bytesFromBuffer, remainingBytes);
    } else {
      result = UnsafeByteOperations.unsafeWrap(buffer, bytesFromBuffer, remainingBytes);
    }
    buffer = new byte[Math.min(Math.max(1, buffer.length), MAX_CHUNK_SIZE)];
    bufferPos = 0;
    return rval;
  }

  /**
   * Returns the current size of the output stream.
   *
   * @return the current size of the output stream
   */
  public int size() {
    return result.size() + bufferPos;
  }

  @Override
  public String toString() {
    return String.format(
        "<ByteStringOutputStream@%s size=%d>",
        Integer.toHexString(System.identityHashCode(this)), size());
  }
}
