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
package org.apache.beam.runners.dataflow.worker;

import java.nio.ByteBuffer;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Ints;

class ByteArrayReader {
  private static final ByteBuffer EMPTY = ByteBuffer.allocate(0).asReadOnlyBuffer();

  private final byte[] arr;
  private int pos;

  public ByteArrayReader(byte[] arr) {
    this.arr = arr;
    this.pos = 0;
  }

  public int available() {
    return arr.length - pos;
  }

  public int readInt() {
    int ret = Ints.fromBytes(arr[pos], arr[pos + 1], arr[pos + 2], arr[pos + 3]);
    pos += 4;
    return ret;
  }

  public ByteBuffer read(int size) {
    if (size == 0) {
      return EMPTY;
    }

    ByteBuffer ret = ByteBuffer.wrap(arr, pos, size);
    pos += size;
    return ret;
  }
}
