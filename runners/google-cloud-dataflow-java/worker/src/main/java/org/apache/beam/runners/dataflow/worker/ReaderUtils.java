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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;

/** Utilities for working with {@link NativeReader} objects. */
public class ReaderUtils {
  /**
   * Creates a {@link NativeReader.NativeReaderIterator} from the given {@link NativeReader} and
   * reads it to the end.
   *
   * @param reader {@link NativeReader} to read from
   */
  public static <T> List<T> readAllFromReader(NativeReader<T> reader) throws IOException {
    try (NativeReader.NativeReaderIterator<T> iterator = reader.iterator()) {
      return readRemainingFromIterator(iterator, false);
    }
  }

  /**
   * Read elements from a {@link NativeReader.NativeReaderIterator} until either the reader is
   * exhausted, or {@code n} elements are read. Specifying {@code n == Integer.MAX_VALUE} means read
   * all remaining elements.
   */
  public static <T> List<T> readNItemsFromIterator(
      NativeReader.NativeReaderIterator<T> reader, int n, boolean started) throws IOException {
    List<T> res = new ArrayList<>();
    for (long i = 0; n == Integer.MAX_VALUE || i < n; i++) {
      if (!((i == 0 && !started) ? reader.start() : reader.advance())) {
        break;
      }
      res.add(reader.getCurrent());
    }
    return res;
  }

  /**
   * Read elements from a {@link NativeReader.NativeReaderIterator} until either the reader is
   * exhausted, or n elements are read. Specifying {@code n == Integer.MAX_VALUE} means read all
   * remaining elements.
   */
  public static <T> List<T> readNItemsFromUnstartedIterator(
      NativeReader.NativeReaderIterator<T> reader, int n) throws IOException {
    return readNItemsFromIterator(reader, n, false);
  }

  /**
   * Read elements from a {@link NativeReader.NativeReaderIterator} until the reader is exhausted.
   */
  public static <T> List<T> readRemainingFromIterator(
      NativeReader.NativeReaderIterator<T> reader, boolean started) throws IOException {
    return readNItemsFromIterator(reader, Integer.MAX_VALUE, started);
  }
}
