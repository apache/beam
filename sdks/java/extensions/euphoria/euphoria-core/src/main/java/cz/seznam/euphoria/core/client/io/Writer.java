/**
 * Copyright 2016 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.client.io;

import java.io.Closeable;
import java.io.IOException;

/**
 * Writer for data to a particular partition.
 */
public interface Writer<T> extends Closeable {

  /** Write element to the output. */
  void write(T elem) throws IOException;


  /**
   * Flush all pending writes to output.
   * This method might be called multiple times, but is always called
   * just before {@code commit} or {@code rollback}.
   **/
  default void flush() throws IOException {

  }

  /** Commit the write process. */
  void commit() throws IOException;

  /** Rollback the write process. Optional operation. */
  default void rollback() throws IOException {}

  /**
   * Close the writer and release all its resources.
   * This method will be called as the last method on this object.
   **/
  @Override
  void close() throws IOException;
  
}
