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
package cz.seznam.euphoria.core.client.io;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import java.io.Closeable;
import java.io.IOException;

/**
 * Writer for data to a particular partition.
 *
 * @param <T> the type of elements being writing by this writer
 */
@Audience(Audience.Type.CLIENT)
public interface Writer<T> extends Closeable {

  /**
   * Write element to the output.
   *
   * @param elem the element to write
   * @throws IOException if performing the write fails for some reason
   */
  void write(T elem) throws IOException;

  /**
   * Flush all pending writes to output. This method might be called multiple times, but is always
   * called just before {@code commit} or {@code rollback}.
   *
   * @throws IOException failure upon writing pending data
   */
  default void flush() throws IOException {}

  /**
   * Commit the write process.
   *
   * @throws IOException failure to perform the commit
   */
  void commit() throws IOException;

  /**
   * Rollback the write process. Optional operation.
   *
   * @throws IOException failure to perform the corresponding clean up
   */
  default void rollback() throws IOException {}

  /**
   * Close the writer and release all its resources. This method will be called as the last method
   * on this object.
   *
   * @throws IOException failure to perform the corresponding clean up
   */
  @Override
  void close() throws IOException;
}
