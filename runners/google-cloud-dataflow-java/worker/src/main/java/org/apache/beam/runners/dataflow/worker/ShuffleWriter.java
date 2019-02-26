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

/** ShuffleWriter writes chunks of records to a shuffle dataset. */
interface ShuffleWriter extends AutoCloseable {
  /**
   * Writes a chunk of records. The chunk is a sequence of pairs encoded as:
   * <key-size><key><secondary-key-size><secondary-key><value-size><value> where the sizes are
   * 4-byte big-endian integers.
   */
  public void write(byte[] chunk) throws IOException;

  /** Flushes written records and closes this writer. */
  @Override
  public void close() throws IOException;
}
