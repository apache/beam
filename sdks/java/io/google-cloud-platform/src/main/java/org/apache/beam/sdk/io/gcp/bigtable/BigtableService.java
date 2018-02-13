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
package org.apache.beam.sdk.io.gcp.bigtable;

import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.SampleRowKeysResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletionStage;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO.BigtableSource;
import org.apache.beam.sdk.values.KV;

/**
 * An interface for real or fake implementations of Cloud Bigtable.
 */
interface BigtableService extends Serializable {

  /**
   * The interface of a class that can write to Cloud Bigtable.
   */
  interface Writer {
    /**
     * Writes a single row transaction to Cloud Bigtable. The key of the {@code record} is the
     * row key to be mutated and the iterable of mutations represent the changes to be made to the
     * row.
     *
     * @throws IOException if there is an error submitting the write.
     */
    CompletionStage<MutateRowResponse> writeRecord(KV<ByteString, Iterable<Mutation>> record)
        throws IOException;

    /**
     * Flushes the writer.
     *
     * @throws IOException if any writes did not succeed
     */
    void flush() throws IOException;

    /**
     * Closes the writer.
     *
     * @throws IOException if there is an error closing the writer
     */
    void close() throws IOException;
  }

  /**
   * The interface of a class that reads from Cloud Bigtable.
   */
  interface Reader {
    /**
     * Reads the first element (including initialization, such as opening a network connection) and
     * returns true if an element was found.
     */
    boolean start() throws IOException;

    /**
     * Attempts to read the next element, and returns true if an element has been read.
     */
    boolean advance() throws IOException;

    /**
     * Closes the reader.
     *
     * @throws IOException if there is an error.
     */
    void close() throws IOException;

    /**
     * Returns the last row read by a successful start() or advance(), or throws if there is no
     * current row because the last such call was unsuccessful.
     */
    Row getCurrentRow() throws NoSuchElementException;
  }

  /**
   * Returns the BigtableOptions used to configure this BigtableService.
   */
  BigtableOptions getBigtableOptions();

  /**
   * Returns {@code true} if the table with the give name exists.
   */
  boolean tableExists(String tableId) throws IOException;

  /**
   * Returns a {@link Reader} that will read from the specified source.
   */
  Reader createReader(BigtableSource source) throws IOException;

  /**
   * Returns a {@link Writer} that will write to the specified table.
   */
  Writer openForWriting(String tableId) throws IOException;

  /**
   * Returns a set of row keys sampled from the underlying table. These contain information about
   * the distribution of keys within the table.
   */
  List<SampleRowKeysResponse> getSampleRowKeys(BigtableSource source) throws IOException;
}
