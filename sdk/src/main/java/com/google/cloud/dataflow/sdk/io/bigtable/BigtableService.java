/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.io.bigtable;

import com.google.bigtable.v1.Mutation;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;

import java.io.IOException;
import java.io.Serializable;

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
    ListenableFuture<Empty> writeRecord(KV<ByteString, Iterable<Mutation>> record)
        throws IOException;

    /**
     * Closes the writer.
     *
     * @throws IOException if any writes did not succeed
     */
    void close() throws IOException;
  }

  /**
   * Returns {@code true} if the table with the give name exists.
   */
  boolean tableExists(String tableId) throws IOException;

  /**
   * Returns a {@link Writer} that will write to the specified table.
   */
  Writer openForWriting(String tableId) throws IOException;
}
