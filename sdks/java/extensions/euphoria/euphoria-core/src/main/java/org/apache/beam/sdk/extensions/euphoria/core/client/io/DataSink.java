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
package org.apache.beam.sdk.extensions.euphoria.core.client.io;

import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;

import java.io.IOException;
import java.io.Serializable;

/**
 * Sink for a dataset.
 *
 * @param <T> the type of the element consumed by this sink
 */
@Audience(Audience.Type.CLIENT)
public interface DataSink<T> extends Serializable {

  /**
   * Perform initialization before writing to the sink. Called before writing begins. It must be
   * ensured that implementation of this method is idempotent (may be called more than once in the
   * case of failure/retry).
   */
  default void initialize() {}

  /**
   * Open {@link Writer} for given partition id (zero based).
   *
   * @param partitionId the id of the partition to open for write access
   * @return a writer to the specified partition
   */
  Writer<T> openWriter(int partitionId);

  /**
   * Commit all partitions.
   *
   * @throws IOException if commit written out data fails for some reason
   */
  void commit() throws IOException;

  /**
   * Rollback all partitions.
   *
   * @throws IOException if rolling back written out data fails for some reason
   */
  void rollback() throws IOException;

  /**
   * Called when adding output sink to the resulting DAG for processing. Purpose of this method is
   * to enable sink to add arbitrary transformations to the dataset, before it gets persisted via
   * this sink.
   *
   * @param output the dataset being written by this sink
   * @return true if the flow was modified, i.e. if any transformation was applied on the output.
   *     Note that, in that case you have to call {@link Dataset#persist} manually on the resulting
   *     dataset.
   */
  default boolean prepareDataset(Dataset<T> output) {
    // by default do nothing
    return false;
  }
}
