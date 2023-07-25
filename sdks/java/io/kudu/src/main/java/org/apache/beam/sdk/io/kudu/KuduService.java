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
package org.apache.beam.sdk.io.kudu;

import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.kudu.client.KuduException;

/** An interface for real, mock, or fake implementations of Kudu services. */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
interface KuduService<T> extends Serializable {

  /**
   * Returns a {@link org.apache.beam.sdk.io.BoundedSource.BoundedReader} that will read from Kudu
   * using the spec from {@link org.apache.beam.sdk.io.kudu.KuduIO.KuduSource}.
   */
  BoundedSource.BoundedReader<T> createReader(KuduIO.KuduSource<T> source);

  /** Create a {@link Writer} that writes entities into the KKudu instance. */
  Writer createWriter(KuduIO.Write<T> spec) throws KuduException;

  /** Returns a list containing a serialized scanner per tablet. */
  List<byte[]> createTabletScanners(KuduIO.Read<T> spec) throws KuduException;

  /** Writer for an entity. */
  interface Writer<T> extends AutoCloseable, Serializable {

    /**
     * Opens a new session for writing. This must be called exactly once before calling {@link
     * #write(Object)}.
     */
    void openSession() throws KuduException;

    /**
     * Writes the entity to Kudu. A call to {@link #openSession()} must be made before writing.
     * Writes may be asynchronous in which case implementations must surface errors when the session
     * is closed.
     */
    void write(T entity) throws KuduException;

    /** Closes the session, surfacing any errors that may have occurred during writing. */
    void closeSession() throws Exception;
  }
}
