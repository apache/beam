/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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

import cz.seznam.euphoria.core.annotation.audience.Audience;
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
   *  Perform initialization before writing to the sink.
   *  Called before writing begins. It must be ensured that
   *  implementation of this method is idempotent (may be called
   *  more than once in the case of failure/retry).
   */
  default void initialize() {}

  /**
   * Open {@link Writer} for given partition id (zero based).
   *
   * @param partitionId the id of the partition to open for write access
   *
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
}
