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
package org.apache.beam.io.debezium;

import java.io.Serializable;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Strategy interface for persisting and restoring Debezium connector offsets across pipeline
 * restarts.
 *
 * <p>When configured via {@link DebeziumIO.Read#withOffsetRetainer(OffsetRetainer)}, the pipeline
 * behaves as follows:
 *
 * <ol>
 *   <li>On startup, {@link #loadOffset()} is called once. If a non-null offset is returned, the
 *       Debezium connector resumes from that position; otherwise it starts from the beginning of
 *       the change stream.
 *   <li>After each successful {@code task.commit()}, {@link #saveOffset(Map)} is called with the
 *       latest committed offset.
 * </ol>
 *
 * <p>A ready-to-use filesystem-based implementation is provided by {@link
 * FileSystemOffsetRetainer}, which supports any Beam-compatible filesystem (local, GCS, S3, etc.)
 *
 * <p>Implementations must be {@link Serializable} because they are embedded inside {@link
 * DebeziumIO.Read}, which is a {@link org.apache.beam.sdk.transforms.PTransform} that gets
 * serialized and shipped to workers.
 */
public interface OffsetRetainer extends Serializable {

  /**
   * Returns the most recently saved offset, or {@code null} if no offset has been saved yet.
   *
   * <p>A {@code null} return causes the connector to start from the beginning of the change stream.
   * Implementations should handle transient I/O errors gracefully and return {@code null} on
   * failure rather than propagating an exception.
   */
  @Nullable
  Map<String, Object> loadOffset();

  /**
   * Persists the given offset so it can be recovered after a pipeline restart.
   *
   * <p>Called after each successful {@code task.commit()} with the latest committed offset.
   * Implementations should swallow transient errors rather than throwing, so that a failed save
   * does not terminate the pipeline.
   *
   * @param offset The current connector offset, as returned by {@link
   *     org.apache.kafka.connect.source.SourceRecord#sourceOffset()}.
   */
  void saveOffset(Map<String, Object> offset);
}
