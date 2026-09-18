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
package org.apache.beam.sdk.io.solace.read;

import java.util.Objects;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checkpoint for an unbounded Solace source. Consists of the Solace messages waiting to be
 * acknowledged.
 */
@DefaultCoder(AvroCoder.class)
@Internal
@VisibleForTesting
public class SolaceCheckpointMark implements UnboundedSource.CheckpointMark {
  private static final Logger LOG = LoggerFactory.getLogger(SolaceCheckpointMark.class);
  private String readerUuid;
  private long checkpointId;

  @SuppressWarnings("initialization") // Avro will set the fields by breaking abstraction
  private SolaceCheckpointMark() {
    this.readerUuid = "";
    this.checkpointId = 0;
  }

  /**
   * Creates a new {@link SolaceCheckpointMark}.
   *
   * @param readerUuid - the UUID of the originating reader.
   * @param checkpointId - the unique ID of this checkpoint.
   */
  SolaceCheckpointMark(String readerUuid, long checkpointId) {
    this.readerUuid = readerUuid;
    this.checkpointId = checkpointId;
  }

  @Override
  public void finalizeCheckpoint() {
    if (readerUuid == null || readerUuid.isEmpty()) {
      LOG.warn("SolaceIO.Read: Checkpoint has no reader UUID, cannot finalize.");
      return;
    }
    UnboundedSolaceReader<?> reader = ActiveReadersRegistry.get(readerUuid);
    if (reader != null) {
      reader.finalizeCheckpoint(checkpointId);
    } else {
      LOG.warn(
          "SolaceIO.Read: Reader with UUID {} not found in registry. "
              + "Checkpoint {} cannot be finalized. Messages will be redelivered if session is closed.",
          readerUuid,
          checkpointId);
    }
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == null) {
      return false;
    }
    if (this == o) {
      return true;
    }
    if (!(o instanceof SolaceCheckpointMark)) {
      return false;
    }
    SolaceCheckpointMark that = (SolaceCheckpointMark) o;
    return checkpointId == that.checkpointId && Objects.equals(readerUuid, that.readerUuid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(readerUuid, checkpointId);
  }
}
