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
package org.apache.beam.sdk.io.aws2.sqs;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class SqsCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {

  /**
   * The reader this checkpoint was created from.
   *
   * <p><b>Not persisted</b> in durable checkpoint, {@code null} after restoring the checkpoint.
   *
   * <p><b>CAUTION:</b>Between a checkpoint being taken and {@link #finalizeCheckpoint()} being
   * called the 'true' active reader may have changed.
   */
  private transient Optional<SqsUnboundedReader> reader;

  /**
   * Contains message ids that have been passed downstream since the last checkpoint.
   *
   * <p>Corresponding messages have to be purged from SQS when finalizing the checkpoint to prevent
   * re-delivery.
   *
   * <p><b>Not persisted</b> in durable checkpoint, {@code null} after restoring the checkpoint.
   */
  private @Nullable List<String> safeToDeleteIds;

  /**
   * Contains receipt handles of messages which have been received from SQS, but not yet passed
   * downstream at the time of the snapshot.
   *
   * <p>When restoring from a checkpoint, the visibility timeout of corresponding messages is set to
   * {@code 0} to trigger immediate re-delivery.
   */
  @VisibleForTesting final List<String> notYetReadReceipts;

  SqsCheckpointMark(
      SqsUnboundedReader reader,
      Collection<String> messagesToDelete,
      Collection<String> notYetReadReceipts) {
    this.reader = Optional.of(reader);
    this.safeToDeleteIds = ImmutableList.copyOf(messagesToDelete);
    this.notYetReadReceipts = ImmutableList.copyOf(notYetReadReceipts);
  }

  @Override
  public void finalizeCheckpoint() throws IOException {
    checkState(
        reader.isPresent() && safeToDeleteIds != null, "Cannot finalize a restored checkpoint");
    // Even if the 'true' active reader has changed since the checkpoint was taken we are
    // fine:
    // - The underlying SQS topic will not have changed, so the following deletes will still
    // go to the right place.
    // - We'll delete the ACK ids from the readers in-flight state, but that only affect
    // flow control and stats, neither of which are relevant anymore.
    try {
      reader.get().delete(safeToDeleteIds);
    } finally {
      int remainingInFlight = reader.get().numInFlightCheckpoints.decrementAndGet();
      checkState(remainingInFlight >= 0, "Miscounted in-flight checkpoints");
      reader.get().maybeCloseClient();
      reader = Optional.empty();
      safeToDeleteIds = null;
    }
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SqsCheckpointMark that = (SqsCheckpointMark) o;
    return Objects.equal(safeToDeleteIds, that.safeToDeleteIds);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(safeToDeleteIds);
  }
}
