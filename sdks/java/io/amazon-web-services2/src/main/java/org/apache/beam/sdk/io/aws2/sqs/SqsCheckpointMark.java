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
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class SqsCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {

  // Pinned to the value computed before readerId and sequence were added, so checkpoints written
  // by older versions can still be decoded.
  private static final long serialVersionUID = 6453448298069362204L;

  private static final Logger LOG = LoggerFactory.getLogger(SqsCheckpointMark.class);

  /**
   * The reader that took this checkpoint and its position among that reader's checkpoints, so a
   * restored copy can locate the live reader in this JVM. May be {@code null} when decoded from an
   * older version or constructed without a registry.
   */
  private final @Nullable String readerId;

  private final long sequence;

  /**
   * The reader this checkpoint was created from.
   *
   * <p><b>Not persisted</b> in durable checkpoint, {@code null} after restoring the checkpoint.
   */
  private transient Optional<SqsUnboundedReader> reader;

  private transient @Nullable PendingCheckpoints pendingCheckpoints;

  /**
   * Contains message ids that have been passed downstream since the last checkpoint.
   *
   * <p>Corresponding messages have to be purged from SQS when finalizing the checkpoint to prevent
   * re-delivery.
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
    this(null, 0, reader, messagesToDelete, notYetReadReceipts);
  }

  SqsCheckpointMark(
      @Nullable PendingCheckpoints pendingCheckpoints,
      long sequence,
      SqsUnboundedReader reader,
      Collection<String> messagesToDelete,
      Collection<String> notYetReadReceipts) {
    this.pendingCheckpoints = pendingCheckpoints;
    this.readerId = pendingCheckpoints == null ? null : pendingCheckpoints.readerId;
    this.sequence = sequence;
    this.reader = Optional.of(reader);
    this.safeToDeleteIds = ImmutableList.copyOf(messagesToDelete);
    this.notYetReadReceipts = ImmutableList.copyOf(notYetReadReceipts);
  }

  @Override
  public void finalizeCheckpoint() throws IOException {
    PendingCheckpoints pending = pendingCheckpoints;
    if (pending == null && readerId != null) {
      pending = PendingCheckpoints.LIVE.getIfPresent(readerId);
    }
    if (pending != null) {
      List<SqsCheckpointMark> checkpoints = pending.removeUpTo(sequence);
      if (checkpoints.isEmpty()) {
        return;
      }
      Set<String> toDelete = new LinkedHashSet<>();
      for (SqsCheckpointMark checkpoint : checkpoints) {
        if (checkpoint.safeToDeleteIds != null) {
          toDelete.addAll(checkpoint.safeToDeleteIds);
        }
      }
      try {
        pending.reader.delete(new ArrayList<>(toDelete));
      } finally {
        for (SqsCheckpointMark checkpoint : checkpoints) {
          checkpoint.clearAndDecrement();
        }
        pending.reader.maybeCloseClient();
      }
      return;
    }

    if (reader == null || !reader.isPresent()) {
      LOG.debug(
          "Finalized checkpoint {}#{}: its reader is not in this JVM, nothing to acknowledge",
          readerId,
          sequence);
      return;
    }

    checkState(safeToDeleteIds != null, "Cannot finalize an already finalized checkpoint");
    // Even if the 'true' active reader has changed since the checkpoint was taken we are
    // fine:
    // - The underlying SQS topic will not have changed, so the following deletes will still
    // go to the right place.
    // - We'll delete the ACK ids from the readers in-flight state, but that only affect
    // flow control and stats, neither of which are relevant anymore.
    SqsUnboundedReader liveReader = reader.get();
    try {
      liveReader.delete(safeToDeleteIds);
    } finally {
      clearAndDecrement(liveReader);
      liveReader.maybeCloseClient();
    }
  }

  private void clearAndDecrement() {
    SqsUnboundedReader liveReader =
        pendingCheckpoints != null
            ? pendingCheckpoints.reader
            : (reader != null && reader.isPresent() ? reader.get() : null);
    if (liveReader != null) {
      clearAndDecrement(liveReader);
    } else {
      reader = Optional.empty();
      safeToDeleteIds = null;
    }
  }

  private synchronized void clearAndDecrement(SqsUnboundedReader liveReader) {
    if (safeToDeleteIds == null && (reader == null || !reader.isPresent())) {
      return;
    }
    int remainingInFlight = liveReader.numInFlightCheckpoints.decrementAndGet();
    checkState(remainingInFlight >= 0, "Miscounted in-flight checkpoints");
    reader = Optional.empty();
    safeToDeleteIds = null;
  }

  static final class PendingCheckpoints {
    private static final Cache<String, PendingCheckpoints> LIVE =
        CacheBuilder.newBuilder().weakValues().build();

    final String readerId = UUID.randomUUID().toString();
    final SqsUnboundedReader reader;
    private final List<SqsCheckpointMark> checkpoints = new ArrayList<>();
    private long nextSequence;

    private PendingCheckpoints(SqsUnboundedReader reader) {
      this.reader = reader;
    }

    static PendingCheckpoints create(SqsUnboundedReader reader) {
      PendingCheckpoints pending = new PendingCheckpoints(reader);
      LIVE.put(pending.readerId, pending);
      return pending;
    }

    synchronized long nextSequence() {
      return nextSequence++;
    }

    synchronized void add(SqsCheckpointMark checkpoint) {
      checkpoints.add(checkpoint);
    }

    synchronized List<SqsCheckpointMark> removeUpTo(long sequence) {
      List<SqsCheckpointMark> removed = new ArrayList<>();
      checkpoints.removeIf(
          checkpoint -> {
            if (checkpoint.sequence <= sequence) {
              removed.add(checkpoint);
              return true;
            }
            return false;
          });
      return removed;
    }

    synchronized List<SqsCheckpointMark> removeAll() {
      LIVE.invalidate(readerId);
      List<SqsCheckpointMark> removed = new ArrayList<>(checkpoints);
      checkpoints.clear();
      return removed;
    }

    synchronized int size() {
      return checkpoints.size();
    }
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SqsCheckpointMark)) {
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
