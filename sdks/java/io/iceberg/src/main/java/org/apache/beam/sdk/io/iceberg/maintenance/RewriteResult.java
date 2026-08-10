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
package org.apache.beam.sdk.io.iceberg.maintenance;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.apache.beam.sdk.transforms.Combine;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link RewriteDataFiles} run outputs a single {@link RewriteResult}, which represents a
 * structured summary of the run. An empty run still produces a result, albeit an empty one.
 *
 * <p>Multiple stages in the run can produce a {@link RewriteResult} (the planning stage,
 * filed-parent count, the commit stage). Each stage only sets the fields relevant to it. {@link
 * Merge} sums them into the one final row. All counts therefore default to 0 and the two id fields
 * are nullable.
 *
 * <p>Rewrite failures are tolerated and reported here. {@link #getFailedRewriteParents()} counts
 * the distinct planned parent groups that failed to rewrite (their input files stay live and are
 * retried on the next run).
 */
@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class RewriteResult implements Serializable {

  /** The rewrite operation's id (minted at planning time); null on an empty no-op run. */
  @SchemaFieldNumber("0")
  public abstract @Nullable String getOperationId();

  /** The snapshot planning ran against; null on an empty no-op run. */
  @SchemaFieldNumber("1")
  public abstract @Nullable Long getStartingSnapshotId();

  /** Parent groups KEPT for rewrite (after the {@code maxRewriteBytes} budget skip). */
  @SchemaFieldNumber("2")
  public abstract long getPlannedParentGroups();

  /** Data files selected for rewrite across all planned groups. */
  @SchemaFieldNumber("3")
  public abstract long getPlannedFiles();

  /** Input byte size of all planned groups. */
  @SchemaFieldNumber("4")
  public abstract long getPlannedBytes();

  /**
   * DISTINCT parent groups with at least one subgroup that failed to rewrite. A partial-progress
   * run tolerates any number of these and still succeeds.
   */
  @SchemaFieldNumber("5")
  public abstract long getFailedRewriteParents();

  /** Snapshots committed (one per successful commit batch, including idempotent re-emits). */
  @SchemaFieldNumber("6")
  public abstract long getCommittedSnapshots();

  /** Commit batches that failed terminally under partial progress. */
  @SchemaFieldNumber("7")
  public abstract long getFailedCommits();

  /** Compacted data files added by the committed snapshots. */
  @SchemaFieldNumber("8")
  public abstract long getFilesAdded();

  /** Input data files removed by the committed snapshots. */
  @SchemaFieldNumber("9")
  public abstract long getFilesRemoved();

  /** Input byte size of the groups that were committed. */
  @SchemaFieldNumber("10")
  public abstract long getRewrittenBytes();

  /** A builder with every count pre-set to 0 and the id fields left null. */
  public static Builder builder() {
    return new AutoValue_RewriteResult.Builder()
        .setPlannedParentGroups(0L)
        .setPlannedFiles(0L)
        .setPlannedBytes(0L)
        .setFailedRewriteParents(0L)
        .setCommittedSnapshots(0L)
        .setFailedCommits(0L)
        .setFilesAdded(0L)
        .setFilesRemoved(0L)
        .setRewrittenBytes(0L);
  }

  /** The all-zeros, null-id identity (also the empty-run result). */
  public static RewriteResult zeros() {
    return builder().build();
  }

  private static @Nullable String firstNonNull(@Nullable String a, @Nullable String b) {
    return a != null ? a : b;
  }

  private static @Nullable Long firstNonNull(@Nullable Long a, @Nullable Long b) {
    return a != null ? a : b;
  }

  /** Field-wise sum of two fragments: numeric fields add, id fields take the first non-null. */
  static RewriteResult merge(RewriteResult a, RewriteResult b) {
    return builder()
        .setOperationId(firstNonNull(a.getOperationId(), b.getOperationId()))
        .setStartingSnapshotId(firstNonNull(a.getStartingSnapshotId(), b.getStartingSnapshotId()))
        .setPlannedParentGroups(a.getPlannedParentGroups() + b.getPlannedParentGroups())
        .setPlannedFiles(a.getPlannedFiles() + b.getPlannedFiles())
        .setPlannedBytes(a.getPlannedBytes() + b.getPlannedBytes())
        .setFailedRewriteParents(a.getFailedRewriteParents() + b.getFailedRewriteParents())
        .setCommittedSnapshots(a.getCommittedSnapshots() + b.getCommittedSnapshots())
        .setFailedCommits(a.getFailedCommits() + b.getFailedCommits())
        .setFilesAdded(a.getFilesAdded() + b.getFilesAdded())
        .setFilesRemoved(a.getFilesRemoved() + b.getFilesRemoved())
        .setRewrittenBytes(a.getRewrittenBytes() + b.getRewrittenBytes())
        .build();
  }

  /**
   * Sums the per-stage fragments into the one final result. The identity is {@link #zeros()}, so
   * {@code Combine.globally} in the bounded global window emits one zeros row even on EMPTY input —
   * that is what makes an empty-table run still produce a result row.
   */
  public static class Merge extends Combine.CombineFn<RewriteResult, RewriteResult, RewriteResult> {
    @Override
    public RewriteResult createAccumulator() {
      return zeros();
    }

    @Override
    public RewriteResult addInput(RewriteResult accumulator, RewriteResult input) {
      return merge(accumulator, input);
    }

    @Override
    public RewriteResult mergeAccumulators(Iterable<RewriteResult> accumulators) {
      RewriteResult merged = zeros();
      for (RewriteResult accumulator : accumulators) {
        merged = merge(merged, accumulator);
      }
      return merged;
    }

    @Override
    public RewriteResult extractOutput(RewriteResult accumulator) {
      return accumulator;
    }

    @Override
    public Coder<RewriteResult> getAccumulatorCoder(
        CoderRegistry registry, Coder<RewriteResult> inputCoder) {
      return inputCoder;
    }
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setOperationId(@Nullable String v);

    public abstract Builder setStartingSnapshotId(@Nullable Long v);

    public abstract Builder setPlannedParentGroups(long v);

    public abstract Builder setPlannedFiles(long v);

    public abstract Builder setPlannedBytes(long v);

    public abstract Builder setFailedRewriteParents(long v);

    public abstract Builder setCommittedSnapshots(long v);

    public abstract Builder setFailedCommits(long v);

    public abstract Builder setFilesAdded(long v);

    public abstract Builder setFilesRemoved(long v);

    public abstract Builder setRewrittenBytes(long v);

    public abstract RewriteResult build();
  }
}
