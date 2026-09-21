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
package org.apache.beam.sdk.io.iceberg.cdc.sink;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.checkerframework.checker.nullness.qual.Nullable;

/** The committer's metrics, all namespaced under {@link CommitDeltas}. */
final class CommitterMetrics implements Serializable {

  final Counter snapshotsCreated = Metrics.counter(CommitDeltas.class, "snapshotsCreated");
  final Counter committedDataFiles = Metrics.counter(CommitDeltas.class, "committedDataFiles");
  final Counter committedDeleteFiles = Metrics.counter(CommitDeltas.class, "committedDeleteFiles");
  final Counter committedRecords = Metrics.counter(CommitDeltas.class, "committedRecords");
  final Counter committedEqualityDeleteRecords =
      Metrics.counter(CommitDeltas.class, "committedEqualityDeleteRecords");
  final Counter committedBytes = Metrics.counter(CommitDeltas.class, "committedBytes");
  final Distribution commitDurationMs =
      Metrics.distribution(CommitDeltas.class, "commitDurationMs");
  final Counter commitFailures = Metrics.counter(CommitDeltas.class, "commitFailures");

  final Counter alreadyCommittedWindowsSkipped =
      Metrics.counter(CommitDeltas.class, "alreadyCommittedWindowsSkipped");
  final Counter orphanFiles = Metrics.counter(CommitDeltas.class, "orphanFiles");
  final Counter tokenParseFailures = Metrics.counter(CommitDeltas.class, "tokenParseFailures");
  final Counter suspectedTokenExpiry = Metrics.counter(CommitDeltas.class, "suspectedTokenExpiry");
  final Counter crossWindowSequenceInversions =
      Metrics.counter(CommitDeltas.class, "crossWindowSequenceInversions");
  final Counter specMismatchedWindows =
      Metrics.counter(CommitDeltas.class, "specMismatchedWindows");

  final Counter heartbeatCommits = Metrics.counter(CommitDeltas.class, "heartbeatCommits");

  /** Records a successful commit's volume metrics. */
  void recordCommit(CommitSummary summary) {
    committedDataFiles.inc(summary.dataFileCount);
    committedDeleteFiles.inc(summary.deleteFileCount);
    committedRecords.inc(summary.dataRecords);
    committedEqualityDeleteRecords.inc(summary.equalityDeleteRecords);
    committedBytes.inc(summary.bytes);
    snapshotsCreated.inc();
  }

  /** One commit's volume and partition-spec summary, computed from its files. */
  static final class CommitSummary {
    final long dataFileCount;
    final long deleteFileCount;
    final long dataRecords;
    final long equalityDeleteRecords;
    final long bytes;
    final @Nullable Integer firstSpecId;
    final Set<Integer> specIds;
    final boolean hasEqualityDeletes;

    private CommitSummary(
        long dataFileCount,
        long deleteFileCount,
        long dataRecords,
        long equalityDeleteRecords,
        long bytes,
        @Nullable Integer firstSpecId,
        Set<Integer> specIds,
        boolean hasEqualityDeletes) {
      this.dataFileCount = dataFileCount;
      this.deleteFileCount = deleteFileCount;
      this.dataRecords = dataRecords;
      this.equalityDeleteRecords = equalityDeleteRecords;
      this.bytes = bytes;
      this.firstSpecId = firstSpecId;
      this.specIds = specIds;
      this.hasEqualityDeletes = hasEqualityDeletes;
    }

    static CommitSummary of(List<DataFile> dataFiles, List<DeleteFile> deleteFiles) {
      long dataRecords = 0;
      long bytes = 0;
      @Nullable Integer firstSpecId = null;
      Set<Integer> specIds = new HashSet<>();
      for (DataFile f : dataFiles) {
        dataRecords += f.recordCount();
        bytes += f.fileSizeInBytes();
        if (firstSpecId == null) {
          firstSpecId = f.specId();
        }
        specIds.add(f.specId());
      }
      long equalityDeleteRecords = 0;
      boolean hasEqualityDeletes = false;
      for (DeleteFile f : deleteFiles) {
        bytes += f.fileSizeInBytes();
        if (firstSpecId == null) {
          firstSpecId = f.specId();
        }
        specIds.add(f.specId());
        if (f.content() == FileContent.EQUALITY_DELETES) {
          equalityDeleteRecords += f.recordCount();
          hasEqualityDeletes = true;
        }
      }
      return new CommitSummary(
          dataFiles.size(),
          deleteFiles.size(),
          dataRecords,
          equalityDeleteRecords,
          bytes,
          firstSpecId,
          specIds,
          hasEqualityDeletes);
    }
  }
}
