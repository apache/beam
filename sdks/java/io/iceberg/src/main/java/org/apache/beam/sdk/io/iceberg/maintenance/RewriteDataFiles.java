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

import static org.apache.iceberg.actions.RewriteDataFiles.OUTPUT_SPEC_ID;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.SnapshotInfo;
import org.apache.beam.sdk.io.iceberg.TableCache;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BinPackRewriteFilePlanner;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bin-pack compaction for an Iceberg table. Rewrites small data files into fewer, target-sized
 * files. Planning runs once on a single worker and creates large batches (parents). Each parent
 * batch is split into subgroups and spread across writer DoFns. Subgroups are then grouped back
 * into their original batches. Partial progress will do parallel commits (based on {@code
 * maxCommits}) and atomic mode will put everything into a single commit.
 *
 * <h2>Failure semantics
 *
 * <p>The default <b>atomic mode</b> is all-or-nothing. If any rewrite subgroup fails, all subgroup
 * output files will be deleted (regardless if successful or not) and the pipeline fails. This
 * deletion is safe because those files were never handed to a commit.
 *
 * <p>If rewrites succeed but the single <i>commit</i> fails, its output files are <b>not</b>
 * deleted: a stray retry or concurrent attempt of that same commit could still succeed. Deleting
 * these files could risk creating a snapshot that references missing data. Instead, the pipeline
 * fails and each output file is left tagged with this operation's id. A later remove-orphan-files
 * run can reclaim it. The rewrite's <i>input</i> files are never deleted on any failure, so no data
 * is lost.
 *
 * <p><b>Partial progress</b> mode bounds the blast radius of failures. Rewrite-group failures are
 * always tolerated and reported in the {@link RewriteResult}. Terminal commit failures are bounded
 * by {@code maxFailedCommits}. Any individual commit-budget failure does not roll back the
 * partial-progress commits that already succeeded. The pipeline will still fail but compaction did
 * make progress.
 */
public class RewriteDataFiles extends PTransform<PCollection<SnapshotInfo>, PCollectionTuple> {
  private static final Logger LOG = LoggerFactory.getLogger(RewriteDataFiles.class);
  public static final String REWRITE_PREFIX =
      IcebergMaintenance.MAINTENANCE_PREFIX + "[RewriteDataFiles] ";

  /** The snapshots this rewrite committed (one per successful commit batch). */
  public static final TupleTag<SnapshotInfo> SNAPSHOTS = new TupleTag<>() {};

  /**
   * The single structured {@link RewriteResult} summary of the run (always exactly one element).
   */
  public static final TupleTag<RewriteResult> RESULT = new TupleTag<>() {};

  private final String tableIdentifier;
  private final SerializableTable table;
  private final IcebergCatalogConfig catalogConfig;
  private final Configuration rewriteConfig;

  RewriteDataFiles(
      String tableIdentifier,
      SerializableTable table,
      IcebergCatalogConfig catalogConfig,
      Configuration rewriteConfig) {
    this.tableIdentifier = tableIdentifier;
    this.table = table;
    this.catalogConfig = catalogConfig;
    this.rewriteConfig = rewriteConfig;
  }

  static RewriteDataFiles create(
      String tableIdentifier,
      SerializableTable table,
      IcebergCatalogConfig catalogConfig,
      @Nullable Configuration rewriteConfig) {
    return new RewriteDataFiles(
        tableIdentifier,
        table,
        catalogConfig,
        rewriteConfig != null ? rewriteConfig : Configuration.builder().build());
  }

  @Override
  public PCollectionTuple expand(PCollection<SnapshotInfo> impulse) {
    rewriteConfig.validate();
    rewriteConfig.validateRewriteOptions(table);
    Preconditions.checkArgument(
        impulse.isBounded() == PCollection.IsBounded.BOUNDED,
        "RewriteDataFiles only supports bounded (batch) input. Streaming/continuous "
            + "maintenance is not yet supported.");
    LOG.info(REWRITE_PREFIX + "Running {} task.", RewriteDataFiles.class.getSimpleName());

    // Plan the rewrite once (single worker), fan out the rewrite work, then commit each batch
    PCollectionTuple planned =
        impulse.apply("Plan Rewrite Groups", new PlanRewriteGroups(table, rewriteConfig));
    PCollectionTuple rewritten =
        planned
            .get(PlanRewriteGroups.GROUPS)
            .apply("Redistribute Groups", Redistribute.arbitrarily())
            .setCoder(KvCoder.of(VarIntCoder.of(), SUB_GROUP_CODER))
            .apply(
                "Rewrite SubGroups",
                ParDo.of(new RewriteSubGroupDoFn(table, rewriteConfig.writeProperties()))
                    .withOutputTags(
                        RewriteSubGroupDoFn.REWRITTEN,
                        TupleTagList.of(RewriteSubGroupDoFn.FAILED_PARENTS)));
    PCollection<KV<Integer, ExecutedGroup>> executed =
        rewritten
            .get(RewriteSubGroupDoFn.REWRITTEN)
            .setCoder(KvCoder.of(VarIntCoder.of(), EXECUTED_GROUP_CODER));

    PCollection<KV<Integer, Iterable<ExecutedGroup>>> grouped =
        executed.apply("Group by Commit Key", GroupByKey.create());

    // Distinct failed-parent count
    PCollection<Long> failedParents = countFailedParents(rewritten);

    // Batches to commit. In atomic mode (all or nothing), if any group failed to be rewritten, all
    // groups get passed through a gate that aborts the whole rewrite.
    // In partial-progress mode, batches are committed independently.
    PCollection<KV<Integer, Iterable<ExecutedGroup>>> toCommit;
    if (rewriteConfig.partialProgressEnabled()) {
      toCommit = grouped;
    } else {
      PCollectionView<Long> rewriteFailureCountView =
          failedParents.apply("Rewrite Failure Count View", View.asSingleton());
      PCollectionTuple gated =
          grouped.apply(
              "Atomic Commit Gate",
              ParDo.of(new AtomicCommitGate(rewriteFailureCountView))
                  .withSideInputs(rewriteFailureCountView)
                  .withOutputTags(
                      AtomicCommitGate.PASS, TupleTagList.of(AtomicCommitGate.CLEANUP)));

      // Delete the successful-but-uncommittable outputs of an aborted atomic rewrite, then fail.
      gated
          .get(AtomicCommitGate.CLEANUP)
          .setCoder(KvCoder.of(VarIntCoder.of(), ListCoder.of(StringUtf8Coder.of())))
          .apply(
              "Clean Up Aborted Atomic Rewrite",
              ParDo.of(new CleanupAndFail(tableIdentifier, catalogConfig)));

      // if EVERY group failed there is no batch for the gate to abort, so fail here.
      executed
          .apply("Count Successful Rewrites", Count.globally())
          .apply(
              "Assert Atomic Rewrite Progressed",
              ParDo.of(new AssertAtomicRewriteProgressed(rewriteFailureCountView))
                  .withSideInputs(rewriteFailureCountView));
      toCommit =
          gated
              .get(AtomicCommitGate.PASS)
              .setCoder(KvCoder.of(VarIntCoder.of(), IterableCoder.of(EXECUTED_GROUP_CODER)));
    }

    // Commit each batch in parallel
    // A terminal commit failure in atomic mode fails the pipeline directly from CommitRewriteGroups
    // without deleting its output files. A bundle retry could still commit them.
    // Worst case, they are left as tagged orphans.
    PCollectionTuple committed =
        toCommit.apply(
            "Commit Rewrites",
            ParDo.of(new CommitRewriteGroups(tableIdentifier, catalogConfig, rewriteConfig))
                .withOutputTags(
                    CommitRewriteGroups.COMMITTED,
                    TupleTagList.of(CommitRewriteGroups.FAILED_COMMITS)
                        .and(CommitRewriteGroups.COMMIT_SUMMARY)));
    committed.get(CommitRewriteGroups.COMMIT_SUMMARY).setCoder(RESULT_CODER);

    // Partial-progress mode bounds the commit stage: a terminal commit failure is tolerated
    // but charged to maxFailedCommits. Rewrite-group failures are reported in the RewriteResult.
    if (rewriteConfig.partialProgressEnabled()) {
      committed
          .get(CommitRewriteGroups.FAILED_COMMITS)
          .setCoder(VarIntCoder.of())
          .apply("Count Commit Failures", Count.globally())
          .apply(
              "Assert Commit Failures Within Limit",
              ParDo.of(new AssertMaxFailures("commit", rewriteConfig.maxFailedCommits())));
    }

    // Assemble the single RewriteResult from the per-stage fragments:
    // - the planning summary
    // - the failed-parent count
    // - each commit's summary
    PCollection<SnapshotInfo> committedSnapshots =
        committed.get(CommitRewriteGroups.COMMITTED).setCoder(SNAPSHOT_CODER);
    PCollection<RewriteResult> failedParentsFragment =
        failedParents
            .apply(
                "Failed-Parent Fragment",
                MapElements.into(TypeDescriptor.of(RewriteResult.class))
                    .via(
                        (Long count) ->
                            RewriteResult.builder()
                                .setFailedRewriteParents(count == null ? 0L : count)
                                .build()))
            .setCoder(RESULT_CODER);
    PCollection<RewriteResult> result =
        PCollectionList.of(planned.get(PlanRewriteGroups.PLAN_SUMMARY))
            .and(failedParentsFragment)
            .and(committed.get(CommitRewriteGroups.COMMIT_SUMMARY))
            .apply("Flatten Rewrite Fragments", Flatten.pCollections())
            .apply("Merge into one Rewrite Result", Combine.globally(new RewriteResult.Merge()));

    return PCollectionTuple.of(SNAPSHOTS, committedSnapshots).and(RESULT, result);
  }

  /** Dedupes REWRITE_FAILURES and counts the failed PARENT groups. */
  private static PCollection<Long> countFailedParents(PCollectionTuple rewritten) {
    return rewritten
        .get(RewriteSubGroupDoFn.FAILED_PARENTS)
        .setCoder(VarIntCoder.of())
        .apply("Distinct Failed Parents", Distinct.create())
        .apply("Count Rewrite Failures", Count.globally());
  }

  /**
   * Fails the pipeline when the partial-progress COMMIT-failure count exceeds {@code
   * maxFailedCommits}.
   */
  static class AssertMaxFailures extends DoFn<Long, Void> {
    private final String unit;
    private final int max;

    AssertMaxFailures(String unit, int max) {
      this.unit = unit;
      this.max = max;
    }

    @ProcessElement
    public void process(@Element Long failedCount) {
      if (failedCount != null && failedCount > max) {
        throw new IllegalStateException(
            String.format(
                "%d %s failure(s) exceeded the maximum allowed of %d. Check worker logs for the "
                    + "underlying errors; raise maxFailedCommits, or increase maxCommits to spread "
                    + "the work across more parallel batches.",
                failedCount, unit, max));
      }
    }
  }

  /**
   * Deletes {@code paths}, using {@link SupportsBulkOperations#deleteFiles} in one call when the
   * {@link FileIO} supports it and falling back to per-file deletes otherwise. Returns the number
   * deleted. Best-effort: failures are logged, not thrown, and the leftovers stay
   * operation-id-tagged for a later remove-orphan-files run.
   */
  static int deleteOrphans(FileIO io, List<String> paths) {
    if (paths.isEmpty()) {
      return 0;
    }
    if (io instanceof SupportsBulkOperations) {
      try {
        ((SupportsBulkOperations) io).deleteFiles(paths);
        return paths.size();
      } catch (BulkDeletionFailureException e) {
        int failed = e.numberFailedObjects();
        LOG.warn(
            REWRITE_PREFIX + "Bulk delete failed for {} of {} orphan rewrite output file(s).",
            failed,
            paths.size(),
            e);
        return Math.max(0, paths.size() - failed);
      } catch (RuntimeException e) {
        // A non-Bulk failure. Fall through to the per-file loop
        LOG.warn(
            REWRITE_PREFIX
                + "Bulk delete raised a non-Bulk error; falling back to per-file deletes.",
            e);
      }
    }
    int deleted = 0;
    for (String path : paths) {
      try {
        io.deleteFile(path);
        deleted++;
      } catch (Exception e) {
        LOG.warn(REWRITE_PREFIX + "Failed to delete orphan rewrite output {}", path, e);
      }
    }
    return deleted;
  }

  /**
   * Cleanup stage for an atomic rewrite aborted because a file group failed to <b>rewrite</b>.
   *
   * <p>When any subgroup in an atomic rewrite fails, the {@link AtomicCommitGate} routes the whole
   * batch's sibling output files here. The files are routed here before the commit stage, so they
   * are never handed to a commit and are safe to delete. After deleting, this stage fails the
   * pipeline.
   */
  static class CleanupAndFail extends DoFn<KV<Integer, List<String>>, Void> {
    private final String tableIdentifier;
    private final IcebergCatalogConfig catalogConfig;

    CleanupAndFail(String tableIdentifier, IcebergCatalogConfig catalogConfig) {
      this.tableIdentifier = tableIdentifier;
      this.catalogConfig = catalogConfig;
    }

    @ProcessElement
    public void process(@Element KV<Integer, List<String>> element) {
      int commitKey = element.getKey();
      List<String> orphanPaths = element.getValue();
      Table table =
          TableCache.getAndRefreshIfStale(catalogConfig, TableIdentifier.parse(tableIdentifier));
      int deleted = deleteOrphans(table.io(), orphanPaths);
      LOG.info(
          REWRITE_PREFIX
              + "Atomic rewrite aborted (rewrite-group failure) for key {}; deleted {}/{} orphan "
              + "output file(s) before failing the pipeline.",
          commitKey,
          deleted,
          orphanPaths.size());

      throw new RuntimeException(
          String.format(
              "RewriteDataFiles aborted: one or more file groups in commit batch %s could not be "
                  + "rewritten. The commit was not attempted. %s/%s orphan output file(s) have been "
                  + "cleaned up. Consider enabling partial progress to tolerate individual group "
                  + "failures.",
              commitKey, deleted, orphanPaths.size()));
    }
  }

  /**
   * Atomic-mode gate between grouping and commit.
   *
   * <p>If ANY group failed to rewrite this routes the whole batch's successful output files to
   * {@link #CLEANUP} (delete, then fail) rather than committing a partial rewrite and leaking the
   * siblings as orphans.
   *
   * <p>If ALL groups succeed, it passes the batch to {@link #PASS} for the normal commit.
   */
  static class AtomicCommitGate
      extends DoFn<KV<Integer, Iterable<ExecutedGroup>>, KV<Integer, Iterable<ExecutedGroup>>> {
    static final TupleTag<KV<Integer, Iterable<ExecutedGroup>>> PASS = new TupleTag<>() {};
    static final TupleTag<KV<Integer, List<String>>> CLEANUP = new TupleTag<>() {};

    private final PCollectionView<Long> rewriteFailureCountView;

    AtomicCommitGate(PCollectionView<Long> rewriteFailureCountView) {
      this.rewriteFailureCountView = rewriteFailureCountView;
    }

    @ProcessElement
    public void process(
        @Element KV<Integer, Iterable<ExecutedGroup>> batch,
        MultiOutputReceiver out,
        ProcessContext c) {
      long rewriteFailures = c.sideInput(rewriteFailureCountView);
      if (rewriteFailures <= 0) {
        out.get(PASS).output(batch);
        return;
      }
      // A sibling group failed, so this atomic rewrite cannot commit. Collect this batch's
      // successful output files so they are deleted (not leaked), then fail downstream.
      List<String> orphanPaths = ExecutedGroup.newFilePaths(batch.getValue());
      LOG.info(
          REWRITE_PREFIX
              + "Atomic rewrite aborted: {} file group(s) failed to rewrite; routing {} successful "
              + "output file(s) from commit key {} to cleanup.",
          rewriteFailures,
          orphanPaths.size(),
          batch.getKey());
      out.get(CLEANUP).output(KV.of(batch.getKey(), orphanPaths));
    }
  }

  /**
   * Atomic-mode safety net for the case where EVERY file group failed to rewrite, so no commit
   * batch reaches {@link AtomicCommitGate} to clean up or fail. It is gated on {@code successCount
   * == 0} so it doesn't race with the gate's cleanup in the partial-failure case (where some groups
   * succeeded and the gate handles the abort).
   */
  static class AssertAtomicRewriteProgressed extends DoFn<Long, Void> {
    private final PCollectionView<Long> rewriteFailureCountView;

    AssertAtomicRewriteProgressed(PCollectionView<Long> rewriteFailureCountView) {
      this.rewriteFailureCountView = rewriteFailureCountView;
    }

    @ProcessElement
    public void process(ProcessContext c) {
      long successes = c.element();
      long failures = c.sideInput(rewriteFailureCountView);
      if (failures > 0 && successes == 0) {
        throw new IllegalStateException(
            String.format(
                "Atomic rewrite failed: all %d planned file group(s) failed to rewrite and nothing "
                    + "was committed. Check worker logs for the underlying errors.",
                failures));
      }
    }
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class Configuration implements Serializable {
    public static Builder builder() {
      return new AutoValue_RewriteDataFiles_Configuration.Builder();
    }

    @SchemaFieldDescription(
        "A snapshot ID used for planning and as the starting snapshot id for commit validation when replacing the files")
    @Pure
    public abstract @Nullable Long getSnapshotId();

    @SchemaFieldDescription(
        "Whether the filter's column names must match the schema's case exactly when the planning "
            + "scan binds them; default is false. Most predicates are already resolved "
            + "case-insensitively when the filter string is parsed (FilterUtils canonicalizes the "
            + "names), so this rarely changes behavior.")
    @Pure
    public abstract @Nullable Boolean getCaseSensitive();

    @Pure
    public abstract @Nullable String getFilter();

    @Pure
    public abstract @Nullable Map<String, String> getRewriteOptions();

    @SchemaFieldDescription(
        "Iceberg write properties (e.g. write.parquet.compression-codec) that override the table's "
            + "own write properties for the compacted output files; default is none (inherit the "
            + "table's properties).")
    @Pure
    public abstract @Nullable Map<String, String> getWriteProperties();

    @SchemaFieldDescription(
        "Branch to compact and commit to (write-audit-publish). When set, planning reads the "
            + "branch's head snapshot and the rewrite commits to that branch; MAIN is untouched. "
            + "An explicit snapshotId still wins. Default is null (compact the table's main branch).")
    @Pure
    public abstract @Nullable String getBranch();

    @SchemaFieldDescription(
        "Extra snapshot-summary properties to stamp on each rewrite commit (org attribution etc.). "
            + "Keys starting with 'beam.rewrite.' are reserved for idempotency stamps and rejected. "
            + "Default is none.")
    @Pure
    public abstract @Nullable Map<String, String> getSnapshotProperties();

    @SchemaFieldDescription(
        "Enable committing groups of files independently (partial progress); default is false")
    @Pure
    public abstract @Nullable Boolean getPartialProgressEnabled();

    @SchemaFieldDescription(
        "Maximum number of commits when partial progress is enabled; default is 10")
    @Pure
    public abstract @Nullable Integer getMaxCommits();

    @SchemaFieldDescription(
        "Maximum number of failed commits allowed before the operation fails; default matches maxCommits")
    @Pure
    public abstract @Nullable Integer getMaxFailedCommits();

    @SchemaFieldDescription(
        "Use the starting sequence number when committing rewritten files; default is true")
    @Pure
    public abstract @Nullable Boolean getUseStartingSequenceNumber();

    @SchemaFieldDescription(
        "Maximum number of bytes to rewrite in a single operation; default is Long.MAX_VALUE")
    @Pure
    public abstract @Nullable Long getMaxRewriteBytes();

    // Resolved-accessor helpers — non-abstract so AutoValue does not treat them as schema fields.

    public boolean partialProgressEnabled() {
      return MoreObjects.firstNonNull(getPartialProgressEnabled(), false);
    }

    public int maxCommits() {
      return MoreObjects.firstNonNull(getMaxCommits(), 10);
    }

    public int maxFailedCommits() {
      return MoreObjects.firstNonNull(getMaxFailedCommits(), maxCommits());
    }

    public boolean useStartingSequenceNumber() {
      return MoreObjects.firstNonNull(getUseStartingSequenceNumber(), true);
    }

    public long maxRewriteBytes() {
      return MoreObjects.firstNonNull(getMaxRewriteBytes(), Long.MAX_VALUE);
    }

    public Map<String, String> writeProperties() {
      return MoreObjects.firstNonNull(getWriteProperties(), Collections.emptyMap());
    }

    public Map<String, String> snapshotProperties() {
      return MoreObjects.firstNonNull(getSnapshotProperties(), Collections.emptyMap());
    }

    public boolean caseSensitive() {
      return MoreObjects.firstNonNull(getCaseSensitive(), false);
    }

    public void validate() {
      Preconditions.checkArgument(
          getBranch() == null || getSnapshotId() == null,
          "Set only one of branch or snapshotId: an explicit snapshot pin on a branch run would "
              + "validate the commit against the wrong (main) ancestry.");
      Preconditions.checkArgument(
          !partialProgressEnabled() || maxCommits() > 0,
          "partial-progress.max-commits must be positive when partial progress is enabled");
      Preconditions.checkArgument(maxFailedCommits() >= 0, "maxFailedCommits must be non-negative");
      Preconditions.checkArgument(maxRewriteBytes() > 0, "maxRewriteBytes must be positive");
      for (String key : snapshotProperties().keySet()) {
        Preconditions.checkArgument(
            !key.startsWith("beam.rewrite."),
            "snapshotProperties key '%s' is reserved: keys starting with 'beam.rewrite.' collide "
                + "with the idempotency stamps written on each rewrite commit.",
            key);
      }
    }

    /**
     * Validates {@code rewriteOptions} against the native bin-pack planner's options (plus {@code
     * output-spec-id}) and rejects unknown keys and invalid values up front.
     *
     * <p><b>Intentional gap:</b> Iceberg's <em>action-level</em> options are NOT accepted here. In
     * particular {@code remove-dangling-deletes} is unsupported: this rewrite already drops the
     * file-scoped deletion vectors tied to the data files it rewrites, but it does not perform
     * Iceberg's broader table-wide dangling-delete sweep. Run that as a separate maintenance action
     * if needed. Other action-level knobs (e.g. {@code max-concurrent-file-group-rewrites}) are
     * likewise superseded by this connector's parallelism model and the typed {@code Configuration}
     * fields.
     *
     * <p><b>Repartitioning cost:</b> setting {@code output-spec-id} to a spec that repartitions the
     * data (or compacting a table after a spec evolution) makes a subgroup's rows fan out across
     * output partitions — one open writer each. That fan-out is bounded ({@link WriterFactory}
     * throws past a cap rather than OOMing); prefer compacting with the table's current spec, where
     * each subgroup stays within a single partition.
     */
    void validateRewriteOptions(Table table) {
      @Nullable Map<String, String> options = getRewriteOptions();
      if (options == null || options.isEmpty()) {
        return;
      }
      BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table);
      // (1) Reject unknown KEYS. Iceberg's planner consumes output-spec-id but omits it from
      // validOptions(), so allow it explicitly. (max-file-group-input-files is another such option
      // in newer Iceberg, but it does not exist in 1.10. Add to validOptions when the dependency is
      // upgraded.) Point action-level options that are handled elsewhere at their typed field.
      Set<String> validOptions = new HashSet<>(planner.validOptions());
      validOptions.add(OUTPUT_SPEC_ID);
      Set<String> unknown = new HashSet<>(options.keySet());
      unknown.removeAll(validOptions);
      if (!unknown.isEmpty()) {
        StringBuilder message =
            new StringBuilder(
                String.format(
                    "Unsupported rewrite option(s): %s. Supported planner options are %s.",
                    unknown, validOptions));
        for (String key : unknown) {
          @Nullable String hint = ACTION_OPTION_HINTS.get(key);
          if (hint != null) {
            message.append(
                String.format(" ('%s' is an Iceberg action-level option: %s.)", key, hint));
          }
        }
        throw new IllegalArgumentException(message.toString());
      }
      // (2) Check output-spec-id existence with a friendly, spec-ids-listing message. 1.10's
      // planner
      // also validates this inside init() (SizeBasedFileRewritePlanner.outputSpecId), but tersely —
      // so check it FIRST here so the user sees the known-spec-ids hint.
      String outputSpecId = options.get(OUTPUT_SPEC_ID);
      if (outputSpecId != null && !table.specs().containsKey(Integer.parseInt(outputSpecId))) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid rewrite option output-spec-id=%s: the table has no partition spec with that "
                    + "id. Known spec ids: %s",
                outputSpecId, table.specs().keySet()));
      }
      // (3) Reject invalid VALUES up front. planner.init() parses and validates the option values
      // (file sizes, rewrite-job-order name, numeric parsing) and throws IllegalArgumentException.
      planner.init(options);
    }

    // Iceberg action-level rewrite options that this connector handles via typed Configuration
    // fields (or deliberately does not support), mapped to the guidance shown when a user passes
    // one through rewriteOptions. max-file-group-size-bytes is a real planner option (not listed).
    private static final Map<String, String> ACTION_OPTION_HINTS =
        ImmutableMap.<String, String>builder()
            .put("partial-progress.enabled", "use Configuration.setPartialProgressEnabled(...)")
            .put("partial-progress.max-commits", "use Configuration.setMaxCommits(...)")
            .put(
                "use-starting-sequence-number",
                "use Configuration.setUseStartingSequenceNumber(...)")
            .put(
                "remove-dangling-deletes",
                "unsupported: this rewrite drops only the deletion vectors tied to the files it "
                    + "rewrites; run a separate dangling-delete sweep if needed")
            .put(
                "max-concurrent-file-group-rewrites",
                "not applicable: the Beam runner owns parallelism (tune worker count / "
                    + "--maxNumWorkers)")
            .build();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setSnapshotId(@Nullable Long snapshotId);

      public abstract Builder setCaseSensitive(@Nullable Boolean caseSensitive);

      public abstract Builder setFilter(@Nullable String filter);

      public abstract Builder setRewriteOptions(@Nullable Map<String, String> options);

      public abstract Builder setWriteProperties(@Nullable Map<String, String> writeProperties);

      public abstract Builder setBranch(@Nullable String branch);

      public abstract Builder setSnapshotProperties(
          @Nullable Map<String, String> snapshotProperties);

      public abstract Builder setPartialProgressEnabled(@Nullable Boolean partialProgressEnabled);

      public abstract Builder setMaxCommits(@Nullable Integer maxCommits);

      public abstract Builder setMaxFailedCommits(@Nullable Integer maxFailedCommits);

      public abstract Builder setUseStartingSequenceNumber(
          @Nullable Boolean useStartingSequenceNumber);

      public abstract Builder setMaxRewriteBytes(@Nullable Long maxRewriteBytes);

      public abstract Configuration build();
    }
  }

  static final SchemaCoder<RewriteSubGroup> SUB_GROUP_CODER;
  static final SchemaCoder<ExecutedGroup> EXECUTED_GROUP_CODER;
  static final SchemaCoder<SnapshotInfo> SNAPSHOT_CODER;
  static final SchemaCoder<RewriteResult> RESULT_CODER;

  static {
    try {
      SUB_GROUP_CODER = SchemaRegistry.createDefault().getSchemaCoder(RewriteSubGroup.class);
      EXECUTED_GROUP_CODER = SchemaRegistry.createDefault().getSchemaCoder(ExecutedGroup.class);
      SNAPSHOT_CODER = SchemaRegistry.createDefault().getSchemaCoder(SnapshotInfo.class);
      RESULT_CODER = SchemaRegistry.createDefault().getSchemaCoder(RewriteResult.class);
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }
  }
}
