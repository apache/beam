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
package org.apache.beam.sdk.io.gcp.spanner;

import static org.apache.beam.sdk.io.gcp.spanner.MutationUtils.isPointDelete;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.ServiceFactory;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.AbortedException;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.Op;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Stopwatch;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.UnsignedBytes;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Experimental {@link PTransform Transforms} for reading from and writing to <a
 * href="https://cloud.google.com/spanner">Google Cloud Spanner</a>.
 *
 * <h3>Reading from Cloud Spanner</h3>
 *
 * <p>To read from Cloud Spanner, apply {@link SpannerIO.Read} transformation. It will return a
 * {@link PCollection} of {@link Struct Structs}, where each element represents an individual row
 * returned from the read operation. Both Query and Read APIs are supported. See more information
 * about <a href="https://cloud.google.com/spanner/docs/reads">reading from Cloud Spanner</a>
 *
 * <p>To execute a <strong>query</strong>, specify a {@link SpannerIO.Read#withQuery(Statement)} or
 * {@link SpannerIO.Read#withQuery(String)} during the construction of the transform.
 *
 * <pre>{@code
 * PCollection<Struct> rows = p.apply(
 *     SpannerIO.read()
 *         .withInstanceId(instanceId)
 *         .withDatabaseId(dbId)
 *         .withQuery("SELECT id, name, email FROM users"));
 * }</pre>
 *
 * <p>To use the Read API, specify a {@link SpannerIO.Read#withTable(String) table name} and a
 * {@link SpannerIO.Read#withColumns(List) list of columns}.
 *
 * <pre>{@code
 * PCollection<Struct> rows = p.apply(
 *    SpannerIO.read()
 *        .withInstanceId(instanceId)
 *        .withDatabaseId(dbId)
 *        .withTable("users")
 *        .withColumns("id", "name", "email"));
 * }</pre>
 *
 * <p>To optimally read using index, specify the index name using {@link SpannerIO.Read#withIndex}.
 *
 * <p>The transform is guaranteed to be executed on a consistent snapshot of data, utilizing the
 * power of read only transactions. Staleness of data can be controlled using {@link
 * SpannerIO.Read#withTimestampBound} or {@link SpannerIO.Read#withTimestamp(Timestamp)} methods. <a
 * href="https://cloud.google.com/spanner/docs/transactions">Read more</a> about transactions in
 * Cloud Spanner.
 *
 * <p>It is possible to read several {@link PCollection PCollections} within a single transaction.
 * Apply {@link SpannerIO#createTransaction()} transform, that lazily creates a transaction. The
 * result of this transformation can be passed to read operation using {@link
 * SpannerIO.Read#withTransaction(PCollectionView)}.
 *
 * <pre>{@code
 * SpannerConfig spannerConfig = ...
 *
 * PCollectionView<Transaction> tx = p.apply(
 *    SpannerIO.createTransaction()
 *        .withSpannerConfig(spannerConfig)
 *        .withTimestampBound(TimestampBound.strong()));
 *
 * PCollection<Struct> users = p.apply(
 *    SpannerIO.read()
 *        .withSpannerConfig(spannerConfig)
 *        .withQuery("SELECT name, email FROM users")
 *        .withTransaction(tx));
 *
 * PCollection<Struct> tweets = p.apply(
 *    SpannerIO.read()
 *        .withSpannerConfig(spannerConfig)
 *        .withQuery("SELECT user, tweet, date FROM tweets")
 *        .withTransaction(tx));
 * }</pre>
 *
 * <h3>Writing to Cloud Spanner</h3>
 *
 * <p>The Cloud Spanner {@link SpannerIO.Write} transform writes to Cloud Spanner by executing a
 * collection of input row {@link Mutation Mutations}. The mutations are grouped into batches for
 * efficiency.
 *
 * <p>To configure the write transform, create an instance using {@link #write()} and then specify
 * the destination Cloud Spanner instance ({@link Write#withInstanceId(String)} and destination
 * database ({@link Write#withDatabaseId(String)}). For example:
 *
 * <pre>{@code
 * // Earlier in the pipeline, create a PCollection of Mutations to be written to Cloud Spanner.
 * PCollection<Mutation> mutations = ...;
 * // Write mutations.
 * SpannerWriteResult result = mutations.apply(
 *     "Write", SpannerIO.write().withInstanceId("instance").withDatabaseId("database"));
 * }</pre>
 *
 * <h3>SpannerWriteResult</h3>
 *
 * <p>The {@link SpannerWriteResult SpannerWriteResult} object contains the results of the
 * transform, including a {@link PCollection} of MutationGroups that failed to write, and a {@link
 * PCollection} that can be used in batch pipelines as a completion signal to {@link
 * org.apache.beam.sdk.transforms.Wait Wait.OnSignal} to indicate when all input has been written.
 * Note that in streaming pipelines, this signal will never be triggered as the input is unbounded
 * and this {@link PCollection} is using the {@link GlobalWindow}.
 *
 * <h3>Batching and Grouping</h3>
 *
 * <p>To reduce the number of transactions sent to Spanner, the {@link Mutation Mutations} are
 * grouped into batches. The default maximum size of the batch is set to 1MB or 5000 mutated cells,
 * or 500 rows (whichever is reached first). To override this use {@link
 * Write#withBatchSizeBytes(long) withBatchSizeBytes()}, {@link Write#withMaxNumMutations(long)
 * withMaxNumMutations()} or {@link Write#withMaxNumMutations(long) withMaxNumRows()}. Setting
 * either to a small value or zero disables batching.
 *
 * <p>Note that the <a
 * href="https://cloud.google.com/spanner/quotas#limits_for_creating_reading_updating_and_deleting_data">maximum
 * size of a single transaction</a> is 20,000 mutated cells - including cells in indexes. If you
 * have a large number of indexes and are getting exceptions with message: <tt>INVALID_ARGUMENT: The
 * transaction contains too many mutations</tt> you will need to specify a smaller number of {@code
 * MaxNumMutations}.
 *
 * <p>The batches written are obtained from by grouping enough {@link Mutation Mutations} from the
 * Bundle provided by Beam to form several batches. This group of {@link Mutation Mutations} is then
 * sorted by table and primary key, and the batches are created from the sorted group. Each batch
 * will then have rows for the same table, with keys that are 'close' to each other, thus optimising
 * write efficiency by each batch affecting as few table splits as possible performance.
 *
 * <p>This grouping factor (number of batches) is controlled by the parameter {@link
 * Write#withGroupingFactor(int) withGroupingFactor()}.
 *
 * <p>Note that each worker will need enough memory to hold {@code GroupingFactor x
 * MaxBatchSizeBytes} Mutations, so if you have a large {@code MaxBatchSize} you may need to reduce
 * {@code GroupingFactor}
 *
 * <p>While Grouping and Batching increases write efficiency, it dramatically increases the latency
 * between when a Mutation is received by the transform, and when it is actually written to the
 * database. This is because enough Mutations need to be received to fill the grouped batches. In
 * Batch pipelines (bounded sources), this is not normally an issue, but in Streaming (unbounded)
 * pipelines, this latency is often seen as unacceptable.
 *
 * <p>There are therefore 3 different ways that this transform can be configured:
 *
 * <ul>
 *   <li>With Grouping and Batching. <br>
 *       This is the default for Batch pipelines, where sorted batches of Mutations are created and
 *       written. This is the most efficient way to ingest large amounts of data, but the highest
 *       latency before writing
 *   <li>With Batching but no Grouping <br>
 *       If {@link Write#withGroupingFactor(int) .withGroupingFactor(1)}, is set, grouping is
 *       disabled. This is the default for Streaming pipelines. Unsorted batches are created and
 *       written as soon as enough mutations to fill a batch are received. This reflects a
 *       compromise where a small amount of additional latency enables more efficient writes
 *   <li>Without any Batching <br>
 *       If {@link Write#withBatchSizeBytes(long) .withBatchSizeBytes(0)} is set, no batching is
 *       performed and the Mutations are written to the database as soon as they are received.
 *       ensuring the lowest latency before Mutations are written.
 * </ul>
 *
 * <h3>Monitoring</h3>
 *
 * <p>Several counters are provided for monitoring purpooses:
 *
 * <ul>
 *   <li><tt>batchable_mutation_groups</tt><br>
 *       Counts the mutations that are batched for writing to Spanner.
 *   <li><tt>unbatchable_mutation_groups</tt><br>
 *       Counts the mutations that can not be batched and are applied individually - either because
 *       they are too large to fit into a batch, or they are ranged deletes.
 *   <li><tt>mutation_group_batches_received, mutation_group_batches_write_success,
 *       mutation_group_batches_write_failed</tt><br>
 *       Count the number of batches that are processed. If Failure Mode is set to {@link
 *       FailureMode#REPORT_FAILURES REPORT_FAILURES}, then failed batches will be split up and the
 *       individual mutation groups retried separately.
 *   <li><tt>mutation_groups_received, mutation_groups_write_success,
 *       mutation_groups_write_fail</tt><br>
 *       Count the number of individual MutationGroups that are processed.
 *   <li><tt>spanner_write_success, spanner_write_fail</tt><br>
 *       The number of writes to Spanner that have occurred.
 *   <li><tt>spanner_write_retries</tt><br>
 *       The number of times a write is retried after a failure - either due to a timeout, or when
 *       batches fail and {@link FailureMode#REPORT_FAILURES REPORT_FAILURES} is set so that
 *       individual Mutation Groups are retried.
 *   <li><tt>spanner_write_timeouts</tt><br>
 *       The number of timeouts that occur when writing to Spanner. Writes that timed out are
 *       retried after a backoff. Large numbers of timeouts suggest an overloaded Spanner instance.
 *   <li><tt>spanner_write_total_latency_ms</tt><br>
 *       The total amount of time spent writing to Spanner, in milliseconds.
 * </ul>
 *
 * <h3>Database Schema Preparation</h3>
 *
 * <p>The Write transform reads the database schema on pipeline start to know which columns are used
 * as primary keys of the tables and indexes. This is so that the transform knows how to sort the
 * grouped Mutations by table name and primary key as described above.
 *
 * <p>If the database schema, any additional tables or indexes are created in the same pipeline then
 * there will be a race condition, leading to a situation where the schema is read before the table
 * is created its primary key will not be known. This will mean that the sorting/batching will not
 * be optimal and performance will be reduced (warnings will be logged for rows using unknown
 * tables)
 *
 * <p>To prevent this race condition, use {@link Write#withSchemaReadySignal(PCollection)} to pass a
 * signal {@link PCollection} (for example the output of the transform that creates the table(s))
 * which will be used with {@link Wait.OnSignal} to prevent the schema from being read until it is
 * ready. The Write transform will be paused until this signal {@link PCollection} is closed.
 *
 * <h3>Transactions</h3>
 *
 * <p>The transform does not provide same transactional guarantees as Cloud Spanner. In particular,
 *
 * <ul>
 *   <li>Individual Mutations are submitted atomically, but all Mutations are not submitted in the
 *       same transaction.
 *   <li>A Mutation is applied at least once;
 *   <li>If the pipeline was unexpectedly stopped, mutations that were already applied will not get
 *       rolled back.
 * </ul>
 *
 * <p>Use {@link MutationGroup MutationGroups} with the {@link WriteGrouped} transform to ensure
 * that a small set mutations is bundled together. It is guaranteed that mutations in a {@link
 * MutationGroup} are submitted in the same transaction. Note that a MutationGroup must not exceed
 * the Spanner transaction limits.
 *
 * <pre>{@code
 * // Earlier in the pipeline, create a PCollection of MutationGroups to be written to Cloud Spanner.
 * PCollection<MutationGroup> mutationGroups = ...;
 * // Write mutation groups.
 * SpannerWriteResult result = mutationGroups.apply(
 *     "Write",
 *     SpannerIO.write().withInstanceId("instance").withDatabaseId("database").grouped());
 * }</pre>
 *
 * <h3>Streaming Support</h3>
 *
 * <p>{@link SpannerIO.Write} can be used as a streaming sink, however as with batch mode note that
 * the write order of individual {@link Mutation}/{@link MutationGroup} objects is not guaranteed.
 */
@Experimental(Kind.SOURCE_SINK)
public class SpannerIO {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerIO.class);

  private static final long DEFAULT_BATCH_SIZE_BYTES = 1024L * 1024L; // 1 MB
  // Max number of mutations to batch together.
  private static final int DEFAULT_MAX_NUM_MUTATIONS = 5000;
  // Max number of mutations to batch together.
  private static final int DEFAULT_MAX_NUM_ROWS = 500;
  // Multiple of mutation size to use to gather and sort mutations
  private static final int DEFAULT_GROUPING_FACTOR = 1000;

  /**
   * Creates an uninitialized instance of {@link Read}. Before use, the {@link Read} must be
   * configured with a {@link Read#withInstanceId} and {@link Read#withDatabaseId} that identify the
   * Cloud Spanner database.
   */
  public static Read read() {
    return new AutoValue_SpannerIO_Read.Builder()
        .setSpannerConfig(SpannerConfig.create())
        .setTimestampBound(TimestampBound.strong())
        .setReadOperation(ReadOperation.create())
        .setBatching(true)
        .build();
  }

  /**
   * A {@link PTransform} that works like {@link #read}, but executes read operations coming from a
   * {@link PCollection}.
   */
  public static ReadAll readAll() {
    return new AutoValue_SpannerIO_ReadAll.Builder()
        .setSpannerConfig(SpannerConfig.create())
        .setTimestampBound(TimestampBound.strong())
        .setBatching(true)
        .build();
  }

  /**
   * Returns a transform that creates a batch transaction. By default, {@link
   * TimestampBound#strong()} transaction is created, to override this use {@link
   * CreateTransaction#withTimestampBound(TimestampBound)}.
   */
  @Experimental
  public static CreateTransaction createTransaction() {
    return new AutoValue_SpannerIO_CreateTransaction.Builder()
        .setSpannerConfig(SpannerConfig.create())
        .setTimestampBound(TimestampBound.strong())
        .build();
  }

  /**
   * Creates an uninitialized instance of {@link Write}. Before use, the {@link Write} must be
   * configured with a {@link Write#withInstanceId} and {@link Write#withDatabaseId} that identify
   * the Cloud Spanner database being written.
   */
  @Experimental
  public static Write write() {
    return new AutoValue_SpannerIO_Write.Builder()
        .setSpannerConfig(SpannerConfig.create())
        .setBatchSizeBytes(DEFAULT_BATCH_SIZE_BYTES)
        .setMaxNumMutations(DEFAULT_MAX_NUM_MUTATIONS)
        .setMaxNumRows(DEFAULT_MAX_NUM_ROWS)
        .setFailureMode(FailureMode.FAIL_FAST)
        .build();
  }

  /** Implementation of {@link #readAll}. */
  @AutoValue
  public abstract static class ReadAll
      extends PTransform<PCollection<ReadOperation>, PCollection<Struct>> {

    abstract SpannerConfig getSpannerConfig();

    abstract @Nullable PCollectionView<Transaction> getTransaction();

    abstract @Nullable TimestampBound getTimestampBound();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setSpannerConfig(SpannerConfig spannerConfig);

      abstract Builder setTransaction(PCollectionView<Transaction> transaction);

      abstract Builder setTimestampBound(TimestampBound timestampBound);

      abstract Builder setBatching(Boolean batching);

      abstract ReadAll build();
    }

    /** Specifies the Cloud Spanner configuration. */
    public ReadAll withSpannerConfig(SpannerConfig spannerConfig) {
      return toBuilder().setSpannerConfig(spannerConfig).build();
    }

    /** Specifies the Cloud Spanner project. */
    public ReadAll withProjectId(String projectId) {
      return withProjectId(ValueProvider.StaticValueProvider.of(projectId));
    }

    /** Specifies the Cloud Spanner project. */
    public ReadAll withProjectId(ValueProvider<String> projectId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withProjectId(projectId));
    }

    /** Specifies the Cloud Spanner instance. */
    public ReadAll withInstanceId(String instanceId) {
      return withInstanceId(ValueProvider.StaticValueProvider.of(instanceId));
    }

    /** Specifies the Cloud Spanner instance. */
    public ReadAll withInstanceId(ValueProvider<String> instanceId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withInstanceId(instanceId));
    }

    /** Specifies the Cloud Spanner database. */
    public ReadAll withDatabaseId(String databaseId) {
      return withDatabaseId(ValueProvider.StaticValueProvider.of(databaseId));
    }

    /** Specifies the Cloud Spanner host. */
    public ReadAll withHost(ValueProvider<String> host) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withHost(host));
    }

    public ReadAll withHost(String host) {
      return withHost(ValueProvider.StaticValueProvider.of(host));
    }

    /** Specifies the Cloud Spanner database. */
    public ReadAll withDatabaseId(ValueProvider<String> databaseId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withDatabaseId(databaseId));
    }

    @VisibleForTesting
    ReadAll withServiceFactory(ServiceFactory<Spanner, SpannerOptions> serviceFactory) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withServiceFactory(serviceFactory));
    }

    public ReadAll withTransaction(PCollectionView<Transaction> transaction) {
      return toBuilder().setTransaction(transaction).build();
    }

    public ReadAll withTimestamp(Timestamp timestamp) {
      return withTimestampBound(TimestampBound.ofReadTimestamp(timestamp));
    }

    public ReadAll withTimestampBound(TimestampBound timestampBound) {
      return toBuilder().setTimestampBound(timestampBound).build();
    }

    /**
     * By default Batch API is used to read data from Cloud Spanner. It is useful to disable
     * batching when the underlying query is not root-partitionable.
     */
    public ReadAll withBatching(boolean batching) {
      return toBuilder().setBatching(batching).build();
    }

    abstract Boolean getBatching();

    @Override
    public PCollection<Struct> expand(PCollection<ReadOperation> input) {
      PTransform<PCollection<ReadOperation>, PCollection<Struct>> readTransform;
      if (getBatching()) {
        readTransform =
            BatchSpannerRead.create(getSpannerConfig(), getTransaction(), getTimestampBound());
      } else {
        readTransform =
            NaiveSpannerRead.create(getSpannerConfig(), getTransaction(), getTimestampBound());
      }
      return input
          .apply("Reshuffle", Reshuffle.viaRandomKey())
          .apply("Read from Cloud Spanner", readTransform);
    }
  }

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<Struct>> {

    abstract SpannerConfig getSpannerConfig();

    abstract ReadOperation getReadOperation();

    abstract @Nullable TimestampBound getTimestampBound();

    abstract @Nullable PCollectionView<Transaction> getTransaction();

    abstract @Nullable PartitionOptions getPartitionOptions();

    abstract Boolean getBatching();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setSpannerConfig(SpannerConfig spannerConfig);

      abstract Builder setReadOperation(ReadOperation readOperation);

      abstract Builder setTimestampBound(TimestampBound timestampBound);

      abstract Builder setTransaction(PCollectionView<Transaction> transaction);

      abstract Builder setPartitionOptions(PartitionOptions partitionOptions);

      abstract Builder setBatching(Boolean batching);

      abstract Read build();
    }

    /** Specifies the Cloud Spanner configuration. */
    public Read withSpannerConfig(SpannerConfig spannerConfig) {
      return toBuilder().setSpannerConfig(spannerConfig).build();
    }

    /** Specifies the Cloud Spanner project. */
    public Read withProjectId(String projectId) {
      return withProjectId(ValueProvider.StaticValueProvider.of(projectId));
    }

    /** Specifies the Cloud Spanner project. */
    public Read withProjectId(ValueProvider<String> projectId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withProjectId(projectId));
    }

    /** Specifies the Cloud Spanner instance. */
    public Read withInstanceId(String instanceId) {
      return withInstanceId(ValueProvider.StaticValueProvider.of(instanceId));
    }

    /** Specifies the Cloud Spanner instance. */
    public Read withInstanceId(ValueProvider<String> instanceId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withInstanceId(instanceId));
    }

    /** Specifies the Cloud Spanner database. */
    public Read withDatabaseId(String databaseId) {
      return withDatabaseId(ValueProvider.StaticValueProvider.of(databaseId));
    }

    /** Specifies the Cloud Spanner database. */
    public Read withDatabaseId(ValueProvider<String> databaseId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withDatabaseId(databaseId));
    }

    /** Specifies the Cloud Spanner host. */
    public Read withHost(ValueProvider<String> host) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withHost(host));
    }

    public Read withHost(String host) {
      return withHost(ValueProvider.StaticValueProvider.of(host));
    }

    /** If true the uses Cloud Spanner batch API. */
    public Read withBatching(boolean batching) {
      return toBuilder().setBatching(batching).build();
    }

    @VisibleForTesting
    Read withServiceFactory(ServiceFactory<Spanner, SpannerOptions> serviceFactory) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withServiceFactory(serviceFactory));
    }

    public Read withTransaction(PCollectionView<Transaction> transaction) {
      return toBuilder().setTransaction(transaction).build();
    }

    public Read withTimestamp(Timestamp timestamp) {
      return withTimestampBound(TimestampBound.ofReadTimestamp(timestamp));
    }

    public Read withTimestampBound(TimestampBound timestampBound) {
      return toBuilder().setTimestampBound(timestampBound).build();
    }

    public Read withTable(String table) {
      return withReadOperation(getReadOperation().withTable(table));
    }

    public Read withReadOperation(ReadOperation operation) {
      return toBuilder().setReadOperation(operation).build();
    }

    public Read withColumns(String... columns) {
      return withColumns(Arrays.asList(columns));
    }

    public Read withColumns(List<String> columns) {
      return withReadOperation(getReadOperation().withColumns(columns));
    }

    public Read withQuery(Statement statement) {
      return withReadOperation(getReadOperation().withQuery(statement));
    }

    public Read withQuery(String sql) {
      return withQuery(Statement.of(sql));
    }

    public Read withKeySet(KeySet keySet) {
      return withReadOperation(getReadOperation().withKeySet(keySet));
    }

    public Read withIndex(String index) {
      return withReadOperation(getReadOperation().withIndex(index));
    }

    public Read withPartitionOptions(PartitionOptions partitionOptions) {
      return withReadOperation(getReadOperation().withPartitionOptions(partitionOptions));
    }

    @Override
    public PCollection<Struct> expand(PBegin input) {
      getSpannerConfig().validate();
      checkArgument(
          getTimestampBound() != null,
          "SpannerIO.read() runs in a read only transaction and requires timestamp to be set "
              + "with withTimestampBound or withTimestamp method");

      if (getReadOperation().getQuery() != null) {
        // TODO: validate query?
      } else if (getReadOperation().getTable() != null) {
        // Assume read
        checkNotNull(
            getReadOperation().getColumns(),
            "For a read operation SpannerIO.read() requires a list of "
                + "columns to set with withColumns method");
        checkArgument(
            !getReadOperation().getColumns().isEmpty(),
            "For a read operation SpannerIO.read() requires a"
                + " list of columns to set with withColumns method");
      } else {
        throw new IllegalArgumentException(
            "SpannerIO.read() requires configuring query or read operation.");
      }

      ReadAll readAll =
          readAll()
              .withSpannerConfig(getSpannerConfig())
              .withTimestampBound(getTimestampBound())
              .withBatching(getBatching())
              .withTransaction(getTransaction());
      return input.apply(Create.of(getReadOperation())).apply("Execute query", readAll);
    }
  }

  /**
   * A {@link PTransform} that create a transaction.
   *
   * @see SpannerIO
   */
  @AutoValue
  public abstract static class CreateTransaction
      extends PTransform<PBegin, PCollectionView<Transaction>> {

    abstract SpannerConfig getSpannerConfig();

    abstract @Nullable TimestampBound getTimestampBound();

    abstract Builder toBuilder();

    @Override
    public PCollectionView<Transaction> expand(PBegin input) {
      getSpannerConfig().validate();

      return input
          .apply(Create.of(1))
          .apply("Create transaction", ParDo.of(new CreateTransactionFn(this)))
          .apply("As PCollectionView", View.asSingleton());
    }

    /** Specifies the Cloud Spanner configuration. */
    public CreateTransaction withSpannerConfig(SpannerConfig spannerConfig) {
      return toBuilder().setSpannerConfig(spannerConfig).build();
    }

    /** Specifies the Cloud Spanner project. */
    public CreateTransaction withProjectId(String projectId) {
      return withProjectId(ValueProvider.StaticValueProvider.of(projectId));
    }

    /** Specifies the Cloud Spanner project. */
    public CreateTransaction withProjectId(ValueProvider<String> projectId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withProjectId(projectId));
    }

    /** Specifies the Cloud Spanner instance. */
    public CreateTransaction withInstanceId(String instanceId) {
      return withInstanceId(ValueProvider.StaticValueProvider.of(instanceId));
    }

    /** Specifies the Cloud Spanner instance. */
    public CreateTransaction withInstanceId(ValueProvider<String> instanceId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withInstanceId(instanceId));
    }

    /** Specifies the Cloud Spanner database. */
    public CreateTransaction withDatabaseId(String databaseId) {
      return withDatabaseId(ValueProvider.StaticValueProvider.of(databaseId));
    }

    /** Specifies the Cloud Spanner database. */
    public CreateTransaction withDatabaseId(ValueProvider<String> databaseId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withDatabaseId(databaseId));
    }

    /** Specifies the Cloud Spanner host. */
    public CreateTransaction withHost(ValueProvider<String> host) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withHost(host));
    }

    public CreateTransaction withHost(String host) {
      return withHost(ValueProvider.StaticValueProvider.of(host));
    }

    @VisibleForTesting
    CreateTransaction withServiceFactory(ServiceFactory<Spanner, SpannerOptions> serviceFactory) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withServiceFactory(serviceFactory));
    }

    public CreateTransaction withTimestampBound(TimestampBound timestampBound) {
      return toBuilder().setTimestampBound(timestampBound).build();
    }

    /** A builder for {@link CreateTransaction}. */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setSpannerConfig(SpannerConfig spannerConfig);

      public abstract Builder setTimestampBound(TimestampBound newTimestampBound);

      public abstract CreateTransaction build();
    }
  }

  /** A failure handling strategy. */
  public enum FailureMode {
    /** Invalid write to Spanner will cause the pipeline to fail. A default strategy. */
    FAIL_FAST,
    /** Invalid mutations will be returned as part of the result of the write transform. */
    REPORT_FAILURES
  }

  /**
   * A {@link PTransform} that writes {@link Mutation} objects to Google Cloud Spanner.
   *
   * @see SpannerIO
   */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<Mutation>, SpannerWriteResult> {

    abstract SpannerConfig getSpannerConfig();

    abstract long getBatchSizeBytes();

    abstract long getMaxNumMutations();

    abstract long getMaxNumRows();

    abstract FailureMode getFailureMode();

    abstract @Nullable PCollection<?> getSchemaReadySignal();

    abstract OptionalInt getGroupingFactor();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setSpannerConfig(SpannerConfig spannerConfig);

      abstract Builder setBatchSizeBytes(long batchSizeBytes);

      abstract Builder setMaxNumMutations(long maxNumMutations);

      abstract Builder setMaxNumRows(long maxNumRows);

      abstract Builder setFailureMode(FailureMode failureMode);

      abstract Builder setSchemaReadySignal(PCollection<?> schemaReadySignal);

      abstract Builder setGroupingFactor(int groupingFactor);

      abstract Write build();
    }

    /** Specifies the Cloud Spanner configuration. */
    public Write withSpannerConfig(SpannerConfig spannerConfig) {
      return toBuilder().setSpannerConfig(spannerConfig).build();
    }

    /** Specifies the Cloud Spanner project. */
    public Write withProjectId(String projectId) {
      return withProjectId(ValueProvider.StaticValueProvider.of(projectId));
    }

    /** Specifies the Cloud Spanner project. */
    public Write withProjectId(ValueProvider<String> projectId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withProjectId(projectId));
    }

    /** Specifies the Cloud Spanner instance. */
    public Write withInstanceId(String instanceId) {
      return withInstanceId(ValueProvider.StaticValueProvider.of(instanceId));
    }

    /** Specifies the Cloud Spanner instance. */
    public Write withInstanceId(ValueProvider<String> instanceId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withInstanceId(instanceId));
    }

    /** Specifies the Cloud Spanner database. */
    public Write withDatabaseId(String databaseId) {
      return withDatabaseId(ValueProvider.StaticValueProvider.of(databaseId));
    }

    /** Specifies the Cloud Spanner database. */
    public Write withDatabaseId(ValueProvider<String> databaseId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withDatabaseId(databaseId));
    }

    /** Specifies the Cloud Spanner host. */
    public Write withHost(ValueProvider<String> host) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withHost(host));
    }

    /** Specifies the Cloud Spanner host. */
    public Write withHost(String host) {
      return withHost(ValueProvider.StaticValueProvider.of(host));
    }

    /**
     * Specifies the deadline for the Commit API call. Default is 15 secs. DEADLINE_EXCEEDED errors
     * will prompt a backoff/retry until the value of {@link #withMaxCumulativeBackoff(Duration)} is
     * reached. DEADLINE_EXCEEDED errors are are reported with logging and counters.
     */
    public Write withCommitDeadline(Duration commitDeadline) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withCommitDeadline(commitDeadline));
    }

    /**
     * Specifies the maximum cumulative backoff time when retrying after DEADLINE_EXCEEDED errors.
     * Default is 15 mins.
     *
     * <p>If the mutations still have not been written after this time, they are treated as a
     * failure, and handled according to the setting of {@link #withFailureMode(FailureMode)}.
     */
    public Write withMaxCumulativeBackoff(Duration maxCumulativeBackoff) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withMaxCumulativeBackoff(maxCumulativeBackoff));
    }

    @VisibleForTesting
    Write withServiceFactory(ServiceFactory<Spanner, SpannerOptions> serviceFactory) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withServiceFactory(serviceFactory));
    }

    /** Same transform but can be applied to {@link PCollection} of {@link MutationGroup}. */
    public WriteGrouped grouped() {
      return new WriteGrouped(this);
    }

    /**
     * Specifies the batch size limit (max number of bytes mutated per batch). Default value is 1MB
     */
    public Write withBatchSizeBytes(long batchSizeBytes) {
      return toBuilder().setBatchSizeBytes(batchSizeBytes).build();
    }

    /** Specifies failure mode. {@link FailureMode#FAIL_FAST} mode is selected by default. */
    public Write withFailureMode(FailureMode failureMode) {
      return toBuilder().setFailureMode(failureMode).build();
    }

    /**
     * Specifies the cell mutation limit (maximum number of mutated cells per batch). Default value
     * is 5000
     */
    public Write withMaxNumMutations(long maxNumMutations) {
      return toBuilder().setMaxNumMutations(maxNumMutations).build();
    }

    /**
     * Specifies the row mutation limit (maximum number of mutated rows per batch). Default value is
     * 1000
     */
    public Write withMaxNumRows(long maxNumRows) {
      return toBuilder().setMaxNumRows(maxNumRows).build();
    }

    /**
     * Specifies an optional input PCollection that can be used as the signal for {@link
     * Wait.OnSignal} to indicate when the database schema is ready to be read.
     *
     * <p>To be used when the database schema is created by another section of the pipeline, this
     * causes this transform to wait until the {@code signal PCollection} has been closed before
     * reading the schema from the database.
     *
     * @see Wait.OnSignal
     */
    public Write withSchemaReadySignal(PCollection<?> signal) {
      return toBuilder().setSchemaReadySignal(signal).build();
    }

    /**
     * Specifies the multiple of max mutation (in terms of both bytes per batch and cells per batch)
     * that is used to select a set of mutations to sort by key for batching. This sort uses local
     * memory on the workers, so using large values can cause out of memory errors. Default value is
     * 1000.
     */
    public Write withGroupingFactor(int groupingFactor) {
      return toBuilder().setGroupingFactor(groupingFactor).build();
    }

    @Override
    public SpannerWriteResult expand(PCollection<Mutation> input) {
      getSpannerConfig().validate();

      return input
          .apply("To mutation group", ParDo.of(new ToMutationGroupFn()))
          .apply("Write mutations to Cloud Spanner", new WriteGrouped(this));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      populateDisplayDataWithParamaters(builder);
    }

    private void populateDisplayDataWithParamaters(DisplayData.Builder builder) {
      getSpannerConfig().populateDisplayData(builder);
      builder.add(
          DisplayData.item("batchSizeBytes", getBatchSizeBytes())
              .withLabel("Max batch size in bytes"));
      builder.add(
          DisplayData.item("maxNumMutations", getMaxNumMutations())
              .withLabel("Max number of mutated cells in each batch"));
      builder.add(
          DisplayData.item("maxNumRows", getMaxNumRows())
              .withLabel("Max number of rows in each batch"));
      // Grouping factor default value depends on whether it is a batch or streaming pipeline.
      // This function is not aware of that state, so use 'DEFAULT' if unset.
      builder.add(
          DisplayData.item(
                  "groupingFactor",
                  (getGroupingFactor().isPresent()
                      ? Integer.toString(getGroupingFactor().getAsInt())
                      : "DEFAULT"))
              .withLabel("Number of batches to sort over"));
    }
  }

  /** Same as {@link Write} but supports grouped mutations. */
  public static class WriteGrouped
      extends PTransform<PCollection<MutationGroup>, SpannerWriteResult> {
    private final Write spec;
    private static final TupleTag<MutationGroup> BATCHABLE_MUTATIONS_TAG =
        new TupleTag<MutationGroup>("batchableMutations") {};
    private static final TupleTag<Iterable<MutationGroup>> UNBATCHABLE_MUTATIONS_TAG =
        new TupleTag<Iterable<MutationGroup>>("unbatchableMutations") {};

    private static final TupleTag<Void> MAIN_OUT_TAG = new TupleTag<Void>("mainOut") {};
    private static final TupleTag<MutationGroup> FAILED_MUTATIONS_TAG =
        new TupleTag<MutationGroup>("failedMutations") {};
    private static final SerializableCoder<MutationGroup> CODER =
        SerializableCoder.of(MutationGroup.class);

    public WriteGrouped(Write spec) {
      this.spec = spec;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      spec.populateDisplayDataWithParamaters(builder);
    }

    @Override
    public SpannerWriteResult expand(PCollection<MutationGroup> input) {
      PCollection<Iterable<MutationGroup>> batches;

      if (spec.getBatchSizeBytes() <= 1
          || spec.getMaxNumMutations() <= 1
          || spec.getMaxNumRows() <= 1) {
        LOG.info("Batching of mutationGroups is disabled");
        TypeDescriptor<Iterable<MutationGroup>> descriptor =
            new TypeDescriptor<Iterable<MutationGroup>>() {};
        batches =
            input.apply(MapElements.into(descriptor).via(element -> ImmutableList.of(element)));
      } else {

        // First, read the Cloud Spanner schema.
        PCollection<Void> schemaSeed =
            input.getPipeline().apply("Create Seed", Create.of((Void) null));
        if (spec.getSchemaReadySignal() != null) {
          // Wait for external signal before reading schema.
          schemaSeed = schemaSeed.apply("Wait for schema", Wait.on(spec.getSchemaReadySignal()));
        }
        final PCollectionView<SpannerSchema> schemaView =
            schemaSeed
                .apply(
                    "Read information schema",
                    ParDo.of(new ReadSpannerSchema(spec.getSpannerConfig())))
                .apply("Schema View", View.asSingleton());

        // Split the mutations into batchable and unbatchable mutations.
        // Filter out mutation groups too big to be batched.
        PCollectionTuple filteredMutations =
            input
                .apply(
                    "RewindowIntoGlobal",
                    Window.<MutationGroup>into(new GlobalWindows())
                        .triggering(DefaultTrigger.of())
                        .discardingFiredPanes())
                .apply(
                    "Filter Unbatchable Mutations",
                    ParDo.of(
                            new BatchableMutationFilterFn(
                                schemaView,
                                UNBATCHABLE_MUTATIONS_TAG,
                                spec.getBatchSizeBytes(),
                                spec.getMaxNumMutations(),
                                spec.getMaxNumRows()))
                        .withSideInputs(schemaView)
                        .withOutputTags(
                            BATCHABLE_MUTATIONS_TAG, TupleTagList.of(UNBATCHABLE_MUTATIONS_TAG)));

        // Build a set of Mutation groups from the current bundle,
        // sort them by table/key then split into batches.
        PCollection<Iterable<MutationGroup>> batchedMutations =
            filteredMutations
                .get(BATCHABLE_MUTATIONS_TAG)
                .apply(
                    "Gather Sort And Create Batches",
                    ParDo.of(
                            new GatherSortCreateBatchesFn(
                                spec.getBatchSizeBytes(),
                                spec.getMaxNumMutations(),
                                spec.getMaxNumRows(),
                                // Do not group on streaming unless explicitly set.
                                spec.getGroupingFactor()
                                    .orElse(
                                        input.isBounded() == IsBounded.BOUNDED
                                            ? DEFAULT_GROUPING_FACTOR
                                            : 1),
                                schemaView))
                        .withSideInputs(schemaView));

        // Merge the batched and unbatchable mutation PCollections and write to Spanner.
        batches =
            PCollectionList.of(filteredMutations.get(UNBATCHABLE_MUTATIONS_TAG))
                .and(batchedMutations)
                .apply("Merge", Flatten.pCollections());
      }

      PCollectionTuple result =
          batches.apply(
              "Write batches to Spanner",
              ParDo.of(
                      new WriteToSpannerFn(
                          spec.getSpannerConfig(), spec.getFailureMode(), FAILED_MUTATIONS_TAG))
                  .withOutputTags(MAIN_OUT_TAG, TupleTagList.of(FAILED_MUTATIONS_TAG)));

      return new SpannerWriteResult(
          input.getPipeline(),
          result.get(MAIN_OUT_TAG),
          result.get(FAILED_MUTATIONS_TAG),
          FAILED_MUTATIONS_TAG);
    }

    @VisibleForTesting
    static MutationGroup decode(byte[] bytes) {
      ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
      try {
        return CODER.decode(bis);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @VisibleForTesting
    static byte[] encode(MutationGroup g) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try {
        CODER.encode(g, bos);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return bos.toByteArray();
    }
  }

  private static class ToMutationGroupFn extends DoFn<Mutation, MutationGroup> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      Mutation value = c.element();
      c.output(MutationGroup.create(value));
    }
  }

  /**
   * Gathers a set of mutations together, gets the keys, encodes them to byte[], sorts them and then
   * outputs the encoded sorted list.
   *
   * <p>Testing notes: With very small amounts of data, each mutation group is in a separate bundle,
   * and as batching and sorting is over the bundle, this effectively means that no batching will
   * occur, Therefore this DoFn has to be tested in isolation.
   */
  @VisibleForTesting
  static class GatherSortCreateBatchesFn extends DoFn<MutationGroup, Iterable<MutationGroup>> {

    private final long maxBatchSizeBytes;
    private final long maxBatchNumMutations;
    private final long maxBatchNumRows;
    private final long maxSortableSizeBytes;
    private final long maxSortableNumMutations;
    private final long maxSortableNumRows;
    private final PCollectionView<SpannerSchema> schemaView;
    private final ArrayList<MutationGroupContainer> mutationsToSort = new ArrayList<>();

    // total size of MutationGroups in mutationsToSort.
    private long sortableSizeBytes = 0;
    // total number of mutated cells in mutationsToSort
    private long sortableNumCells = 0;
    // total number of rows mutated in mutationsToSort
    private long sortableNumRows = 0;

    GatherSortCreateBatchesFn(
        long maxBatchSizeBytes,
        long maxNumMutations,
        long maxNumRows,
        long groupingFactor,
        PCollectionView<SpannerSchema> schemaView) {
      this.maxBatchSizeBytes = maxBatchSizeBytes;
      this.maxBatchNumMutations = maxNumMutations;
      this.maxBatchNumRows = maxNumRows;

      if (groupingFactor <= 0) {
        groupingFactor = 1;
      }

      this.maxSortableSizeBytes = maxBatchSizeBytes * groupingFactor;
      this.maxSortableNumMutations = maxNumMutations * groupingFactor;
      this.maxSortableNumRows = maxNumRows * groupingFactor;
      this.schemaView = schemaView;

      initSorter();
    }

    private synchronized void initSorter() {
      mutationsToSort.clear();
      sortableSizeBytes = 0;
      sortableNumCells = 0;
      sortableNumRows = 0;
    }

    @FinishBundle
    public synchronized void finishBundle(FinishBundleContext c) throws Exception {
      sortAndOutputBatches(new OutputReceiverForFinishBundle(c));
    }

    private synchronized void sortAndOutputBatches(OutputReceiver<Iterable<MutationGroup>> out)
        throws IOException {
      try {
        if (mutationsToSort.isEmpty()) {
          // nothing to output.
          return;
        }

        if (maxSortableNumMutations == maxBatchNumMutations) {
          // no grouping is occurring, no need to sort and make batches, just output what we have.
          outputBatch(out, 0, mutationsToSort.size());
          return;
        }

        // Sort then split the sorted mutations into batches.
        mutationsToSort.sort(Comparator.naturalOrder());
        int batchStart = 0;
        int batchEnd = 0;

        // total size of the current batch.
        long batchSizeBytes = 0;
        // total number of mutated cells.
        long batchCells = 0;
        // total number of rows mutated.
        long batchRows = 0;

        // collect and output batches.
        while (batchEnd < mutationsToSort.size()) {
          MutationGroupContainer mg = mutationsToSort.get(batchEnd);

          if (((batchCells + mg.numCells) > maxBatchNumMutations)
              || ((batchSizeBytes + mg.sizeBytes) > maxBatchSizeBytes
                  || (batchRows + mg.numRows > maxBatchNumRows))) {
            // Cannot add new element, current batch is full; output.
            outputBatch(out, batchStart, batchEnd);
            batchStart = batchEnd;
            batchSizeBytes = 0;
            batchCells = 0;
            batchRows = 0;
          }

          batchEnd++;
          batchSizeBytes += mg.sizeBytes;
          batchCells += mg.numCells;
          batchRows += mg.numRows;
        }

        if (batchStart < batchEnd) {
          // output remaining elements
          outputBatch(out, batchStart, mutationsToSort.size());
        }
      } finally {
        initSorter();
      }
    }

    private void outputBatch(
        OutputReceiver<Iterable<MutationGroup>> out, int batchStart, int batchEnd) {
      out.output(
          mutationsToSort.subList(batchStart, batchEnd).stream()
              .map(o -> o.mutationGroup)
              .collect(Collectors.toList()));
    }

    @ProcessElement
    public synchronized void processElement(
        ProcessContext c, OutputReceiver<Iterable<MutationGroup>> out) throws Exception {
      SpannerSchema spannerSchema = c.sideInput(schemaView);
      MutationKeyEncoder encoder = new MutationKeyEncoder(spannerSchema);
      MutationGroup mg = c.element();
      long groupSize = MutationSizeEstimator.sizeOf(mg);
      long groupCells = MutationCellCounter.countOf(spannerSchema, mg);
      long groupRows = mg.size();

      synchronized (this) {
        if (((sortableNumCells + groupCells) > maxSortableNumMutations)
            || (sortableSizeBytes + groupSize) > maxSortableSizeBytes
            || (sortableNumRows + groupRows) > maxSortableNumRows) {
          sortAndOutputBatches(out);
        }

        mutationsToSort.add(
            new MutationGroupContainer(
                mg, groupSize, groupCells, groupRows, encoder.encodeTableNameAndKey(mg.primary())));
        sortableSizeBytes += groupSize;
        sortableNumCells += groupCells;
        sortableNumRows += groupRows;
      }
    }

    // Container class to store a MutationGroup, its sortable encoded key and its statistics.
    private static final class MutationGroupContainer
        implements Comparable<MutationGroupContainer> {

      final MutationGroup mutationGroup;
      final long sizeBytes;
      final long numCells;
      final long numRows;
      final byte[] encodedKey;

      MutationGroupContainer(
          MutationGroup mutationGroup,
          long sizeBytes,
          long numCells,
          long numRows,
          byte[] encodedKey) {
        this.mutationGroup = mutationGroup;
        this.sizeBytes = sizeBytes;
        this.numCells = numCells;
        this.numRows = numRows;
        this.encodedKey = encodedKey;
      }

      @Override
      public int compareTo(MutationGroupContainer o) {
        return UnsignedBytes.lexicographicalComparator().compare(this.encodedKey, o.encodedKey);
      }
    }

    // TODO(BEAM-1287): Remove this when FinishBundle has added support for an {@link
    // OutputReceiver}
    private static class OutputReceiverForFinishBundle
        implements OutputReceiver<Iterable<MutationGroup>> {

      private final FinishBundleContext c;

      OutputReceiverForFinishBundle(FinishBundleContext c) {
        this.c = c;
      }

      @Override
      public void output(Iterable<MutationGroup> output) {
        outputWithTimestamp(output, Instant.now());
      }

      @Override
      public void outputWithTimestamp(Iterable<MutationGroup> output, Instant timestamp) {
        c.output(output, timestamp, GlobalWindow.INSTANCE);
      }
    }
  }

  /**
   * Filters MutationGroups larger than the batch size to the output tagged with {@code
   * UNBATCHABLE_MUTATIONS_TAG}.
   *
   * <p>Testing notes: As batching does not occur during full pipline testing, this DoFn must be
   * tested in isolation.
   */
  @VisibleForTesting
  static class BatchableMutationFilterFn extends DoFn<MutationGroup, MutationGroup> {

    private final PCollectionView<SpannerSchema> schemaView;
    private final TupleTag<Iterable<MutationGroup>> unbatchableMutationsTag;
    private final long batchSizeBytes;
    private final long maxNumMutations;
    private final long maxNumRows;
    private final Counter batchableMutationGroupsCounter =
        Metrics.counter(WriteGrouped.class, "batchable_mutation_groups");
    private final Counter unBatchableMutationGroupsCounter =
        Metrics.counter(WriteGrouped.class, "unbatchable_mutation_groups");

    BatchableMutationFilterFn(
        PCollectionView<SpannerSchema> schemaView,
        TupleTag<Iterable<MutationGroup>> unbatchableMutationsTag,
        long batchSizeBytes,
        long maxNumMutations,
        long maxNumRows) {
      this.schemaView = schemaView;
      this.unbatchableMutationsTag = unbatchableMutationsTag;
      this.batchSizeBytes = batchSizeBytes;
      this.maxNumMutations = maxNumMutations;
      this.maxNumRows = maxNumRows;
    }

    @DoFn.ProcessElement
    public void processElement(ProcessContext c) {
      MutationGroup mg = c.element();
      if (mg.primary().getOperation() == Op.DELETE && !isPointDelete(mg.primary())) {
        // Ranged deletes are not batchable.
        c.output(unbatchableMutationsTag, Arrays.asList(mg));
        unBatchableMutationGroupsCounter.inc();
        return;
      }

      SpannerSchema spannerSchema = c.sideInput(schemaView);
      long groupSize = MutationSizeEstimator.sizeOf(mg);
      long groupCells = MutationCellCounter.countOf(spannerSchema, mg);
      long groupRows = Iterables.size(mg);

      if (groupSize >= batchSizeBytes || groupCells >= maxNumMutations || groupRows >= maxNumRows) {
        c.output(unbatchableMutationsTag, Arrays.asList(mg));
        unBatchableMutationGroupsCounter.inc();
      } else {
        c.output(mg);
        batchableMutationGroupsCounter.inc();
      }
    }
  }

  @VisibleForTesting
  static class WriteToSpannerFn extends DoFn<Iterable<MutationGroup>, Void> {

    private final SpannerConfig spannerConfig;
    private final FailureMode failureMode;

    // SpannerAccessor can not be serialized so must be initialized at runtime in setup().
    private transient SpannerAccessor spannerAccessor;

    /* Number of times an aborted write to spanner could be retried */
    private static final int ABORTED_RETRY_ATTEMPTS = 5;
    /* Error string in Aborted exception during schema change */
    private final String errString =
        "Transaction aborted. "
            + "Database schema probably changed during transaction, retry may succeed.";

    @VisibleForTesting static Sleeper sleeper = Sleeper.DEFAULT;

    private final Counter mutationGroupBatchesReceived =
        Metrics.counter(WriteGrouped.class, "mutation_group_batches_received");
    private final Counter mutationGroupBatchesWriteSuccess =
        Metrics.counter(WriteGrouped.class, "mutation_group_batches_write_success");
    private final Counter mutationGroupBatchesWriteFail =
        Metrics.counter(WriteGrouped.class, "mutation_group_batches_write_fail");

    private final Counter mutationGroupsReceived =
        Metrics.counter(WriteGrouped.class, "mutation_groups_received");
    private final Counter mutationGroupsWriteSuccess =
        Metrics.counter(WriteGrouped.class, "mutation_groups_write_success");
    private final Counter mutationGroupsWriteFail =
        Metrics.counter(WriteGrouped.class, "mutation_groups_write_fail");

    private final Counter spannerWriteSuccess =
        Metrics.counter(WriteGrouped.class, "spanner_write_success");
    private final Counter spannerWriteFail =
        Metrics.counter(WriteGrouped.class, "spanner_write_fail");
    private final Distribution spannerWriteLatency =
        Metrics.distribution(WriteGrouped.class, "spanner_write_latency_ms");
    private final Counter spannerWriteTimeouts =
        Metrics.counter(WriteGrouped.class, "spanner_write_timeouts");
    private final Counter spannerWriteRetries =
        Metrics.counter(WriteGrouped.class, "spanner_write_retries");

    private final TupleTag<MutationGroup> failedTag;

    // Fluent Backoff is not serializable so create at runtime in setup().
    private transient FluentBackoff bundleWriteBackoff;

    WriteToSpannerFn(
        SpannerConfig spannerConfig, FailureMode failureMode, TupleTag<MutationGroup> failedTag) {
      this.spannerConfig = spannerConfig;
      this.failureMode = failureMode;
      this.failedTag = failedTag;
    }

    @Setup
    public void setup() {
      spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
      bundleWriteBackoff =
          FluentBackoff.DEFAULT
              .withMaxCumulativeBackoff(spannerConfig.getMaxCumulativeBackoff().get())
              .withInitialBackoff(spannerConfig.getMaxCumulativeBackoff().get().dividedBy(60));
    }

    @Teardown
    public void teardown() {
      spannerAccessor.close();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Iterable<MutationGroup> mutations = c.element();

      // Batch upsert rows.
      try {
        mutationGroupBatchesReceived.inc();
        mutationGroupsReceived.inc(Iterables.size(mutations));
        Iterable<Mutation> batch = Iterables.concat(mutations);
        writeMutations(batch);
        mutationGroupBatchesWriteSuccess.inc();
        mutationGroupsWriteSuccess.inc(Iterables.size(mutations));
        return;
      } catch (SpannerException e) {
        mutationGroupBatchesWriteFail.inc();
        if (failureMode == FailureMode.REPORT_FAILURES) {
          // fall through and retry individual mutationGroups.
        } else if (failureMode == FailureMode.FAIL_FAST) {
          mutationGroupsWriteFail.inc(Iterables.size(mutations));
          throw e;
        } else {
          throw new IllegalArgumentException("Unknown failure mode " + failureMode);
        }
      }

      // If we are here, writing a batch has failed, retry individual mutations.
      for (MutationGroup mg : mutations) {
        try {
          spannerWriteRetries.inc();
          writeMutations(mg);
          mutationGroupsWriteSuccess.inc();
        } catch (SpannerException e) {
          mutationGroupsWriteFail.inc();
          LOG.warn("Failed to write the mutation group: " + mg, e);
          c.output(failedTag, mg);
        }
      }
    }

    /*
     Spanner aborts all inflight transactions during a schema change. Client is expected
     to retry silently. These must not be counted against retry backoff.
    */
    private void spannerWriteWithRetryIfSchemaChange(Iterable<Mutation> batch)
        throws SpannerException {
      for (int retry = 1; ; retry++) {
        try {
          spannerAccessor.getDatabaseClient().writeAtLeastOnce(batch);
          return;
        } catch (AbortedException e) {
          if (retry >= ABORTED_RETRY_ATTEMPTS) {
            throw e;
          }
          if (e.isRetryable() || e.getMessage().contains(errString)) {
            continue;
          }
          throw e;
        }
      }
    }

    /** Write the Mutations to Spanner, handling DEADLINE_EXCEEDED with backoff/retries. */
    private void writeMutations(Iterable<Mutation> mutations) throws SpannerException, IOException {
      BackOff backoff = bundleWriteBackoff.backoff();
      long mutationsSize = Iterables.size(mutations);

      while (true) {
        Stopwatch timer = Stopwatch.createStarted();
        // loop is broken on success, timeout backoff/retry attempts exceeded, or other failure.
        try {
          spannerWriteWithRetryIfSchemaChange(mutations);
          spannerWriteSuccess.inc();
          return;
        } catch (SpannerException exception) {
          if (exception.getErrorCode() == ErrorCode.DEADLINE_EXCEEDED) {
            spannerWriteTimeouts.inc();

            // Potentially backoff/retry after DEADLINE_EXCEEDED.
            long sleepTimeMsecs = backoff.nextBackOffMillis();
            if (sleepTimeMsecs == BackOff.STOP) {
              LOG.error(
                  "DEADLINE_EXCEEDED writing batch of {} mutations to Cloud Spanner. "
                      + "Aborting after too many retries.",
                  mutationsSize);
              spannerWriteFail.inc();
              throw exception;
            }
            LOG.info(
                "DEADLINE_EXCEEDED writing batch of {} mutations to Cloud Spanner, "
                    + "retrying after backoff of {}ms\n"
                    + "({})",
                mutationsSize,
                sleepTimeMsecs,
                exception.getMessage());
            spannerWriteRetries.inc();
            try {
              sleeper.sleep(sleepTimeMsecs);
            } catch (InterruptedException e) {
              // ignore.
            }
          } else {
            // Some other failure: pass up the stack.
            spannerWriteFail.inc();
            throw exception;
          }
        } finally {
          spannerWriteLatency.update(timer.elapsed(TimeUnit.MILLISECONDS));
        }
      }
    }
  }

  private SpannerIO() {} // Prevent construction.
}
