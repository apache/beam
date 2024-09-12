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

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.io.gcp.spanner.MutationUtils.isPointDelete;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamsConstants.DEFAULT_CHANGE_STREAM_NAME;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamsConstants.DEFAULT_INCLUSIVE_END_AT;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamsConstants.DEFAULT_INCLUSIVE_START_AT;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamsConstants.DEFAULT_RPC_PRIORITY;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamsConstants.MAX_INCLUSIVE_END_AT;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamsConstants.THROUGHPUT_WINDOW_SECONDS;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.NameGenerator.generatePartitionMetadataTableName;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.auth.Credentials;
import com.google.auto.value.AutoValue;
import com.google.cloud.ServiceFactory;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.AbortedException;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.Op;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Options.RpcPriority;
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
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.runners.core.metrics.GcpResourceIdentifiers;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.ServiceCallMetric;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamsConstants;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.MetadataSpannerConfigFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.action.ActionFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.CleanUpReadChangeStreamDoFn;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.DetectNewPartitionsDoFn;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.InitializeDoFn;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.PostProcessingMetricsDoFn;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.ReadChangeStreamPartitionDoFn;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.estimator.BytesThroughputEstimator;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.estimator.SizeEstimator;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.MapperFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.WithTimestamps;
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
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Stopwatch;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.UnsignedBytes;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *
 * <h3>Reading from Cloud Spanner</h3>
 *
 * <h4>Bulk reading of a single query or table</h4>
 *
 * <p>To perform a single read from Cloud Spanner, construct a {@link Read} transform using {@link
 * SpannerIO#read() SpannerIO.read()}. It will return a {@link PCollection} of {@link Struct
 * Structs}, where each element represents an individual row returned from the read operation. Both
 * Query and Read APIs are supported. See more information about <a
 * href="https://cloud.google.com/spanner/docs/reads">reading from Cloud Spanner</a>
 *
 * <p>To execute a <strong>Query</strong>, specify a {@link Read#withQuery(Statement)} or {@link
 * Read#withQuery(String)} during the construction of the transform.
 *
 * <pre>{@code
 * PCollection<Struct> rows = p.apply(
 *     SpannerIO.read()
 *         .withInstanceId(instanceId)
 *         .withDatabaseId(dbId)
 *         .withQuery("SELECT id, name, email FROM users"));
 * }</pre>
 *
 * <p>Reads by default use the <a
 * href="https://cloud.google.com/spanner/docs/reads#read_data_in_parallel">PartitionQuery API</a>
 * which enforces some limitations on the type of queries that can be used so that the data can be
 * read in parallel. If the query is not supported by the PartitionQuery API, then you can specify a
 * non-partitioned read by setting {@link Read#withBatching(boolean) withBatching(false)}. If the
 * amount of data being read by a non-partitioned read is very large, it may be useful to add a
 * {@link Reshuffle#viaRandomKey()} transform on the output so that the downstream transforms can
 * run in parallel.
 *
 * <p>To read an entire <strong>Table</strong>, use {@link Read#withTable(String)} and optionally
 * specify a {@link Read#withColumns(List) list of columns}.
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
 * <p>To read using an <strong>Index</strong>, specify the index name using {@link
 * Read#withIndex(String)}.
 *
 * <pre>{@code
 * PCollection<Struct> rows = p.apply(
 *    SpannerIO.read()
 *        .withInstanceId(instanceId)
 *        .withDatabaseId(dbId)
 *        .withTable("users")
 *        .withIndex("users_by_name")
 *        .withColumns("id", "name", "email"));
 * }</pre>
 *
 * <h4>Read consistency</h4>
 *
 * <p>The transform is guaranteed to be executed on a consistent snapshot of data, utilizing the
 * power of read only transactions. Staleness of data can be controlled using {@link
 * Read#withTimestampBound} or {@link Read#withTimestamp(Timestamp)} methods. <a
 * href="https://cloud.google.com/spanner/docs/transactions#read-only_transactions">Read more</a>
 * about transactions in Cloud Spanner.
 *
 * <p>It is possible to read several {@link PCollection PCollections} within a single transaction.
 * Apply {@link SpannerIO#createTransaction()} transform, that lazily creates a transaction. The
 * result of this transformation can be passed to read operation using {@link
 * Read#withTransaction(PCollectionView)}.
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
 * <h4>Bulk reading of multiple queries or tables</h4>
 *
 * You can perform multiple consistent reads on a set of tables or using a set of queries by
 * constructing a {@link ReadAll} transform using {@link SpannerIO#readAll() SpannerIO.readAll()}.
 * This transform takes a {@link PCollection} of {@link ReadOperation} elements, and performs the
 * partitioned read on each of them using the same Read Only Transaction for consistent results.
 *
 * <p>Note that this transform should <strong>not</strong> be used in Streaming pipelines. This is
 * because the same Read Only Transaction, which is created once when the pipeline is first
 * executed, will be used for all reads. The data being read will therefore become stale, and if no
 * reads are made for more than 1 hour, the transaction will automatically timeout and be closed by
 * the Spanner server, meaning that any subsequent reads will fail.
 *
 * <pre>{@code
 * // Build a collection of ReadOperations.
 * PCollection<ReadOperation> reads = ...
 *
 * PCollection<Struct> rows = reads.apply(
 *     SpannerIO.readAll()
 *         .withInstanceId(instanceId)
 *         .withDatabaseId(dbId)
 * }</pre>
 *
 * <h3>Writing to Cloud Spanner</h3>
 *
 * <p>The Cloud Spanner {@link Write} transform writes to Cloud Spanner by executing a collection of
 * input row {@link Mutation Mutations}. The mutations are grouped into batches for efficiency.
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
 * PCollection} that can be used in batch pipelines as a completion signal to {@link Wait
 * Wait.OnSignal} to indicate when all input has been written. Note that in streaming pipelines,
 * this signal will never be triggered as the input is unbounded and this {@link PCollection} is
 * using the {@link GlobalWindow}.
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
 * <p>{@link Write} can be used as a streaming sink, however as with batch mode note that the write
 * order of individual {@link Mutation}/{@link MutationGroup} objects is not guaranteed.
 *
 * <p>{@link Read} and {@link ReadAll} can be used in Streaming pipelines to read a set of Facts on
 * pipeline startup.
 *
 * <p>{@link ReadAll} should not be used on an unbounded {@code PCollection<ReadOperation>}, for the
 * reasons stated above.
 *
 * <h3>Updates to the I/O connector code</h3>
 *
 * For any significant significant updates to this I/O connector, please consider involving
 * corresponding code reviewers mentioned <a
 * href="https://github.com/apache/beam/blob/master/sdks/java/io/google-cloud-platform/OWNERS">
 * here</a>.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SpannerIO {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerIO.class);

  private static final long DEFAULT_BATCH_SIZE_BYTES = 1024L * 1024L; // 1 MB
  // Max number of mutations to batch together.
  private static final int DEFAULT_MAX_NUM_MUTATIONS = 5000;
  // Max number of mutations to batch together.
  private static final int DEFAULT_MAX_NUM_ROWS = 500;
  // Multiple of mutation size to use to gather and sort mutations
  private static final int DEFAULT_GROUPING_FACTOR = 1000;

  // Size of caches for read/write ServiceCallMetric objects .
  // This is a reasonable limit, as for reads, each worker will process very few different table
  // read requests, and for writes, batching will ensure that write operations for the same
  // table occur at the same time (within a bundle).
  static final int METRICS_CACHE_SIZE = 100;

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

  public static Read readWithSchema() {
    return read()
        .withBeamRowConverters(
            TypeDescriptor.of(Struct.class),
            StructUtils.structToBeamRow(),
            StructUtils.structFromBeamRow());
  }

  /**
   * Returns a transform that creates a batch transaction. By default, {@link
   * TimestampBound#strong()} transaction is created, to override this use {@link
   * CreateTransaction#withTimestampBound(TimestampBound)}.
   */
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
  public static Write write() {
    return new AutoValue_SpannerIO_Write.Builder()
        .setSpannerConfig(SpannerConfig.create())
        .setBatchSizeBytes(DEFAULT_BATCH_SIZE_BYTES)
        .setMaxNumMutations(DEFAULT_MAX_NUM_MUTATIONS)
        .setMaxNumRows(DEFAULT_MAX_NUM_ROWS)
        .setFailureMode(FailureMode.FAIL_FAST)
        .build();
  }

  /**
   * Creates an uninitialized instance of {@link ReadChangeStream}. Before use, the {@link
   * ReadChangeStream} must be configured with a {@link ReadChangeStream#withProjectId}, {@link
   * ReadChangeStream#withInstanceId}, and {@link ReadChangeStream#withDatabaseId} that identify the
   * Cloud Spanner database being written. It must also be configured with the start time and the
   * change stream name.
   */
  public static ReadChangeStream readChangeStream() {
    return new AutoValue_SpannerIO_ReadChangeStream.Builder()
        .setSpannerConfig(SpannerConfig.create())
        .setChangeStreamName(DEFAULT_CHANGE_STREAM_NAME)
        .setRpcPriority(DEFAULT_RPC_PRIORITY)
        .setInclusiveStartAt(DEFAULT_INCLUSIVE_START_AT)
        .setInclusiveEndAt(DEFAULT_INCLUSIVE_END_AT)
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

    /** Specifies the Cloud Spanner emulator host. */
    public ReadAll withEmulatorHost(ValueProvider<String> emulatorHost) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withEmulatorHost(emulatorHost));
    }

    public ReadAll withEmulatorHost(String emulatorHost) {
      return withEmulatorHost(ValueProvider.StaticValueProvider.of(emulatorHost));
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
     * By default the <a
     * href="https://cloud.google.com/spanner/docs/reads#read_data_in_parallel">PartitionQuery
     * API</a> is used to read data from Cloud Spanner. It is useful to disable batching when the
     * underlying query is not root-partitionable.
     */
    public ReadAll withBatching(boolean batching) {
      return toBuilder().setBatching(batching).build();
    }

    public ReadAll withLowPriority() {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withRpcPriority(RpcPriority.LOW));
    }

    public ReadAll withHighPriority() {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withRpcPriority(RpcPriority.HIGH));
    }

    abstract Boolean getBatching();

    @Override
    public PCollection<Struct> expand(PCollection<ReadOperation> input) {

      if (PCollection.IsBounded.UNBOUNDED == input.isBounded()) {
        // Warn that SpannerIO.ReadAll should not be used on unbounded inputs.
        LOG.warn(
            "SpannerIO.ReadAll({}) is being applied to an unbounded input. "
                + "This is not supported and can lead to runtime failures.",
            this.getName());
      }

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

    /** Helper function to create ServiceCallMetrics. */
    static ServiceCallMetric buildServiceCallMetricForReadOp(
        SpannerConfig config, ReadOperation op) {

      HashMap<String, String> baseLabels = buildServiceCallMetricLabels(config);
      baseLabels.put(MonitoringInfoConstants.Labels.METHOD, "Read");

      if (op.getQuery() != null) {
        String queryName = op.getQueryName();
        if (queryName == null || queryName.isEmpty()) {
          // if queryName is not specified, use a hash of the SQL statement string.
          queryName = String.format("UNNAMED_QUERY#%08x", op.getQuery().getSql().hashCode());
        }

        baseLabels.put(
            MonitoringInfoConstants.Labels.RESOURCE,
            GcpResourceIdentifiers.spannerQuery(
                baseLabels.get(MonitoringInfoConstants.Labels.SPANNER_PROJECT_ID),
                config.getInstanceId().get(),
                config.getDatabaseId().get(),
                queryName));
        baseLabels.put(MonitoringInfoConstants.Labels.SPANNER_QUERY_NAME, queryName);
      } else {
        baseLabels.put(
            MonitoringInfoConstants.Labels.RESOURCE,
            GcpResourceIdentifiers.spannerTable(
                baseLabels.get(MonitoringInfoConstants.Labels.SPANNER_PROJECT_ID),
                config.getInstanceId().get(),
                config.getDatabaseId().get(),
                op.getTable()));
        baseLabels.put(MonitoringInfoConstants.Labels.TABLE_ID, op.getTable());
      }
      return new ServiceCallMetric(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, baseLabels);
    }
  }

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<Struct>> {

    interface ToBeamRowFunction
        extends SerializableFunction<Schema, SerializableFunction<Struct, Row>> {}

    interface FromBeamRowFunction
        extends SerializableFunction<Schema, SerializableFunction<Row, Struct>> {}

    abstract SpannerConfig getSpannerConfig();

    abstract ReadOperation getReadOperation();

    abstract @Nullable TimestampBound getTimestampBound();

    abstract @Nullable PCollectionView<Transaction> getTransaction();

    abstract @Nullable PartitionOptions getPartitionOptions();

    abstract Boolean getBatching();

    abstract @Nullable TypeDescriptor<Struct> getTypeDescriptor();

    abstract @Nullable ToBeamRowFunction getToBeamRowFn();

    abstract @Nullable FromBeamRowFunction getFromBeamRowFn();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setSpannerConfig(SpannerConfig spannerConfig);

      abstract Builder setReadOperation(ReadOperation readOperation);

      abstract Builder setTimestampBound(TimestampBound timestampBound);

      abstract Builder setTransaction(PCollectionView<Transaction> transaction);

      abstract Builder setPartitionOptions(PartitionOptions partitionOptions);

      abstract Builder setBatching(Boolean batching);

      abstract Builder setTypeDescriptor(TypeDescriptor<Struct> typeDescriptor);

      abstract Builder setToBeamRowFn(ToBeamRowFunction toRowFn);

      abstract Builder setFromBeamRowFn(FromBeamRowFunction fromRowFn);

      abstract Read build();
    }

    public Read withBeamRowConverters(
        TypeDescriptor<Struct> typeDescriptor,
        ToBeamRowFunction toRowFn,
        FromBeamRowFunction fromRowFn) {
      return toBuilder()
          .setTypeDescriptor(typeDescriptor)
          .setToBeamRowFn(toRowFn)
          .setFromBeamRowFn(fromRowFn)
          .build();
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

    /** Specifies the Cloud Spanner emulator host. */
    public Read withEmulatorHost(ValueProvider<String> emulatorHost) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withEmulatorHost(emulatorHost));
    }

    public Read withEmulatorHost(String emulatorHost) {
      return withEmulatorHost(ValueProvider.StaticValueProvider.of(emulatorHost));
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

    public Read withQueryName(String queryName) {
      return withReadOperation(getReadOperation().withQueryName(queryName));
    }

    public Read withKeySet(KeySet keySet) {
      return withReadOperation(getReadOperation().withKeySet(keySet));
    }

    public Read withIndex(String index) {
      return withReadOperation(getReadOperation().withIndex(index));
    }

    /**
     * Note that {@link PartitionOptions} are currently ignored. See <a
     * href="https://cloud.google.com/spanner/docs/reference/rpc/google.spanner.v1#google.spanner.v1.PartitionOptions">
     * PartitionOptions in RPC documents</a>
     */
    public Read withPartitionOptions(PartitionOptions partitionOptions) {
      return withReadOperation(getReadOperation().withPartitionOptions(partitionOptions));
    }

    public Read withLowPriority() {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withRpcPriority(RpcPriority.LOW));
    }

    public Read withHighPriority() {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withRpcPriority(RpcPriority.HIGH));
    }

    private SpannerSourceDef createSourceDef() {
      if (getReadOperation().getQuery() != null) {
        return SpannerQuerySourceDef.create(getSpannerConfig(), getReadOperation().getQuery());
      }
      return SpannerTableSourceDef.create(
          getSpannerConfig(), getReadOperation().getTable(), getReadOperation().getColumns());
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
        if (getReadOperation().getTable() != null) {
          throw new IllegalArgumentException(
              "Both query and table cannot be specified at the same time for SpannerIO.read().");
        }
      } else if (getReadOperation().getTable() != null) {
        // Assume read
        checkNotNull(
            getReadOperation().getColumns(),
            "For a read operation SpannerIO.read() requires a list of "
                + "columns to set with withColumns method");
        checkArgument(
            !getReadOperation().getColumns().isEmpty(),
            "For a read operation SpannerIO.read() requires a non-empty"
                + " list of columns to set with withColumns method");
      } else {
        throw new IllegalArgumentException(
            "SpannerIO.read() requires query OR table to set with withTable OR withQuery method.");
      }

      final SpannerSourceDef sourceDef = createSourceDef();

      Schema beamSchema = null;
      if (getTypeDescriptor() != null && getToBeamRowFn() != null && getFromBeamRowFn() != null) {
        beamSchema = sourceDef.getBeamSchema();
      }

      ReadAll readAll =
          readAll()
              .withSpannerConfig(getSpannerConfig())
              .withTimestampBound(getTimestampBound())
              .withBatching(getBatching())
              .withTransaction(getTransaction());

      PCollection<Struct> rows =
          input.apply(Create.of(getReadOperation())).apply("Execute query", readAll);

      if (beamSchema != null) {
        rows.setSchema(
            beamSchema,
            getTypeDescriptor(),
            getToBeamRowFn().apply(beamSchema),
            getFromBeamRowFn().apply(beamSchema));
      }

      return rows;
    }

    SerializableFunction<Struct, Row> getFormatFn() {
      return (SerializableFunction<Struct, Row>)
          input ->
              Row.withSchema(Schema.builder().addInt64Field("Key").build())
                  .withFieldValue("Key", 3L)
                  .build();
    }
  }

  static class ReadRows extends PTransform<PBegin, PCollection<Row>> {

    Read read;
    Schema schema;

    public ReadRows(Read read, Schema schema) {
      super("Read rows");
      this.read = read;
      this.schema = schema;
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
      return input
          .apply(read)
          .apply(
              MapElements.into(TypeDescriptor.of(Row.class))
                  .via(
                      (SerializableFunction<Struct, Row>)
                          struct -> StructUtils.structToBeamRow(struct, schema)))
          .setRowSchema(schema);
    }
  }

  /**
   * A {@link PTransform} that create a transaction. If applied to a {@link PCollection}, it will
   * create a transaction after the {@link PCollection} is closed.
   *
   * @see SpannerIO
   * @see Wait
   */
  @AutoValue
  public abstract static class CreateTransaction
      extends PTransform<PInput, PCollectionView<Transaction>> {

    abstract SpannerConfig getSpannerConfig();

    abstract @Nullable TimestampBound getTimestampBound();

    abstract Builder toBuilder();

    @Override
    public PCollectionView<Transaction> expand(PInput input) {
      getSpannerConfig().validate();

      PCollection<?> collection = input.getPipeline().apply(Create.of(1));

      if (input instanceof PCollection) {
        collection = collection.apply(Wait.on((PCollection<?>) input));
      } else if (!(input instanceof PBegin)) {
        throw new RuntimeException("input must be PBegin or PCollection");
      }

      return collection
          .apply(
              "Create transaction",
              ParDo.of(new CreateTransactionFn(this.getSpannerConfig(), this.getTimestampBound())))
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

    /** Specifies the Cloud Spanner emulator host. */
    public CreateTransaction withEmulatorHost(ValueProvider<String> emulatorHost) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withEmulatorHost(emulatorHost));
    }

    public CreateTransaction withEmulatorHost(String emulatorHost) {
      return withEmulatorHost(ValueProvider.StaticValueProvider.of(emulatorHost));
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

    abstract @Nullable PCollectionView<Dialect> getDialectView();

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

      abstract Builder setDialectView(PCollectionView<Dialect> dialect);

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

    /** Specifies the Cloud Spanner emulator host. */
    public Write withEmulatorHost(ValueProvider<String> emulatorHost) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withEmulatorHost(emulatorHost));
    }

    public Write withEmulatorHost(String emulatorHost) {
      return withEmulatorHost(ValueProvider.StaticValueProvider.of(emulatorHost));
    }

    public Write withDialectView(PCollectionView<Dialect> dialect) {
      return toBuilder().setDialectView(dialect).build();
    }

    /**
     * Specifies the deadline for the Commit API call. Default is 15 secs. DEADLINE_EXCEEDED errors
     * will prompt a backoff/retry until the value of {@link #withMaxCumulativeBackoff(Duration)} is
     * reached. DEADLINE_EXCEEDED errors are reported with logging and counters.
     */
    public Write withCommitDeadline(Duration commitDeadline) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withCommitDeadline(commitDeadline));
    }

    /**
     * Specifies max commit delay for the Commit API call for throughput optimized writes. If not
     * set, Spanner might set a small delay if it thinks that will amortize the cost of the writes.
     * For more information about the feature, <a
     * href="https://cloud.google.com/spanner/docs/throughput-optimized-writes#default-behavior">see
     * documentation</a>
     */
    public Write withMaxCommitDelay(long millis) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withMaxCommitDelay(millis));
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

    public Write withLowPriority() {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withRpcPriority(RpcPriority.LOW));
    }

    public Write withHighPriority() {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withRpcPriority(RpcPriority.HIGH));
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

  static class WriteRows extends PTransform<PCollection<Row>, PDone> {

    private final Write write;
    private final Op operation;
    private final String table;

    private WriteRows(Write write, Op operation, String table) {
      this.write = write;
      this.operation = operation;
      this.table = table;
    }

    public static WriteRows of(Write write, Op operation, String table) {
      return new WriteRows(write, operation, table);
    }

    @Override
    public PDone expand(PCollection<Row> input) {
      input
          .apply(
              MapElements.into(TypeDescriptor.of(Mutation.class))
                  .via(MutationUtils.beamRowToMutationFn(operation, table)))
          .apply(write);
      return PDone.in(input.getPipeline());
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
      PCollectionView<Dialect> dialectView = spec.getDialectView();

      if (dialectView == null) {
        dialectView =
            input
                .getPipeline()
                .apply("CreateSingleton", Create.of(Dialect.GOOGLE_STANDARD_SQL))
                .apply("As PCollectionView", View.asSingleton());
      }

      if (spec.getBatchSizeBytes() <= 1
          || spec.getMaxNumMutations() <= 1
          || spec.getMaxNumRows() <= 1) {
        LOG.info("Batching of mutationGroups is disabled");
        TypeDescriptor<Iterable<MutationGroup>> descriptor =
            new TypeDescriptor<Iterable<MutationGroup>>() {};
        batches = input.apply(MapElements.into(descriptor).via(ImmutableList::of));
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
                    ParDo.of(new ReadSpannerSchema(spec.getSpannerConfig(), dialectView))
                        .withSideInputs(dialectView))
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

  @AutoValue
  public abstract static class ReadChangeStream
      extends PTransform<PBegin, PCollection<DataChangeRecord>> {

    abstract SpannerConfig getSpannerConfig();

    abstract String getChangeStreamName();

    abstract @Nullable String getMetadataInstance();

    abstract @Nullable String getMetadataDatabase();

    abstract @Nullable String getMetadataTable();

    abstract Timestamp getInclusiveStartAt();

    abstract @Nullable Timestamp getInclusiveEndAt();

    abstract @Nullable RpcPriority getRpcPriority();

    /** @deprecated This configuration has no effect, as tracing is not available */
    @Deprecated
    abstract @Nullable Double getTraceSampleProbability();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setSpannerConfig(SpannerConfig spannerConfig);

      abstract Builder setChangeStreamName(String changeStreamName);

      abstract Builder setMetadataInstance(String metadataInstance);

      abstract Builder setMetadataDatabase(String metadataDatabase);

      abstract Builder setMetadataTable(String metadataTable);

      abstract Builder setInclusiveStartAt(Timestamp inclusiveStartAt);

      abstract Builder setInclusiveEndAt(Timestamp inclusiveEndAt);

      abstract Builder setRpcPriority(RpcPriority rpcPriority);

      abstract Builder setTraceSampleProbability(Double probability);

      abstract ReadChangeStream build();
    }

    /** Specifies the Cloud Spanner configuration. */
    public ReadChangeStream withSpannerConfig(SpannerConfig spannerConfig) {
      return toBuilder().setSpannerConfig(spannerConfig).build();
    }

    /** Specifies the Cloud Spanner project. */
    public ReadChangeStream withProjectId(String projectId) {
      return withProjectId(ValueProvider.StaticValueProvider.of(projectId));
    }

    /** Specifies the Cloud Spanner project. */
    public ReadChangeStream withProjectId(ValueProvider<String> projectId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withProjectId(projectId));
    }

    /** Specifies the Cloud Spanner instance. */
    public ReadChangeStream withInstanceId(String instanceId) {
      return withInstanceId(ValueProvider.StaticValueProvider.of(instanceId));
    }

    /** Specifies the Cloud Spanner instance. */
    public ReadChangeStream withInstanceId(ValueProvider<String> instanceId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withInstanceId(instanceId));
    }

    /** Specifies the Cloud Spanner database. */
    public ReadChangeStream withDatabaseId(String databaseId) {
      return withDatabaseId(ValueProvider.StaticValueProvider.of(databaseId));
    }

    /** Specifies the Cloud Spanner database. */
    public ReadChangeStream withDatabaseId(ValueProvider<String> databaseId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withDatabaseId(databaseId));
    }

    /** Specifies the change stream name. */
    public ReadChangeStream withChangeStreamName(String changeStreamName) {
      return toBuilder().setChangeStreamName(changeStreamName).build();
    }

    /** Specifies the metadata database. */
    public ReadChangeStream withMetadataInstance(String metadataInstance) {
      return toBuilder().setMetadataInstance(metadataInstance).build();
    }

    /** Specifies the metadata database. */
    public ReadChangeStream withMetadataDatabase(String metadataDatabase) {
      return toBuilder().setMetadataDatabase(metadataDatabase).build();
    }

    /** Specifies the metadata table name. */
    public ReadChangeStream withMetadataTable(String metadataTable) {
      return toBuilder().setMetadataTable(metadataTable).build();
    }

    /** Specifies the time that the change stream should be read from. */
    public ReadChangeStream withInclusiveStartAt(Timestamp timestamp) {
      return toBuilder().setInclusiveStartAt(timestamp).build();
    }

    /** Specifies the end time of the change stream. */
    public ReadChangeStream withInclusiveEndAt(Timestamp timestamp) {
      return toBuilder().setInclusiveEndAt(timestamp).build();
    }

    /** Specifies the priority of the change stream queries. */
    public ReadChangeStream withRpcPriority(RpcPriority rpcPriority) {
      return toBuilder().setRpcPriority(rpcPriority).build();
    }

    /**
     * Specifies the sample probability of tracing requests.
     *
     * @deprecated This configuration has no effect, as tracing is not available.
     */
    @Deprecated
    public ReadChangeStream withTraceSampleProbability(Double probability) {
      return toBuilder().setTraceSampleProbability(probability).build();
    }

    @Override
    public PCollection<DataChangeRecord> expand(PBegin input) {
      checkArgument(
          getSpannerConfig() != null,
          "SpannerIO.readChangeStream() requires the spanner config to be set.");
      checkArgument(
          getSpannerConfig().getProjectId() != null,
          "SpannerIO.readChangeStream() requires the project ID to be set.");
      checkArgument(
          getSpannerConfig().getInstanceId() != null,
          "SpannerIO.readChangeStream() requires the instance ID to be set.");
      checkArgument(
          getSpannerConfig().getDatabaseId() != null,
          "SpannerIO.readChangeStream() requires the database ID to be set.");
      checkArgument(
          getChangeStreamName() != null,
          "SpannerIO.readChangeStream() requires the name of the change stream to be set.");
      checkArgument(
          getInclusiveStartAt() != null,
          "SpannerIO.readChangeStream() requires the start time to be set.");
      // Inclusive end at is defaulted to ChangeStreamsContants.MAX_INCLUSIVE_END_AT
      checkArgument(
          getInclusiveEndAt() != null,
          "SpannerIO.readChangeStream() requires the end time to be set. If you'd like to process the stream without an end time, you can omit this parameter.");
      if (getMetadataInstance() != null) {
        checkArgument(
            getMetadataDatabase() != null,
            "SpannerIO.readChangeStream() requires the metadata database to be set if metadata instance is set.");
      }

      // Start time must be before end time
      if (getInclusiveEndAt() != null
          && getInclusiveStartAt().toSqlTimestamp().after(getInclusiveEndAt().toSqlTimestamp())) {
        throw new IllegalArgumentException("Start time cannot be after end time.");
      }

      final DatabaseId changeStreamDatabaseId =
          DatabaseId.of(
              getSpannerConfig().getProjectId().get(),
              getSpannerConfig().getInstanceId().get(),
              getSpannerConfig().getDatabaseId().get());
      final String partitionMetadataInstanceId =
          MoreObjects.firstNonNull(
              getMetadataInstance(), changeStreamDatabaseId.getInstanceId().getInstance());
      final String partitionMetadataDatabaseId =
          MoreObjects.firstNonNull(getMetadataDatabase(), changeStreamDatabaseId.getDatabase());
      final DatabaseId fullPartitionMetadataDatabaseId =
          DatabaseId.of(
              getSpannerConfig().getProjectId().get(),
              partitionMetadataInstanceId,
              partitionMetadataDatabaseId);

      final SpannerConfig changeStreamSpannerConfig = buildChangeStreamSpannerConfig();
      final SpannerConfig partitionMetadataSpannerConfig =
          MetadataSpannerConfigFactory.create(
              changeStreamSpannerConfig, partitionMetadataInstanceId, partitionMetadataDatabaseId);
      final Dialect changeStreamDatabaseDialect =
          getDialect(changeStreamSpannerConfig, input.getPipeline().getOptions());
      final Dialect metadataDatabaseDialect =
          getDialect(partitionMetadataSpannerConfig, input.getPipeline().getOptions());
      LOG.info(
          "The Spanner database "
              + changeStreamDatabaseId
              + " has dialect "
              + changeStreamDatabaseDialect);
      LOG.info(
          "The Spanner database "
              + fullPartitionMetadataDatabaseId
              + " has dialect "
              + metadataDatabaseDialect);
      final String partitionMetadataTableName =
          MoreObjects.firstNonNull(
              getMetadataTable(), generatePartitionMetadataTableName(partitionMetadataDatabaseId));
      final String changeStreamName = getChangeStreamName();
      final Timestamp startTimestamp = getInclusiveStartAt();
      // Uses (Timestamp.MAX - 1ns) at max for end timestamp, because we add 1ns to transform the
      // interval into a closed-open in the read change stream restriction (prevents overflow)
      final Timestamp endTimestamp =
          getInclusiveEndAt().compareTo(MAX_INCLUSIVE_END_AT) > 0
              ? MAX_INCLUSIVE_END_AT
              : getInclusiveEndAt();
      final MapperFactory mapperFactory = new MapperFactory(changeStreamDatabaseDialect);
      final ChangeStreamMetrics metrics = new ChangeStreamMetrics();
      final RpcPriority rpcPriority = MoreObjects.firstNonNull(getRpcPriority(), RpcPriority.HIGH);
      final DaoFactory daoFactory =
          new DaoFactory(
              changeStreamSpannerConfig,
              changeStreamName,
              partitionMetadataSpannerConfig,
              partitionMetadataTableName,
              rpcPriority,
              input.getPipeline().getOptions().getJobName(),
              changeStreamDatabaseDialect,
              metadataDatabaseDialect);
      final ActionFactory actionFactory = new ActionFactory();

      final InitializeDoFn initializeDoFn =
          new InitializeDoFn(daoFactory, mapperFactory, startTimestamp, endTimestamp);
      final DetectNewPartitionsDoFn detectNewPartitionsDoFn =
          new DetectNewPartitionsDoFn(daoFactory, mapperFactory, actionFactory, metrics);
      final ReadChangeStreamPartitionDoFn readChangeStreamPartitionDoFn =
          new ReadChangeStreamPartitionDoFn(daoFactory, mapperFactory, actionFactory, metrics);
      final PostProcessingMetricsDoFn postProcessingMetricsDoFn =
          new PostProcessingMetricsDoFn(metrics);

      LOG.info("Partition metadata table that will be used is " + partitionMetadataTableName);

      final PCollection<byte[]> impulseOut = input.apply(Impulse.create());
      final PCollection<PartitionMetadata> partitionsOut =
          impulseOut
              .apply("Initialize the connector", ParDo.of(initializeDoFn))
              .apply("Detect new partitions", ParDo.of(detectNewPartitionsDoFn));
      final Coder<PartitionMetadata> partitionMetadataCoder = partitionsOut.getCoder();
      final SizeEstimator<PartitionMetadata> partitionMetadataSizeEstimator =
          new SizeEstimator<>(partitionMetadataCoder);
      final long averagePartitionBytesSize =
          partitionMetadataSizeEstimator.sizeOf(ChangeStreamsConstants.SAMPLE_PARTITION);
      detectNewPartitionsDoFn.setAveragePartitionBytesSize(averagePartitionBytesSize);

      final PCollection<DataChangeRecord> dataChangeRecordsOut =
          partitionsOut
              .apply("Read change stream partition", ParDo.of(readChangeStreamPartitionDoFn))
              .apply("Gather metrics", ParDo.of(postProcessingMetricsDoFn));
      final Coder<DataChangeRecord> dataChangeRecordCoder = dataChangeRecordsOut.getCoder();
      final SizeEstimator<DataChangeRecord> dataChangeRecordSizeEstimator =
          new SizeEstimator<>(dataChangeRecordCoder);
      final BytesThroughputEstimator<DataChangeRecord> throughputEstimator =
          new BytesThroughputEstimator<>(THROUGHPUT_WINDOW_SECONDS, dataChangeRecordSizeEstimator);
      readChangeStreamPartitionDoFn.setThroughputEstimator(throughputEstimator);

      impulseOut
          .apply(WithTimestamps.of(e -> GlobalWindow.INSTANCE.maxTimestamp()))
          .apply(Wait.on(dataChangeRecordsOut))
          .apply(ParDo.of(new CleanUpReadChangeStreamDoFn(daoFactory)));
      return dataChangeRecordsOut;
    }

    @VisibleForTesting
    SpannerConfig buildChangeStreamSpannerConfig() {
      SpannerConfig changeStreamSpannerConfig = getSpannerConfig();
      // Set default retryable errors for ReadChangeStream
      if (changeStreamSpannerConfig.getRetryableCodes() == null) {
        ImmutableSet<Code> defaultRetryableCodes = ImmutableSet.of(Code.UNAVAILABLE, Code.ABORTED);
        changeStreamSpannerConfig =
            changeStreamSpannerConfig.toBuilder().setRetryableCodes(defaultRetryableCodes).build();
      }
      // Set default retry timeouts for ReadChangeStream
      if (changeStreamSpannerConfig.getExecuteStreamingSqlRetrySettings() == null) {
        changeStreamSpannerConfig =
            changeStreamSpannerConfig
                .toBuilder()
                .setExecuteStreamingSqlRetrySettings(
                    RetrySettings.newBuilder()
                        .setTotalTimeout(org.threeten.bp.Duration.ofMinutes(5))
                        .setInitialRpcTimeout(org.threeten.bp.Duration.ofMinutes(1))
                        .setMaxRpcTimeout(org.threeten.bp.Duration.ofMinutes(1))
                        .build())
                .build();
      }
      return changeStreamSpannerConfig;
    }
  }

  /** If credentials are not set in spannerConfig, uses the credentials from pipeline options. */
  @VisibleForTesting
  static SpannerConfig buildSpannerConfigWithCredential(
      SpannerConfig spannerConfig, PipelineOptions pipelineOptions) {
    if (spannerConfig.getCredentials() == null && pipelineOptions != null) {
      final Credentials credentials = pipelineOptions.as(GcpOptions.class).getGcpCredential();
      if (credentials != null) {
        spannerConfig = spannerConfig.withCredentials(credentials);
      }
    }
    return spannerConfig;
  }

  private static Dialect getDialect(SpannerConfig spannerConfig, PipelineOptions pipelineOptions) {
    // Allow passing the credential from pipeline options to the getDialect() call.
    SpannerConfig spannerConfigWithCredential =
        buildSpannerConfigWithCredential(spannerConfig, pipelineOptions);
    DatabaseClient databaseClient =
        SpannerAccessor.getOrCreate(spannerConfigWithCredential).getDatabaseClient();
    return databaseClient.getDialect();
  }

  /**
   * Interface to display the name of the metadata table on Dataflow UI. This is only used for
   * internal purpose. This should not be used to pass the name of the metadata table.
   */
  public interface SpannerChangeStreamOptions extends StreamingOptions {

    /** Returns the name of the metadata table. */
    String getMetadataTable();

    /** Specifies the name of the metadata table. */
    void setMetadataTable(String table);
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
              .collect(toList()));
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

    // TODO(https://github.com/apache/beam/issues/18203): Remove this when FinishBundle has added
    // support for an {@link OutputReceiver}
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

    @ProcessElement
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
    private final String schemaChangeErrString =
        "Transaction aborted. "
            + "Database schema probably changed during transaction, retry may succeed.";

    /* Error string in Aborted exception for concurrent transaction in Spanner Emulator */
    private final String emulatorErrorString =
        "The emulator only supports one transaction at a time.";

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
    private transient LoadingCache<String, ServiceCallMetric> writeMetricsByTableName;

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

      // Use a LoadingCache for metrics as there can be different tables being written to which
      // result in different service call metrics labels. ServiceCallMetric items are created
      // on-demand and added to the cache.
      writeMetricsByTableName =
          CacheBuilder.newBuilder()
              .maximumSize(METRICS_CACHE_SIZE)
              .build(
                  new CacheLoader<String, ServiceCallMetric>() {
                    @Override
                    public ServiceCallMetric load(String tableName) {
                      return buildWriteServiceCallMetric(spannerConfig, tableName);
                    }
                  });
    }

    @Teardown
    public void teardown() {
      spannerAccessor.close();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      List<MutationGroup> mutations = ImmutableList.copyOf(c.element());

      // Batch upsert rows.
      try {
        mutationGroupBatchesReceived.inc();
        mutationGroupsReceived.inc(mutations.size());
        Iterable<Mutation> batch = Iterables.concat(mutations);
        writeMutations(batch);
        mutationGroupBatchesWriteSuccess.inc();
        mutationGroupsWriteSuccess.inc(mutations.size());
        return;
      } catch (SpannerException e) {
        mutationGroupBatchesWriteFail.inc();
        if (failureMode == FailureMode.REPORT_FAILURES) {
          // fall through and retry individual mutationGroups.
        } else if (failureMode == FailureMode.FAIL_FAST) {
          mutationGroupsWriteFail.inc(mutations.size());
          LOG.error("Failed to write a batch of mutation groups", e);
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
    private void spannerWriteWithRetryIfSchemaChange(List<Mutation> batch) throws SpannerException {
      for (int retry = 1; ; retry++) {
        try {
          spannerAccessor
              .getDatabaseClient()
              .writeAtLeastOnceWithOptions(batch, getTransactionOptions());
          reportServiceCallMetricsForBatch(batch, "ok");
          return;
        } catch (AbortedException e) {
          reportServiceCallMetricsForBatch(batch, e.getErrorCode().getGrpcStatusCode().toString());
          if (retry >= ABORTED_RETRY_ATTEMPTS) {
            throw e;
          }
          if (e.isRetryable()
              || e.getMessage().contains(schemaChangeErrString)
              || e.getMessage().contains(emulatorErrorString)) {
            continue;
          }
          throw e;
        } catch (SpannerException e) {
          reportServiceCallMetricsForBatch(batch, e.getErrorCode().getGrpcStatusCode().toString());
          throw e;
        }
      }
    }

    private Options.TransactionOption[] getTransactionOptions() {
      return Stream.of(
              spannerConfig.getRpcPriority() != null && spannerConfig.getRpcPriority().get() != null
                  ? Options.priority(spannerConfig.getRpcPriority().get())
                  : null,
              spannerConfig.getMaxCommitDelay() != null
                      && spannerConfig.getMaxCommitDelay().get() != null
                  ? Options.maxCommitDelay(
                      java.time.Duration.ofMillis(
                          spannerConfig.getMaxCommitDelay().get().getMillis()))
                  : null)
          .filter(Objects::nonNull)
          .toArray(Options.TransactionOption[]::new);
    }

    private void reportServiceCallMetricsForBatch(List<Mutation> batch, String statusCode) {
      // Get names of all tables in batch of mutations.
      Set<String> tableNames = batch.stream().map(Mutation::getTable).collect(Collectors.toSet());
      for (String tableName : tableNames) {
        writeMetricsByTableName.getUnchecked(tableName).call(statusCode);
      }
    }

    private static ServiceCallMetric buildWriteServiceCallMetric(
        SpannerConfig config, String tableId) {
      HashMap<String, String> baseLabels = buildServiceCallMetricLabels(config);
      baseLabels.put(MonitoringInfoConstants.Labels.METHOD, "Write");
      baseLabels.put(
          MonitoringInfoConstants.Labels.RESOURCE,
          GcpResourceIdentifiers.spannerTable(
              baseLabels.get(MonitoringInfoConstants.Labels.SPANNER_PROJECT_ID),
              config.getInstanceId().get(),
              config.getDatabaseId().get(),
              tableId));
      baseLabels.put(MonitoringInfoConstants.Labels.TABLE_ID, tableId);
      return new ServiceCallMetric(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, baseLabels);
    }

    /** Write the Mutations to Spanner, handling DEADLINE_EXCEEDED with backoff/retries. */
    private void writeMutations(Iterable<Mutation> mutationIterable)
        throws SpannerException, IOException {
      BackOff backoff = bundleWriteBackoff.backoff();
      List<Mutation> mutations = ImmutableList.copyOf(mutationIterable);

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
                  mutations.size());
              spannerWriteFail.inc();
              throw exception;
            }
            LOG.info(
                "DEADLINE_EXCEEDED writing batch of {} mutations to Cloud Spanner, "
                    + "retrying after backoff of {}ms\n"
                    + "({})",
                mutations.size(),
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

  private static HashMap<String, String> buildServiceCallMetricLabels(SpannerConfig config) {
    HashMap<String, String> baseLabels = new HashMap<>();
    baseLabels.put(MonitoringInfoConstants.Labels.PTRANSFORM, "");
    baseLabels.put(MonitoringInfoConstants.Labels.SERVICE, "Spanner");
    baseLabels.put(
        MonitoringInfoConstants.Labels.SPANNER_PROJECT_ID,
        config.getProjectId() == null
                || config.getProjectId().get() == null
                || config.getProjectId().get().isEmpty()
            ? SpannerOptions.getDefaultProjectId()
            : config.getProjectId().get());
    baseLabels.put(
        MonitoringInfoConstants.Labels.SPANNER_INSTANCE_ID, config.getInstanceId().get());
    baseLabels.put(
        MonitoringInfoConstants.Labels.SPANNER_DATABASE_ID, config.getDatabaseId().get());
    return baseLabels;
  }
}
