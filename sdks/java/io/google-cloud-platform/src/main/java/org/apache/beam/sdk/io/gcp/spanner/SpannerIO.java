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
import static org.apache.beam.sdk.io.gcp.spanner.SpannerIO.WriteGrouped.decode;
import static org.apache.beam.sdk.io.gcp.spanner.SpannerIO.WriteGrouped.encode;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.ServiceFactory;
import com.google.cloud.Timestamp;
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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v20_0.com.google.common.primitives.UnsignedBytes;
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
 * PCollectionView<Transaction> tx =
 * p.apply(
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
 * The {@link SpannerWriteResult SpannerWriteResult} object contains the results of the transform,
 * including a {@link PCollection} of MutationGroups that failed to write, and a {@link PCollection}
 * that can be used in batch pipelines as a completion signal to {@link
 * org.apache.beam.sdk.transforms.Wait Wait.OnSignal} to indicate when all input has been written.
 * Note that in streaming pipelines, this signal will never be triggered as the input is unbounded
 * and this {@link PCollection} is using the {@link GlobalWindow}.
 *
 * <h3>Batching</h3>
 *
 * <p>To reduce the number of transactions sent to Spanner, the {@link Mutation Mutations} are
 * grouped into batches The default maximum size of the batch is set to 1MB or 5000 mutated cells.
 * To override this use {@link Write#withBatchSizeBytes(long) withBatchSizeBytes()} and {@link
 * Write#withMaxNumMutations(long) withMaxNumMutations()}. Setting either to a small value or zero
 * disables batching.
 *
 * <p>Note that the <a
 * href="https://cloud.google.com/spanner/quotas#limits_for_creating_reading_updating_and_deleting_data">maximum
 * size of a single transaction</a> is 20,000 mutated cells - including cells in indexes. If you
 * have a large number of indexes and are getting exceptions with message: <tt>INVALID_ARGUMENT: The
 * transaction contains too many mutations</tt> you will need to specify a smaller number of {@code
 * MaxNumMutations}.
 *
 * <p>The batches written are obtained from by grouping enough {@link Mutation Mutations} from the
 * Bundle provided by Beam to form (by default) 1000 batches. This group of {@link Mutation
 * Mutations} is then sorted by Key, and the batches are created from the sorted group. This so that
 * each batch will have keys that are 'close' to each other to optimise write performance. This
 * grouping factor (number of batches) is controlled by the parameter {@link
 * Write#withGroupingFactor(int) withGroupingFactor()}.<br>
 * Note that each worker will need enough memory to hold {@code GroupingFactor x MaxBatchSizeBytes}
 * Mutations, so if you have a large {@code MaxBatchSize} you may need to reduce {@code
 * GroupingFactor}
 *
 * <h3>Database Schema Preparation</h3>
 *
 * <p>The Write transform reads the database schema on pipeline start. If the schema is created as
 * part of the same pipeline, this transform needs to wait until this has happened. Use {@link
 * Write#withSchemaReadySignal(PCollection)} to pass a signal {@link PCollection} which will be used
 * with {@link Wait.OnSignal} to prevent the schema from being read until it is ready. The Write
 * transform will be paused until the signal {@link PCollection} is closed.
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
@Experimental(Experimental.Kind.SOURCE_SINK)
public class SpannerIO {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerIO.class);

  private static final long DEFAULT_BATCH_SIZE_BYTES = 1024L * 1024L; // 1 MB
  // Max number of mutations to batch together.
  private static final int DEFAULT_MAX_NUM_MUTATIONS = 5000;
  // Multiple of mutation size to use to gather and sort mutations
  private static final int DEFAULT_GROUPING_FACTOR = 1000;

  /**
   * Creates an uninitialized instance of {@link Read}. Before use, the {@link Read} must be
   * configured with a {@link Read#withInstanceId} and {@link Read#withDatabaseId} that identify the
   * Cloud Spanner database.
   */
  @Experimental(Experimental.Kind.SOURCE_SINK)
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
  @Experimental(Experimental.Kind.SOURCE_SINK)
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
        .setGroupingFactor(DEFAULT_GROUPING_FACTOR)
        .setFailureMode(FailureMode.FAIL_FAST)
        .build();
  }

  /** Implementation of {@link #readAll}. */
  @Experimental(Experimental.Kind.SOURCE_SINK)
  @AutoValue
  public abstract static class ReadAll
      extends PTransform<PCollection<ReadOperation>, PCollection<Struct>> {

    abstract SpannerConfig getSpannerConfig();

    @Nullable
    abstract PCollectionView<Transaction> getTransaction();

    @Nullable
    abstract TimestampBound getTimestampBound();

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
  @Experimental(Experimental.Kind.SOURCE_SINK)
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<Struct>> {

    abstract SpannerConfig getSpannerConfig();

    abstract ReadOperation getReadOperation();

    @Nullable
    abstract TimestampBound getTimestampBound();

    @Nullable
    abstract PCollectionView<Transaction> getTransaction();

    @Nullable
    abstract PartitionOptions getPartitionOptions();

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
  @Experimental(Experimental.Kind.SOURCE_SINK)
  @AutoValue
  public abstract static class CreateTransaction
      extends PTransform<PBegin, PCollectionView<Transaction>> {

    abstract SpannerConfig getSpannerConfig();

    @Nullable
    abstract TimestampBound getTimestampBound();

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
  @Experimental(Experimental.Kind.SOURCE_SINK)
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<Mutation>, SpannerWriteResult> {

    abstract SpannerConfig getSpannerConfig();

    abstract long getBatchSizeBytes();

    abstract long getMaxNumMutations();

    abstract FailureMode getFailureMode();

    @Nullable
    abstract PCollection getSchemaReadySignal();

    abstract int getGroupingFactor();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setSpannerConfig(SpannerConfig spannerConfig);

      abstract Builder setBatchSizeBytes(long batchSizeBytes);

      abstract Builder setMaxNumMutations(long maxNumMutations);

      abstract Builder setFailureMode(FailureMode failureMode);

      abstract Builder setSchemaReadySignal(PCollection schemaReadySignal);

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
     * Specifies an optional input PCollection that can be used as the signal for {@link
     * Wait.OnSignal} to indicate when the database schema is ready to be read.
     *
     * <p>To be used when the database schema is created by another section of the pipeline, this
     * causes this transform to wait until the {@code signal PCollection} has been closed before
     * reading the schema from the database.
     *
     * @see Wait.OnSignal
     */
    public Write withSchemaReadySignal(PCollection signal) {
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
      getSpannerConfig().populateDisplayData(builder);
      builder.add(
          DisplayData.item("batchSizeBytes", getBatchSizeBytes()).withLabel("Batch Size in Bytes"));
    }
  }

  /**
   * A singleton to compare encoded MutationGroups by encoded Key that wraps {@code
   * UnsignedBytes#lexicographicalComparator} which unfortunately is not serializable.
   */
  private enum EncodedKvMutationGroupComparator
      implements Comparator<KV<byte[], byte[]>>, Serializable {
    INSTANCE {
      @Override
      public int compare(KV<byte[], byte[]> a, KV<byte[], byte[]> b) {
        return UnsignedBytes.lexicographicalComparator().compare(a.getKey(), b.getKey());
      }
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
    public SpannerWriteResult expand(PCollection<MutationGroup> input) {

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
              .apply("To Global Window", Window.into(new GlobalWindows()))
              .apply(
                  "Filter Unbatchable Mutations",
                  ParDo.of(
                          new BatchableMutationFilterFn(
                              schemaView,
                              UNBATCHABLE_MUTATIONS_TAG,
                              spec.getBatchSizeBytes(),
                              spec.getMaxNumMutations()))
                      .withSideInputs(schemaView)
                      .withOutputTags(
                          BATCHABLE_MUTATIONS_TAG, TupleTagList.of(UNBATCHABLE_MUTATIONS_TAG)));

      // Build a set of Mutation groups from the current bundle,
      // sort them by table/key then split into batches.
      PCollection<Iterable<MutationGroup>> batchedMutations =
          filteredMutations
              .get(BATCHABLE_MUTATIONS_TAG)
              .apply(
                  "Gather And Sort",
                  ParDo.of(
                          new GatherBundleAndSortFn(
                              spec.getBatchSizeBytes(),
                              spec.getMaxNumMutations(),
                              spec.getGroupingFactor(),
                              schemaView))
                      .withSideInputs(schemaView))
              .apply(
                  "Create Batches",
                  ParDo.of(
                          new BatchFn(
                              spec.getBatchSizeBytes(), spec.getMaxNumMutations(), schemaView))
                      .withSideInputs(schemaView));

      // Merge the batchable and unbatchable mutations and write to Spanner.
      PCollectionTuple result =
          PCollectionList.of(filteredMutations.get(UNBATCHABLE_MUTATIONS_TAG))
              .and(batchedMutations)
              .apply("Merge", Flatten.pCollections())
              .apply(
                  "Write mutations to Spanner",
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
  static class GatherBundleAndSortFn extends DoFn<MutationGroup, Iterable<KV<byte[], byte[]>>> {
    private final long maxBatchSizeBytes;
    private final long maxNumMutations;

    // total size of the current batch.
    private long batchSizeBytes;
    // total number of mutated cells including indices.
    private long batchCells;

    private final PCollectionView<SpannerSchema> schemaView;

    private transient ArrayList<KV<byte[], byte[]>> mutationsToSort = null;

    GatherBundleAndSortFn(
        long maxBatchSizeBytes,
        long maxNumMutations,
        long groupingFactor,
        PCollectionView<SpannerSchema> schemaView) {
      this.maxBatchSizeBytes = maxBatchSizeBytes * groupingFactor;
      this.maxNumMutations = maxNumMutations * groupingFactor;
      this.schemaView = schemaView;
    }

    @StartBundle
    public synchronized void startBundle() throws Exception {
      if (mutationsToSort == null) {
        initSorter();
      } else {
        throw new IllegalStateException("Sorter should be null here");
      }
    }

    private void initSorter() {
      mutationsToSort = new ArrayList<KV<byte[], byte[]>>((int) maxNumMutations);
      batchSizeBytes = 0;
      batchCells = 0;
    }

    @FinishBundle
    public synchronized void finishBundle(FinishBundleContext c) throws Exception {
      c.output(sortAndGetList(), Instant.now(), GlobalWindow.INSTANCE);
    }

    private Iterable<KV<byte[], byte[]>> sortAndGetList() throws IOException {
      mutationsToSort.sort(EncodedKvMutationGroupComparator.INSTANCE);
      ArrayList<KV<byte[], byte[]>> tmp = mutationsToSort;
      // Ensure no more mutations can be added.
      mutationsToSort = null;
      return tmp;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      SpannerSchema spannerSchema = c.sideInput(schemaView);
      MutationKeyEncoder encoder = new MutationKeyEncoder(spannerSchema);
      MutationGroup mg = c.element();
      long groupSize = MutationSizeEstimator.sizeOf(mg);
      long groupCells = MutationCellCounter.countOf(spannerSchema, mg);

      synchronized (this) {
        if (((batchCells + groupCells) > maxNumMutations)
            || (batchSizeBytes + groupSize) > maxBatchSizeBytes) {
          c.output(sortAndGetList());
          initSorter();
        }

        mutationsToSort.add(KV.of(encoder.encodeTableNameAndKey(mg.primary()), encode(mg)));
        batchSizeBytes += groupSize;
        batchCells += groupCells;
      }
    }
  }

  /** Batches mutations together. */
  @VisibleForTesting
  static class BatchFn extends DoFn<Iterable<KV<byte[], byte[]>>, Iterable<MutationGroup>> {

    private final long maxBatchSizeBytes;
    private final long maxNumMutations;
    private final PCollectionView<SpannerSchema> schemaView;

    BatchFn(
        long maxBatchSizeBytes, long maxNumMutations, PCollectionView<SpannerSchema> schemaView) {
      this.maxBatchSizeBytes = maxBatchSizeBytes;
      this.maxNumMutations = maxNumMutations;
      this.schemaView = schemaView;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      SpannerSchema spannerSchema = c.sideInput(schemaView);
      // Current batch of mutations to be written.
      ImmutableList.Builder<MutationGroup> batch = ImmutableList.builder();
      // total size of the current batch.
      long batchSizeBytes = 0;
      // total number of mutated cells including indices.
      long batchCells = 0;

      // Iterate through list, outputting whenever a batch is complete.
      for (KV<byte[], byte[]> kv : c.element()) {
        MutationGroup mg = decode(kv.getValue());

        long groupSize = MutationSizeEstimator.sizeOf(mg);
        long groupCells = MutationCellCounter.countOf(spannerSchema, mg);

        if (((batchCells + groupCells) > maxNumMutations)
            || ((batchSizeBytes + groupSize) > maxBatchSizeBytes)) {
          // Batch is full: output and reset.
          c.output(batch.build());
          batch = ImmutableList.builder();
          batchSizeBytes = 0;
          batchCells = 0;
        }
        batch.add(mg);
        batchSizeBytes += groupSize;
        batchCells += groupCells;
      }
      // End of list, output what is left.
      if (batchCells > 0) {
        c.output(batch.build());
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
    private final Counter batchableMutationGroupsCounter =
        Metrics.counter(WriteGrouped.class, "batchable_mutation_groups");
    private final Counter unBatchableMutationGroupsCounter =
        Metrics.counter(WriteGrouped.class, "unbatchable_mutation_groups");

    BatchableMutationFilterFn(
        PCollectionView<SpannerSchema> schemaView,
        TupleTag<Iterable<MutationGroup>> unbatchableMutationsTag,
        long batchSizeBytes,
        long maxNumMutations) {
      this.schemaView = schemaView;
      this.unbatchableMutationsTag = unbatchableMutationsTag;
      this.batchSizeBytes = batchSizeBytes;
      this.maxNumMutations = maxNumMutations;
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

      if (groupSize >= batchSizeBytes || groupCells >= maxNumMutations) {
        c.output(unbatchableMutationsTag, Arrays.asList(mg));
        unBatchableMutationGroupsCounter.inc();
      } else {
        c.output(mg);
        batchableMutationGroupsCounter.inc();
      }
    }
  }

  private static class WriteToSpannerFn extends DoFn<Iterable<MutationGroup>, Void> {

    private transient SpannerAccessor spannerAccessor;
    private final SpannerConfig spannerConfig;
    private final FailureMode failureMode;
    private final Counter mutationGroupBatchesCounter =
        Metrics.counter(WriteGrouped.class, "mutation_group_batches");
    private final Counter mutationGroupWriteSuccessCounter =
        Metrics.counter(WriteGrouped.class, "mutation_groups_write_success");
    private final Counter mutationGroupWriteFailCounter =
        Metrics.counter(WriteGrouped.class, "mutation_groups_write_fail");

    private final TupleTag<MutationGroup> failedTag;

    WriteToSpannerFn(
        SpannerConfig spannerConfig, FailureMode failureMode, TupleTag<MutationGroup> failedTag) {
      this.spannerConfig = spannerConfig;
      this.failureMode = failureMode;
      this.failedTag = failedTag;
    }

    @Setup
    public void setup() throws Exception {
      spannerAccessor = spannerConfig.connectToSpanner();
    }

    @Teardown
    public void teardown() throws Exception {
      spannerAccessor.close();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Iterable<MutationGroup> mutations = c.element();
      boolean tryIndividual = false;
      // Batch upsert rows.
      try {
        mutationGroupBatchesCounter.inc();
        Iterable<Mutation> batch = Iterables.concat(mutations);
        spannerAccessor.getDatabaseClient().writeAtLeastOnce(batch);
        mutationGroupWriteSuccessCounter.inc(Iterables.size(mutations));
        return;
      } catch (SpannerException e) {
        if (failureMode == FailureMode.REPORT_FAILURES) {
          tryIndividual = true;
        } else if (failureMode == FailureMode.FAIL_FAST) {
          throw e;
        } else {
          throw new IllegalArgumentException("Unknown failure mode " + failureMode);
        }
      }
      if (tryIndividual) {
        for (MutationGroup mg : mutations) {
          try {
            spannerAccessor.getDatabaseClient().writeAtLeastOnce(mg);
            mutationGroupWriteSuccessCounter.inc();
          } catch (SpannerException e) {
            mutationGroupWriteFailCounter.inc();
            LOG.warn("Failed to write the mutation group: " + mg, e);
            c.output(failedTag, mg);
          }
        }
      }
    }
  }

  private SpannerIO() {} // Prevent construction.
}
