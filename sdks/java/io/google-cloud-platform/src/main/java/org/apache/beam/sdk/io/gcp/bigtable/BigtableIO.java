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
package org.apache.beam.sdk.io.gcp.bigtable;

import static org.apache.beam.sdk.io.gcp.bigtable.BigtableServiceFactory.BigtableServiceEntry;
import static org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import static org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter.BAD_RECORD_TAG;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.gax.batching.BatchingException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.auto.value.AutoValue;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.UniqueIdGenerator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.action.ActionFactory;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.BigtableChangeStreamAccessor;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.BigtableClientOverride;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.DetectNewPartitionsDoFn;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.FilterForMutationDoFn;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.InitializeDoFn;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.ReadChangeStreamPartitionDoFn;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.estimator.CoderSizeEstimator;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.io.range.ByteKeyRangeTracker;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects.ToStringHelper;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PTransform Transforms} for reading from and writing to Google Cloud Bigtable.
 *
 * <p>Please note the Cloud BigTable HBase connector available <a
 * href="https://github.com/googleapis/java-bigtable-hbase/tree/master/bigtable-dataflow-parent/bigtable-hbase-beam">here</a>.
 * We recommend using that connector over this one if <a
 * href="https://cloud.google.com/bigtable/docs/hbase-bigtable">HBase API</a></> works for your
 * needs.
 *
 * <p>For more information about Cloud Bigtable, see the online documentation at <a
 * href="https://cloud.google.com/bigtable/">Google Cloud Bigtable</a>.
 *
 * <h3>Reading from Cloud Bigtable</h3>
 *
 * <p>The Bigtable source returns a set of rows from a single table, returning a {@code
 * PCollection<Row>}.
 *
 * <p>To configure a Cloud Bigtable source, you must supply a table id, a project id, an instance id
 * and optionally a {@link BigtableOptions} to provide more specific connection configuration. By
 * default, {@link BigtableIO.Read} will read all rows in the table. The row ranges to be read can
 * optionally be restricted using {@link BigtableIO.Read#withKeyRanges}, and a {@link RowFilter} can
 * be specified using {@link BigtableIO.Read#withRowFilter}. For example:
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // Scan the entire table.
 * p.apply("read",
 *     BigtableIO.read()
 *         .withProjectId(projectId)
 *         .withInstanceId(instanceId)
 *         .withTableId("table"));
 *
 * // Scan a prefix of the table.
 * ByteKeyRange keyRange = ...;
 * p.apply("read",
 *     BigtableIO.read()
 *         .withProjectId(projectId)
 *         .withInstanceId(instanceId)
 *         .withTableId("table")
 *         .withKeyRange(keyRange));
 *
 * // Scan a subset of rows that match the specified row filter.
 * p.apply("filtered read",
 *     BigtableIO.read()
 *         .withProjectId(projectId)
 *         .withInstanceId(instanceId)
 *         .withTableId("table")
 *         .withRowFilter(filter));
 *
 * // Configure timeouts for reads.
 * // Let each attempt run for 1 second, retry if the attempt failed.
 * // Give up after the request is retried for 60 seconds.
 * Duration attemptTimeout = Duration.millis(1000);
 * Duration operationTimeout = Duration.millis(60 * 1000);
 * p.apply("read",
 *     BigtableIO.read()
 *         .withProjectId(projectId)
 *         .withInstanceId(instanceId)
 *         .withTableId("table")
 *         .withKeyRange(keyRange)
 *         .withAttemptTimeout(attemptTimeout)
 *         .withOperationTimeout(operationTimeout);
 * }</pre>
 *
 * <h3>Writing to Cloud Bigtable</h3>
 *
 * <p>The Bigtable sink executes a set of row mutations on a single table. It takes as input a
 * {@link PCollection PCollection&lt;KV&lt;ByteString, Iterable&lt;Mutation&gt;&gt;&gt;}, where the
 * {@link ByteString} is the key of the row being mutated, and each {@link Mutation} represents an
 * idempotent transformation to that row.
 *
 * <p>To configure a Cloud Bigtable sink, you must supply a table id, a project id, an instance id
 * and optionally a configuration function for {@link BigtableOptions} to provide more specific
 * connection configuration, for example:
 *
 * <pre>{@code
 * PCollection<KV<ByteString, Iterable<Mutation>>> data = ...;
 *
 * data.apply("write",
 *     BigtableIO.write()
 *         .withProjectId("project")
 *         .withInstanceId("instance")
 *         .withTableId("table"));
 * }
 *
 * // Configure batch size for writes
 * data.apply("write",
 *     BigtableIO.write()
 *         .withProjectId("project")
 *         .withInstanceId("instance")
 *         .withTableId("table")
 *         .withBatchElements(100)); // every batch will have 100 elements
 * }</pre>
 *
 * <p>Configure timeout for writes:
 *
 * <pre>{@code
 * // Let each attempt run for 1 second, retry if the attempt failed.
 * // Give up after the request is retried for 60 seconds.
 * Duration attemptTimeout = Duration.millis(1000);
 * Duration operationTimeout = Duration.millis(60 * 1000);
 * data.apply("write",
 *     BigtableIO.write()
 *         .withProjectId("project")
 *         .withInstanceId("instance")
 *         .withTableId("table")
 *         .withAttemptTimeout(attemptTimeout)
 *         .withOperationTimeout(operationTimeout));
 * }</pre>
 *
 * <p>You can also limit the wait time in the finish bundle step by setting the
 * bigtable_writer_wait_timeout_ms experimental flag when you run the pipeline. For example,
 * --experiments=bigtable_writer_wait_timeout_ms=60000 will limit the wait time in finish bundle to
 * be 10 minutes.
 *
 * <p>Optionally, BigtableIO.write() may be configured to emit {@link BigtableWriteResult} elements
 * after each group of inputs is written to Bigtable. These can be used to then trigger user code
 * after writes have completed. See {@link org.apache.beam.sdk.transforms.Wait} for details on the
 * windowing requirements of the signal and input PCollections.
 *
 * <pre>{@code
 * // See Wait.on
 * PCollection<KV<ByteString, Iterable<Mutation>>> data = ...;
 *
 * PCollection<BigtableWriteResult> writeResults =
 *     data.apply("write",
 *         BigtableIO.write()
 *             .withProjectId("project")
 *             .withInstanceId("instance")
 *             .withTableId("table"))
 *             .withWriteResults();
 *
 * // The windowing of `moreData` must be compatible with `data`, see {@link org.apache.beam.sdk.transforms.Wait#on}
 * // for details.
 * PCollection<...> moreData = ...;
 *
 * moreData
 *     .apply("wait for writes", Wait.on(writeResults))
 *     .apply("do something", ParDo.of(...))
 *
 * }</pre>
 *
 * <h3>Streaming Changes from Cloud Bigtable</h3>
 *
 * <p>Cloud Bigtable change streams enable users to capture and stream out mutations from their
 * Cloud Bigtable tables in real-time. Cloud Bigtable change streams enable many use cases including
 * integrating with a user's data analytics pipelines, support audit and archival requirements as
 * well as triggering downstream application logic on specific database changes.
 *
 * <p>Change stream connector creates and manages a metadata table to manage the state of the
 * connector. By default, the table is created in the same instance as the table being streamed.
 * However, it can be overridden with {@link
 * BigtableIO.ReadChangeStream#withMetadataTableProjectId}, {@link
 * BigtableIO.ReadChangeStream#withMetadataTableInstanceId}, {@link
 * BigtableIO.ReadChangeStream#withMetadataTableTableId}, and {@link
 * BigtableIO.ReadChangeStream#withMetadataTableAppProfileId}. The app profile for the metadata
 * table must be a single cluster app profile with single row transaction enabled.
 *
 * <p>Note - To prevent unforeseen stream stalls, the BigtableIO connector outputs all data with an
 * output timestamp of zero, making all data late, which will ensure that the stream will not stall.
 * However, it means that you may have to deal with all data as late data, and features that depend
 * on watermarks will not function. This means that Windowing functions and States and Timers are no
 * longer effectively usable. Example use cases that are not possible because of this include:
 *
 * <ul>
 *   <li>Completeness in a replicated cluster with writes to a row on multiple clusters.
 *   <li>Ordering at the row level in a replicated cluster where the row is being written through
 *       multiple-clusters.
 * </ul>
 *
 * Users can use GlobalWindows with (non-event time) Triggers to group this late data into Panes.
 * You can see an example of this in the pipeline below.
 *
 * <pre>{@code
 * Pipeline pipeline = ...;
 * pipeline
 *    .apply(
 *        BigtableIO.readChangeStream()
 *            .withProjectId(projectId)
 *            .withInstanceId(instanceId)
 *            .withTableId(tableId)
 *            .withAppProfileId(appProfileId)
 *            .withStartTime(startTime));
 * }</pre>
 *
 * <h3>Enable client side metrics</h3>
 *
 * <p>Client side metrics can be enabled with an experiments flag when you run the pipeline:
 * --experiments=bigtable_enable_client_side_metrics. These metrics can provide additional insights
 * to your job. You can read more about client side metrics in this documentation:
 * https://cloud.google.com/bigtable/docs/client-side-metrics.
 *
 * <h3>Permissions</h3>
 *
 * <p>Permission requirements depend on the {@link PipelineRunner} that is used to execute the
 * pipeline. Please refer to the documentation of corresponding {@link PipelineRunner
 * PipelineRunners} for more details.
 *
 * <h3>Updates to the I/O connector code</h3>
 *
 * For any significant updates to this I/O connector, please consider involving corresponding code
 * reviewers mentioned <a
 * href="https://github.com/apache/beam/blob/master/sdks/java/io/google-cloud-platform/OWNERS">
 * here</a>.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BigtableIO {
  private static final Logger LOG = LoggerFactory.getLogger(BigtableIO.class);

  /**
   * Creates an uninitialized {@link BigtableIO.Read}. Before use, the {@code Read} must be
   * initialized with a {@link BigtableIO.Read#withInstanceId} and {@link
   * BigtableIO.Read#withProjectId} that specifies the source Cloud Bigtable instance, and a {@link
   * BigtableIO.Read#withTableId} that specifies which table to read. A {@link RowFilter} may also
   * optionally be specified using {@link BigtableIO.Read#withRowFilter(RowFilter)}.
   */
  public static Read read() {
    return Read.create();
  }

  /**
   * Creates an uninitialized {@link BigtableIO.Write}. Before use, the {@code Write} must be
   * initialized with a {@link BigtableIO.Write#withProjectId} and {@link
   * BigtableIO.Write#withInstanceId} that specifies the destination Cloud Bigtable instance, and a
   * {@link BigtableIO.Write#withTableId} that specifies which table to write.
   */
  public static Write write() {
    return Write.create();
  }

  /**
   * Creates an uninitialized {@link BigtableIO.ReadChangeStream}. Before use, the {@code
   * ReadChangeStream} must be initialized with
   *
   * <ul>
   *   <li>{@link BigtableIO.ReadChangeStream#withProjectId}
   *   <li>{@link BigtableIO.ReadChangeStream#withInstanceId}
   *   <li>{@link BigtableIO.ReadChangeStream#withTableId}
   *   <li>{@link BigtableIO.ReadChangeStream#withAppProfileId}
   * </ul>
   *
   * <p>And optionally with
   *
   * <ul>
   *   <li>{@link BigtableIO.ReadChangeStream#withStartTime} which defaults to now.
   *   <li>{@link BigtableIO.ReadChangeStream#withMetadataTableProjectId} which defaults to value
   *       from {@link BigtableIO.ReadChangeStream#withProjectId}
   *   <li>{@link BigtableIO.ReadChangeStream#withMetadataTableInstanceId} which defaults to value
   *       from {@link BigtableIO.ReadChangeStream#withInstanceId}
   *   <li>{@link BigtableIO.ReadChangeStream#withMetadataTableTableId} which defaults to {@link
   *       MetadataTableAdminDao#DEFAULT_METADATA_TABLE_NAME}
   *   <li>{@link BigtableIO.ReadChangeStream#withMetadataTableAppProfileId} which defaults to value
   *       from {@link BigtableIO.ReadChangeStream#withAppProfileId}
   *   <li>{@link BigtableIO.ReadChangeStream#withChangeStreamName} which defaults to randomly
   *       generated string.
   * </ul>
   */
  public static ReadChangeStream readChangeStream() {
    return ReadChangeStream.create();
  }

  /**
   * A {@link PTransform} that reads from Google Cloud Bigtable. See the class-level Javadoc on
   * {@link BigtableIO} for more information.
   *
   * @see BigtableIO
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<Row>> {

    abstract BigtableConfig getBigtableConfig();

    abstract BigtableReadOptions getBigtableReadOptions();

    @VisibleForTesting
    abstract BigtableServiceFactory getServiceFactory();

    /** Returns the table being read from. */
    public @Nullable String getTableId() {
      ValueProvider<String> tableId = getBigtableReadOptions().getTableId();
      return tableId != null && tableId.isAccessible() ? tableId.get() : null;
    }

    /**
     * Returns the Google Cloud Bigtable instance being read from, and other parameters.
     *
     * @deprecated read options are configured directly on BigtableIO.read(). Use {@link
     *     #populateDisplayData(DisplayData.Builder)} to view the current configurations.
     */
    @Deprecated
    public @Nullable BigtableOptions getBigtableOptions() {
      return getBigtableConfig().getBigtableOptions();
    }

    abstract Builder toBuilder();

    static Read create() {
      BigtableConfig config = BigtableConfig.builder().setValidate(true).build();

      return new AutoValue_BigtableIO_Read.Builder()
          .setBigtableConfig(config)
          .setBigtableReadOptions(
              BigtableReadOptions.builder()
                  .setTableId(StaticValueProvider.of(""))
                  .setKeyRanges(
                      StaticValueProvider.of(Collections.singletonList(ByteKeyRange.ALL_KEYS)))
                  .build())
          .setServiceFactory(new BigtableServiceFactory())
          .build();
    }

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setBigtableConfig(BigtableConfig bigtableConfig);

      abstract Builder setBigtableReadOptions(BigtableReadOptions bigtableReadOptions);

      abstract Builder setServiceFactory(BigtableServiceFactory factory);

      abstract Read build();
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will read from the Cloud Bigtable project
     * indicated by given parameter, requires {@link #withInstanceId} to be called to determine the
     * instance.
     *
     * <p>Does not modify this object.
     */
    public Read withProjectId(ValueProvider<String> projectId) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder().setBigtableConfig(config.withProjectId(projectId)).build();
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will read from the Cloud Bigtable project
     * indicated by given parameter, requires {@link #withInstanceId} to be called to determine the
     * instance.
     *
     * <p>Does not modify this object.
     */
    public Read withProjectId(String projectId) {
      return withProjectId(StaticValueProvider.of(projectId));
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will read from the Cloud Bigtable instance
     * indicated by given parameter, requires {@link #withProjectId} to be called to determine the
     * project.
     *
     * <p>Does not modify this object.
     */
    public Read withInstanceId(ValueProvider<String> instanceId) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder().setBigtableConfig(config.withInstanceId(instanceId)).build();
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will read from the Cloud Bigtable instance
     * indicated by given parameter, requires {@link #withProjectId} to be called to determine the
     * project.
     *
     * <p>Does not modify this object.
     */
    public Read withInstanceId(String instanceId) {
      return withInstanceId(StaticValueProvider.of(instanceId));
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will read from the specified table.
     *
     * <p>Does not modify this object.
     */
    public Read withTableId(ValueProvider<String> tableId) {
      BigtableReadOptions bigtableReadOptions = getBigtableReadOptions();
      return toBuilder()
          .setBigtableReadOptions(bigtableReadOptions.toBuilder().setTableId(tableId).build())
          .build();
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will read from the specified table.
     *
     * <p>Does not modify this object.
     */
    public Read withTableId(String tableId) {
      return withTableId(StaticValueProvider.of(tableId));
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will read using the specified app profile id.
     *
     * <p>Does not modify this object.
     */
    public Read withAppProfileId(ValueProvider<String> appProfileId) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder().setBigtableConfig(config.withAppProfileId(appProfileId)).build();
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will read using the specified app profile id.
     *
     * <p>Does not modify this object.
     */
    public Read withAppProfileId(String appProfileId) {
      return withAppProfileId(StaticValueProvider.of(appProfileId));
    }

    /**
     * WARNING: Should be used only to specify additional parameters for connection to the Cloud
     * Bigtable, instanceId and projectId should be provided over {@link #withInstanceId} and {@link
     * #withProjectId} respectively.
     *
     * <p>Returns a new {@link BigtableIO.Read} that will read from the Cloud Bigtable instance
     * indicated by {@link #withProjectId}, and using any other specified customizations.
     *
     * <p>Does not modify this object.
     *
     * @deprecated please set the configurations directly:
     *     BigtableIO.read().withProjectId(projectId).withInstanceId(instanceId).withTableId(tableId)
     *     and set credentials in {@link PipelineOptions}.
     */
    @Deprecated
    public Read withBigtableOptions(BigtableOptions options) {
      checkArgument(options != null, "options can not be null");
      return withBigtableOptions(options.toBuilder());
    }

    /**
     * WARNING: Should be used only to specify additional parameters for connection to the Cloud
     * Bigtable, instanceId and projectId should be provided over {@link #withInstanceId} and {@link
     * #withProjectId} respectively.
     *
     * <p>Returns a new {@link BigtableIO.Read} that will read from the Cloud Bigtable instance
     * indicated by the given options, and using any other specified customizations.
     *
     * <p>Clones the given {@link BigtableOptions} builder so that any further changes will have no
     * effect on the returned {@link BigtableIO.Read}.
     *
     * <p>Does not modify this object.
     *
     * @deprecated please set the configurations directly:
     *     BigtableIO.read().withProjectId(projectId).withInstanceId(instanceId).withTableId(tableId)
     *     and set credentials in {@link PipelineOptions}.
     */
    @Deprecated
    public Read withBigtableOptions(BigtableOptions.Builder optionsBuilder) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder()
          .setBigtableConfig(config.withBigtableOptions(optionsBuilder.build().toBuilder().build()))
          .build();
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will read from the Cloud Bigtable instance with
     * customized options provided by given configurator.
     *
     * <p>WARNING: instanceId and projectId should not be provided here and should be provided over
     * {@link #withProjectId} and {@link #withInstanceId}.
     *
     * <p>Does not modify this object.
     *
     * @deprecated please set the configurations directly:
     *     BigtableIO.read().withProjectId(projectId).withInstanceId(instanceId).withTableId(tableId)
     *     and set credentials in {@link PipelineOptions}.
     */
    @Deprecated
    public Read withBigtableOptionsConfigurator(
        SerializableFunction<BigtableOptions.Builder, BigtableOptions.Builder> configurator) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder()
          .setBigtableConfig(config.withBigtableOptionsConfigurator(configurator))
          .build();
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will filter the rows read from Cloud Bigtable
     * using the given row filter.
     *
     * <p>Does not modify this object.
     */
    public Read withRowFilter(ValueProvider<RowFilter> filter) {
      checkArgument(filter != null, "filter can not be null");
      BigtableReadOptions bigtableReadOptions = getBigtableReadOptions();
      return toBuilder()
          .setBigtableReadOptions(bigtableReadOptions.toBuilder().setRowFilter(filter).build())
          .build();
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will filter the rows read from Cloud Bigtable
     * using the given row filter.
     *
     * <p>Does not modify this object.
     */
    public Read withRowFilter(RowFilter filter) {
      return withRowFilter(StaticValueProvider.of(filter));
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will break up read requests into smaller batches.
     * This function will switch the base BigtableIO.Reader class to using the SegmentReader. If
     * null is passed, this behavior will be disabled and the stream reader will be used.
     *
     * <p>Does not modify this object.
     *
     * <p>When we have a builder, we initialize the value. When they call the method then we
     * override the value
     */
    public Read withMaxBufferElementCount(@Nullable Integer maxBufferElementCount) {
      BigtableReadOptions bigtableReadOptions = getBigtableReadOptions();
      return toBuilder()
          .setBigtableReadOptions(
              bigtableReadOptions
                  .toBuilder()
                  .setMaxBufferElementCount(maxBufferElementCount)
                  .build())
          .build();
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will read only rows in the specified range.
     *
     * <p>Does not modify this object.
     */
    public Read withKeyRange(ByteKeyRange keyRange) {
      return withKeyRanges(Collections.singletonList(keyRange));
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will read only rows in the specified ranges.
     * Ranges must not overlap.
     *
     * <p>Does not modify this object.
     */
    public Read withKeyRanges(ValueProvider<List<ByteKeyRange>> keyRanges) {
      checkArgument(keyRanges != null, "keyRanges can not be null");
      BigtableReadOptions bigtableReadOptions = getBigtableReadOptions();
      return toBuilder()
          .setBigtableReadOptions(bigtableReadOptions.toBuilder().setKeyRanges(keyRanges).build())
          .build();
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will read only rows in the specified ranges.
     * Ranges must not overlap.
     *
     * <p>Does not modify this object.
     */
    public Read withKeyRanges(List<ByteKeyRange> keyRanges) {
      return withKeyRanges(StaticValueProvider.of(keyRanges));
    }

    /** Disables validation that the table being read from exists. */
    public Read withoutValidation() {
      BigtableConfig config = getBigtableConfig();
      return toBuilder().setBigtableConfig(config.withValidate(false)).build();
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will use an official Bigtable emulator.
     *
     * <p>This is used for testing.
     */
    @VisibleForTesting
    public Read withEmulator(String emulatorHost) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder().setBigtableConfig(config.withEmulator(emulatorHost)).build();
    }

    /**
     * Returns a new {@link BigtableIO.Read} with the attempt timeout. Attempt timeout controls the
     * timeout for each remote call.
     *
     * <p>Does not modify this object.
     */
    public Read withAttemptTimeout(Duration timeout) {
      checkArgument(timeout.isLongerThan(Duration.ZERO), "attempt timeout must be positive");
      BigtableReadOptions readOptions = getBigtableReadOptions();
      return toBuilder()
          .setBigtableReadOptions(readOptions.toBuilder().setAttemptTimeout(timeout).build())
          .build();
    }

    /**
     * Returns a new {@link BigtableIO.Read} with the operation timeout. Operation timeout has
     * ultimate control over how long the logic should keep trying the remote call until it gives up
     * completely.
     *
     * <p>Does not modify this object.
     */
    public Read withOperationTimeout(Duration timeout) {
      checkArgument(timeout.isLongerThan(Duration.ZERO), "operation timeout must be positive");
      BigtableReadOptions readOptions = getBigtableReadOptions();
      return toBuilder()
          .setBigtableReadOptions(readOptions.toBuilder().setOperationTimeout(timeout).build())
          .build();
    }

    Read withServiceFactory(BigtableServiceFactory factory) {
      return toBuilder().setServiceFactory(factory).build();
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
      getBigtableConfig().validate();
      getBigtableReadOptions().validate();

      BigtableSource source =
          new BigtableSource(
              getServiceFactory(),
              getServiceFactory().newId(),
              getBigtableConfig(),
              getBigtableReadOptions(),
              null);
      return input.getPipeline().apply(org.apache.beam.sdk.io.Read.from(source));
    }

    @Override
    public void validate(PipelineOptions options) {
      validateTableExists(getBigtableConfig(), getBigtableReadOptions(), options);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      getBigtableConfig().populateDisplayData(builder);
      getBigtableReadOptions().populateDisplayData(builder);
    }

    @Override
    public final String toString() {
      ToStringHelper helper =
          MoreObjects.toStringHelper(Read.class).add("config", getBigtableConfig());
      return helper.add("readOptions", getBigtableReadOptions()).toString();
    }

    private void validateTableExists(
        BigtableConfig config, BigtableReadOptions readOptions, PipelineOptions options) {
      if (config.getValidate() && config.isDataAccessible() && readOptions.isDataAccessible()) {
        ValueProvider<String> tableIdProvider = checkArgumentNotNull(readOptions.getTableId());
        String tableId = checkArgumentNotNull(tableIdProvider.get());
        try {
          boolean exists = getServiceFactory().checkTableExists(config, options, tableId);
          checkArgument(exists, "Table %s does not exist", tableId);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * A {@link PTransform} that writes to Google Cloud Bigtable. See the class-level Javadoc on
   * {@link BigtableIO} for more information.
   *
   * @see BigtableIO
   */
  @AutoValue
  public abstract static class Write
      extends PTransform<PCollection<KV<ByteString, Iterable<Mutation>>>, PDone> {

    static SerializableFunction<BigtableOptions.Builder, BigtableOptions.Builder>
        enableBulkApiConfigurator(
            final @Nullable SerializableFunction<BigtableOptions.Builder, BigtableOptions.Builder>
                    userConfigurator) {
      return optionsBuilder -> {
        if (userConfigurator != null) {
          optionsBuilder = userConfigurator.apply(optionsBuilder);
        }

        return optionsBuilder.setBulkOptions(
            optionsBuilder.build().getBulkOptions().toBuilder().setUseBulkApi(true).build());
      };
    }

    abstract BigtableConfig getBigtableConfig();

    abstract BigtableWriteOptions getBigtableWriteOptions();

    @VisibleForTesting
    abstract BigtableServiceFactory getServiceFactory();

    abstract ErrorHandler<BadRecord, ?> getBadRecordErrorHandler();

    abstract BadRecordRouter getBadRecordRouter();

    /**
     * Returns the Google Cloud Bigtable instance being written to, and other parameters.
     *
     * @deprecated write options are configured directly on BigtableIO.write(). Use {@link
     *     #populateDisplayData(DisplayData.Builder)} to view the current configurations.
     */
    @Deprecated
    public @Nullable BigtableOptions getBigtableOptions() {
      return getBigtableConfig().getBigtableOptions();
    }

    abstract Builder toBuilder();

    static Write create() {
      BigtableConfig config = BigtableConfig.builder().setValidate(true).build();

      BigtableWriteOptions writeOptions =
          BigtableWriteOptions.builder().setTableId(StaticValueProvider.of("")).build();

      return new AutoValue_BigtableIO_Write.Builder()
          .setBigtableConfig(config)
          .setBigtableWriteOptions(writeOptions)
          .setServiceFactory(new BigtableServiceFactory())
          .setBadRecordErrorHandler(new ErrorHandler.DefaultErrorHandler<>())
          .setBadRecordRouter(BadRecordRouter.THROWING_ROUTER)
          .build();
    }

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setBigtableConfig(BigtableConfig bigtableConfig);

      abstract Builder setBigtableWriteOptions(BigtableWriteOptions writeOptions);

      abstract Builder setServiceFactory(BigtableServiceFactory factory);

      abstract Builder setBadRecordErrorHandler(ErrorHandler<BadRecord, ?> badRecordErrorHandler);

      abstract Builder setBadRecordRouter(BadRecordRouter badRecordRouter);

      abstract Write build();
    }

    /**
     * Returns a new {@link BigtableIO.Write} that will write into the Cloud Bigtable project
     * indicated by given parameter, requires {@link #withInstanceId} to be called to determine the
     * instance.
     *
     * <p>Does not modify this object.
     */
    public Write withProjectId(ValueProvider<String> projectId) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder().setBigtableConfig(config.withProjectId(projectId)).build();
    }

    /**
     * Returns a new {@link BigtableIO.Write} that will write into the Cloud Bigtable project
     * indicated by given parameter, requires {@link #withInstanceId} to be called to determine the
     * instance.
     *
     * <p>Does not modify this object.
     */
    public Write withProjectId(String projectId) {
      return withProjectId(StaticValueProvider.of(projectId));
    }

    /**
     * Returns a new {@link BigtableIO.Write} that will write into the Cloud Bigtable instance
     * indicated by given parameter, requires {@link #withProjectId} to be called to determine the
     * project.
     *
     * <p>Does not modify this object.
     */
    public Write withInstanceId(ValueProvider<String> instanceId) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder().setBigtableConfig(config.withInstanceId(instanceId)).build();
    }

    /**
     * Returns a new {@link BigtableIO.Write} that will write into the Cloud Bigtable instance
     * indicated by given parameter, requires {@link #withProjectId} to be called to determine the
     * project.
     *
     * <p>Does not modify this object.
     */
    public Write withInstanceId(String instanceId) {
      return withInstanceId(StaticValueProvider.of(instanceId));
    }

    /**
     * Returns a new {@link BigtableIO.Write} that will write to the specified table.
     *
     * <p>Does not modify this object.
     */
    public Write withTableId(ValueProvider<String> tableId) {
      BigtableWriteOptions writeOptions = getBigtableWriteOptions();
      return toBuilder()
          .setBigtableWriteOptions(writeOptions.toBuilder().setTableId(tableId).build())
          .build();
    }

    /**
     * Returns a new {@link BigtableIO.Write} that will write to the specified table.
     *
     * <p>Does not modify this object.
     */
    public Write withTableId(String tableId) {
      return withTableId(StaticValueProvider.of(tableId));
    }

    /**
     * Returns a new {@link BigtableIO.Write} that will write using the specified app profile id.
     *
     * <p>Remember that in order to use single-row transactions, this must use a single-cluster
     * routing policy.
     *
     * <p>Does not modify this object.
     */
    public Write withAppProfileId(ValueProvider<String> appProfileId) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder().setBigtableConfig(config.withAppProfileId(appProfileId)).build();
    }

    /**
     * Returns a new {@link BigtableIO.Write} that will write using the specified app profile id.
     *
     * <p>Remember that in order to use single-row transactions, this must use a single-cluster
     * routing policy.
     *
     * <p>Does not modify this object.
     */
    public Write withAppProfileId(String appProfileId) {
      return withAppProfileId(StaticValueProvider.of(appProfileId));
    }

    /**
     * WARNING: Should be used only to specify additional parameters for connection to the Cloud
     * Bigtable, instanceId and projectId should be provided over {@link #withInstanceId} and {@link
     * #withProjectId} respectively.
     *
     * <p>Returns a new {@link BigtableIO.Write} that will write to the Cloud Bigtable instance
     * indicated by the given options, and using any other specified customizations.
     *
     * <p>Does not modify this object.
     *
     * @deprecated please configure the write options directly:
     *     BigtableIO.write().withProjectId(projectId).withInstanceId(instanceId).withTableId(tableId)
     *     and set credentials in {@link PipelineOptions}.
     */
    @Deprecated
    public Write withBigtableOptions(BigtableOptions options) {
      checkArgument(options != null, "options can not be null");
      return withBigtableOptions(options.toBuilder());
    }

    /**
     * WARNING: Should be used only to specify additional parameters for connection to the Cloud
     * Bigtable, instanceId and projectId should be provided over {@link #withInstanceId} and {@link
     * #withProjectId} respectively.
     *
     * <p>Returns a new {@link BigtableIO.Write} that will write to the Cloud Bigtable instance
     * indicated by the given options, and using any other specified customizations.
     *
     * <p>Clones the given {@link BigtableOptions} builder so that any further changes will have no
     * effect on the returned {@link BigtableIO.Write}.
     *
     * <p>Does not modify this object.
     *
     * @deprecated please configure the write options directly:
     *     BigtableIO.write().withProjectId(projectId).withInstanceId(instanceId).withTableId(tableId)
     *     and set credentials in {@link PipelineOptions}.
     */
    @Deprecated
    public Write withBigtableOptions(BigtableOptions.Builder optionsBuilder) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder()
          .setBigtableConfig(config.withBigtableOptions(optionsBuilder.build()))
          .build();
    }

    /**
     * Returns a new {@link BigtableIO.Write} that will read from the Cloud Bigtable instance with
     * customized options provided by given configurator.
     *
     * <p>WARNING: instanceId and projectId should not be provided here and should be provided over
     * {@link #withProjectId} and {@link #withInstanceId}.
     *
     * <p>Does not modify this object.
     *
     * @deprecated please configure the write options directly:
     *     BigtableIO.write().withProjectId(projectId).withInstanceId(instanceId).withTableId(tableId)
     *     and set credentials in {@link PipelineOptions}.
     */
    @Deprecated
    public Write withBigtableOptionsConfigurator(
        SerializableFunction<BigtableOptions.Builder, BigtableOptions.Builder> configurator) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder()
          .setBigtableConfig(
              config.withBigtableOptionsConfigurator(enableBulkApiConfigurator(configurator)))
          .build();
    }

    /** Disables validation that the table being written to exists. */
    public Write withoutValidation() {
      BigtableConfig config = getBigtableConfig();
      return toBuilder().setBigtableConfig(config.withValidate(false)).build();
    }

    /**
     * Returns a new {@link BigtableIO.Write} that will use an official Bigtable emulator.
     *
     * <p>This is used for testing.
     */
    @VisibleForTesting
    public Write withEmulator(String emulatorHost) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder().setBigtableConfig(config.withEmulator(emulatorHost)).build();
    }

    /**
     * Returns a new {@link BigtableIO.Write} with the attempt timeout. Attempt timeout controls the
     * timeout for each remote call.
     *
     * <p>Does not modify this object.
     */
    public Write withAttemptTimeout(Duration timeout) {
      checkArgument(timeout.isLongerThan(Duration.ZERO), "attempt timeout must be positive");
      BigtableWriteOptions options = getBigtableWriteOptions();
      return toBuilder()
          .setBigtableWriteOptions(options.toBuilder().setAttemptTimeout(timeout).build())
          .build();
    }

    /**
     * Returns a new {@link BigtableIO.Write} with the operation timeout. Operation timeout has
     * ultimate control over how long the logic should keep trying the remote call until it gives up
     * completely.
     *
     * <p>Does not modify this object.
     */
    public Write withOperationTimeout(Duration timeout) {
      checkArgument(timeout.isLongerThan(Duration.ZERO), "operation timeout must be positive");
      BigtableWriteOptions options = getBigtableWriteOptions();
      return toBuilder()
          .setBigtableWriteOptions(options.toBuilder().setOperationTimeout(timeout).build())
          .build();
    }

    /**
     * Returns a new {@link BigtableIO.Write} with the max elements a batch can have. After this
     * many elements are accumulated, they will be wrapped up in a batch and sent to Bigtable.
     *
     * <p>Does not modify this object.
     */
    public Write withMaxElementsPerBatch(long size) {
      checkArgument(size > 0, "max elements per batch size must be positive");
      BigtableWriteOptions options = getBigtableWriteOptions();
      return toBuilder()
          .setBigtableWriteOptions(options.toBuilder().setMaxElementsPerBatch(size).build())
          .build();
    }

    /**
     * Returns a new {@link BigtableIO.Write} with the max bytes a batch can have. After this many
     * bytes are accumulated, the elements will be wrapped up in a batch and sent to Bigtable.
     *
     * <p>Does not modify this object.
     */
    public Write withMaxBytesPerBatch(long size) {
      checkArgument(size > 0, "max bytes per batch size must be positive");
      BigtableWriteOptions options = getBigtableWriteOptions();
      return toBuilder()
          .setBigtableWriteOptions(options.toBuilder().setMaxBytesPerBatch(size).build())
          .build();
    }

    /**
     * Returns a new {@link BigtableIO.Write} with the max number of outstanding elements allowed
     * before enforcing flow control.
     *
     * <p>Does not modify this object.
     */
    public Write withMaxOutstandingElements(long count) {
      checkArgument(count > 0, "max outstanding elements must be positive");
      BigtableWriteOptions options = getBigtableWriteOptions();
      return toBuilder()
          .setBigtableWriteOptions(options.toBuilder().setMaxOutstandingElements(count).build())
          .build();
    }

    /**
     * Returns a new {@link BigtableIO.Write} with the max number of outstanding bytes allowed
     * before enforcing flow control.
     *
     * <p>Does not modify this object.
     */
    public Write withMaxOutstandingBytes(long bytes) {
      checkArgument(bytes > 0, "max outstanding bytes must be positive");
      BigtableWriteOptions options = getBigtableWriteOptions();
      return toBuilder()
          .setBigtableWriteOptions(options.toBuilder().setMaxOutstandingBytes(bytes).build())
          .build();
    }

    /**
     * Returns a new {@link BigtableIO.Write} with flow control enabled if enableFlowControl is
     * true.
     *
     * <p>When enabled, traffic to Bigtable is automatically rate-limited to prevent overloading
     * Bigtable clusters while keeping enough load to trigger Bigtable Autoscaling (if enabled) to
     * provision more nodes as needed. It is different from the flow control set by {@link
     * #withMaxOutstandingElements(long)} and {@link #withMaxOutstandingBytes(long)}, which is
     * always enabled on batch writes and limits the number of outstanding requests to the Bigtable
     * server.
     *
     * <p>Does not modify this object.
     */
    public Write withFlowControl(boolean enableFlowControl) {
      BigtableWriteOptions options = getBigtableWriteOptions();
      return toBuilder()
          .setBigtableWriteOptions(options.toBuilder().setFlowControl(enableFlowControl).build())
          .build();
    }

    /** @deprecated This method has been deprecated in Beam 2.60.0. It does not have an effect. */
    @Deprecated
    public Write withThrottlingTargetMs(int throttlingTargetMs) {
      LOG.warn("withThrottlingTargetMs has been removed and does not have effect.");
      return this;
    }

    /** @deprecated This method has been deprecated in Beam 2.60.0. It does not have an effect. */
    @Deprecated
    public Write withThrottlingReportTargetMs(int throttlingReportTargetMs) {
      LOG.warn("withThrottlingReportTargetMs has been removed and does not have an effect.");
      return this;
    }

    public Write withErrorHandler(ErrorHandler<BadRecord, ?> badRecordErrorHandler) {
      return toBuilder()
          .setBadRecordErrorHandler(badRecordErrorHandler)
          .setBadRecordRouter(BadRecordRouter.RECORDING_ROUTER)
          .build();
    }

    @VisibleForTesting
    Write withServiceFactory(BigtableServiceFactory factory) {
      return toBuilder().setServiceFactory(factory).build();
    }

    /**
     * Returns a {@link BigtableIO.WriteWithResults} that will emit a {@link BigtableWriteResult}
     * for each batch of rows written.
     */
    public WriteWithResults withWriteResults() {
      return new WriteWithResults(
          getBigtableConfig(),
          getBigtableWriteOptions(),
          getServiceFactory(),
          getBadRecordErrorHandler(),
          getBadRecordRouter());
    }

    @Override
    public PDone expand(PCollection<KV<ByteString, Iterable<Mutation>>> input) {
      input.apply(withWriteResults());
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PipelineOptions options) {
      withWriteResults().validate(options);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      withWriteResults().populateDisplayData(builder);
    }

    @Override
    public final String toString() {
      return MoreObjects.toStringHelper(Write.class).add("config", getBigtableConfig()).toString();
    }
  }

  /**
   * A {@link PTransform} that writes to Google Cloud Bigtable and emits a {@link
   * BigtableWriteResult} for each batch written. See the class-level Javadoc on {@link BigtableIO}
   * for more information.
   *
   * @see BigtableIO
   */
  public static class WriteWithResults
      extends PTransform<
          PCollection<KV<ByteString, Iterable<Mutation>>>, PCollection<BigtableWriteResult>> {

    private static final String BIGTABLE_WRITER_WAIT_TIMEOUT_MS = "bigtable_writer_wait_timeout_ms";

    private static final TupleTag<BigtableWriteResult> WRITE_RESULTS =
        new TupleTag<>("writeResults");

    private final BigtableConfig bigtableConfig;
    private final BigtableWriteOptions bigtableWriteOptions;

    private final BigtableServiceFactory factory;

    private final ErrorHandler<BadRecord, ?> badRecordErrorHandler;

    private final BadRecordRouter badRecordRouter;

    WriteWithResults(
        BigtableConfig bigtableConfig,
        BigtableWriteOptions bigtableWriteOptions,
        BigtableServiceFactory factory,
        ErrorHandler<BadRecord, ?> badRecordErrorHandler,
        BadRecordRouter badRecordRouter) {
      this.bigtableConfig = bigtableConfig;
      this.bigtableWriteOptions = bigtableWriteOptions;
      this.factory = factory;
      this.badRecordErrorHandler = badRecordErrorHandler;
      this.badRecordRouter = badRecordRouter;
    }

    @Override
    public PCollection<BigtableWriteResult> expand(
        PCollection<KV<ByteString, Iterable<Mutation>>> input) {
      bigtableConfig.validate();
      bigtableWriteOptions.validate();

      // Get experimental flag and set on BigtableWriteOptions
      PipelineOptions pipelineOptions = input.getPipeline().getOptions();
      String closeWaitTimeoutStr =
          ExperimentalOptions.getExperimentValue(pipelineOptions, BIGTABLE_WRITER_WAIT_TIMEOUT_MS);
      Duration closeWaitTimeout = null;
      if (closeWaitTimeoutStr != null) {
        long closeWaitTimeoutMs = Long.parseLong(closeWaitTimeoutStr);
        checkState(closeWaitTimeoutMs > 0, "Close wait timeout must be positive");
        closeWaitTimeout = Duration.millis(closeWaitTimeoutMs);
      }

      PCollectionTuple results =
          input.apply(
              ParDo.of(
                      new BigtableWriterFn(
                          factory,
                          bigtableConfig,
                          bigtableWriteOptions
                              .toBuilder()
                              .setCloseWaitTimeout(closeWaitTimeout)
                              .build(),
                          input.getCoder(),
                          badRecordRouter))
                  .withOutputTags(WRITE_RESULTS, TupleTagList.of(BAD_RECORD_TAG)));

      badRecordErrorHandler.addErrorCollection(
          results.get(BAD_RECORD_TAG).setCoder(BadRecord.getCoder(input.getPipeline())));

      return results.get(WRITE_RESULTS);
    }

    @Override
    public void validate(PipelineOptions options) {
      validateTableExists(bigtableConfig, bigtableWriteOptions, options);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      bigtableConfig.populateDisplayData(builder);
      bigtableWriteOptions.populateDisplayData(builder);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(WriteWithResults.class)
          .add("config", bigtableConfig)
          .add("writeOptions", bigtableWriteOptions)
          .toString();
    }

    private void validateTableExists(
        BigtableConfig config, BigtableWriteOptions writeOptions, PipelineOptions options) {
      if (config.getValidate() && config.isDataAccessible() && writeOptions.isDataAccessible()) {
        ValueProvider<String> tableIdProvider = checkArgumentNotNull(writeOptions.getTableId());
        String tableId = checkArgumentNotNull(tableIdProvider.get());
        try {
          boolean exists = factory.checkTableExists(config, options, tableId);
          checkArgument(exists, "Table %s does not exist", tableId);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private static class BigtableWriterFn
      extends DoFn<KV<ByteString, Iterable<Mutation>>, BigtableWriteResult> {

    private final BigtableServiceFactory factory;
    private final BigtableServiceFactory.ConfigId id;
    private final Coder<KV<ByteString, Iterable<Mutation>>> inputCoder;
    private final BadRecordRouter badRecordRouter;
    private transient ConcurrentLinkedQueue<KV<BigtableWriteException, BoundedWindow>> badRecords =
        null;
    private transient boolean reportedLineage;

    // Assign serviceEntry in startBundle and clear it in tearDown.
    @Nullable private BigtableServiceEntry serviceEntry;

    private transient Queue<CompletableFuture<?>> outstandingWrites;

    BigtableWriterFn(
        BigtableServiceFactory factory,
        BigtableConfig bigtableConfig,
        BigtableWriteOptions writeOptions,
        Coder<KV<ByteString, Iterable<Mutation>>> inputCoder,
        BadRecordRouter badRecordRouter) {
      this.factory = factory;
      this.config = bigtableConfig;
      this.writeOptions = writeOptions;
      this.inputCoder = inputCoder;
      this.badRecordRouter = badRecordRouter;
      this.failures = new ConcurrentLinkedQueue<>();
      this.id = factory.newId();
      LOG.debug("Created Bigtable Write Fn with writeOptions {} ", writeOptions);
    }

    @StartBundle
    public void startBundle(StartBundleContext c) throws IOException {
      recordsWritten = 0;
      this.seenWindows = Maps.newHashMapWithExpectedSize(1);

      // Ideally this would be in @Setup, but we need access to PipelineOptions and there is no easy
      // way to plumb it to @Setup.
      if (serviceEntry == null) {
        serviceEntry =
            factory.getServiceForWriting(id, config, writeOptions, c.getPipelineOptions());
      }

      if (bigtableWriter == null) {
        bigtableWriter = serviceEntry.getService().openForWriting(writeOptions);
      }

      badRecords = new ConcurrentLinkedQueue<>();
      outstandingWrites = new ArrayDeque<>();
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
      drainCompletedElementFutures();
      checkForFailures();
      KV<ByteString, Iterable<Mutation>> record = c.element();
      CompletableFuture<Void> f =
          bigtableWriter
              .writeRecord(record)
              // transform the next CompletionStage to have its own status
              // this allows us to capture any unexpected errors in the handler
              .handle(handleMutationException(record, window));
      outstandingWrites.add(f);
      ++recordsWritten;
      seenWindows.compute(window, (key, count) -> (count != null ? count : 0) + 1);
    }

    private void drainCompletedElementFutures() throws ExecutionException, InterruptedException {
      // burn down the completed futures to avoid unbounded memory growth
      for (Future<?> f = outstandingWrites.peek();
          f != null && f.isDone();
          f = outstandingWrites.peek()) {
        // Also ensure that errors in the handler get bubbled up
        outstandingWrites.remove().get();
      }
    }

    private BiFunction<MutateRowResponse, Throwable, Void> handleMutationException(
        KV<ByteString, Iterable<Mutation>> record, BoundedWindow window) {
      return (MutateRowResponse result, Throwable exception) -> {
        if (exception != null) {
          if (isDataException(exception)) {
            retryIndividualRecord(record, window);
          } else {
            failures.add(new BigtableWriteException(record, exception));
          }
        }
        return null;
      };
    }

    private void retryIndividualRecord(
        KV<ByteString, Iterable<Mutation>> record, BoundedWindow window) {
      try {
        bigtableWriter.writeSingleRecord(record);
      } catch (Throwable e) {
        if (isDataException(e)) {
          // if we get another NotFoundException, we know this is the bad record.
          badRecords.add(KV.of(new BigtableWriteException(record, e), window));
        } else {
          failures.add(new BigtableWriteException(record, e));
        }
      }
    }

    // This method checks if an exception is the result of an error in the data.
    // We first check if the exception is retryable, because if it is, we want to retry it via the
    // runner. If the method is retryable, we check if it is a NotFoundException, or if it's an
    // InvalidArgumentException. A NotFoundException likely means that the mutation is trying to
    // write to a column family that doesn't exist. An InvalidArgumentException means that the
    // mutation itself is invalid, with either an empty row key, invalid timestamp (ts <= -2),
    // an empty mutation, or a column qualifier that is too large.
    private static boolean isDataException(Throwable e) {
      if (e instanceof ApiException && !((ApiException) e).isRetryable()) {
        return e instanceof NotFoundException || e instanceof InvalidArgumentException;
      }
      return false;
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) throws Exception {
      if (bigtableWriter != null) {
        try {
          bigtableWriter.close();
        } catch (IOException e) {
          // If the writer fails due to a batching exception, but no failures were detected
          // it means that error handling was enabled, and that errors were detected and routed
          // to the error queue. Bigtable will successfully write other failures in the batch,
          // so this exception should be ignored
          if (!(e.getCause() instanceof BatchingException)) {
            throw e;
          }
        }

        // Sanity check: ensure that all element futures are resolved. This should be already be the
        // case once bigtableWriter.close() finishes.
        try {
          CompletableFuture.allOf(outstandingWrites.toArray(new CompletableFuture<?>[0]))
              .get(1, TimeUnit.MINUTES);
        } catch (TimeoutException e) {
          throw new IllegalStateException(
              "Unexpected timeout waiting for element future to resolve after the writer was closed",
              e);
        }

        if (!reportedLineage) {
          bigtableWriter.reportLineage();
          reportedLineage = true;
        }
        bigtableWriter = null;
      }

      for (KV<BigtableWriteException, BoundedWindow> badRecord : badRecords) {
        try {
          badRecordRouter.route(
              c,
              badRecord.getKey().getRecord(),
              inputCoder,
              (Exception) badRecord.getKey().getCause(),
              "Failed to write malformed mutation to Bigtable",
              badRecord.getValue());
        } catch (Exception e) {
          failures.add(badRecord.getKey());
        }
      }

      checkForFailures();

      LOG.debug("Wrote {} records", recordsWritten);

      for (Map.Entry<BoundedWindow, Long> entry : seenWindows.entrySet()) {
        c.output(
            BigtableWriteResult.create(entry.getValue()),
            entry.getKey().maxTimestamp(),
            entry.getKey());
      }
    }

    @Teardown
    public void tearDown() throws IOException {
      // in case of exception in processElement, finishBundle will not be called
      // cleanup resources allocated in startBundle
      try {
        if (bigtableWriter != null) {
          bigtableWriter.close();
          bigtableWriter = null;
        }
      } finally {
        if (serviceEntry != null) {
          serviceEntry.close();
          serviceEntry = null;
        }
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      config.populateDisplayData(builder);
    }

    ///////////////////////////////////////////////////////////////////////////////
    private final BigtableConfig config;
    private final BigtableWriteOptions writeOptions;
    private BigtableService.Writer bigtableWriter;
    private long recordsWritten;
    private final ConcurrentLinkedQueue<BigtableWriteException> failures;
    private Map<BoundedWindow, Long> seenWindows;

    /** If any write has asynchronously failed, fail the bundle with a useful error. */
    private void checkForFailures() throws IOException {
      // Note that this function is never called by multiple threads and is the only place that
      // we remove from failures, so this code is safe.
      if (failures.isEmpty()) {
        return;
      }

      StringBuilder logEntry = new StringBuilder();
      int i = 0;
      List<BigtableWriteException> suppressed = Lists.newArrayList();
      for (; i < 10 && !failures.isEmpty(); ++i) {
        BigtableWriteException exc = failures.remove();
        logEntry.append("\n").append(exc.getMessage());
        if (exc.getCause() != null) {
          logEntry.append(": ").append(exc.getCause().getMessage());
        }
        suppressed.add(exc);
      }
      String message =
          String.format(
              "At least %d errors occurred writing to Bigtable. First %d errors: %s",
              i + failures.size(), i, logEntry.toString());
      LOG.error(message);
      IOException exception = new IOException(message);
      for (BigtableWriteException e : suppressed) {
        exception.addSuppressed(e);
      }
      throw exception;
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////
  /** Disallow construction of utility class. */
  private BigtableIO() {}

  private static ByteKey makeByteKey(ByteString key) {
    return ByteKey.copyFrom(key.asReadOnlyByteBuffer());
  }

  static class BigtableSource extends BoundedSource<Row> {
    public BigtableSource(
        BigtableServiceFactory factory,
        BigtableServiceFactory.ConfigId configId,
        BigtableConfig config,
        BigtableReadOptions readOptions,
        @Nullable Long estimatedSizeBytes) {
      this.factory = factory;
      this.configId = configId;
      this.config = config;
      this.readOptions = readOptions;
      this.estimatedSizeBytes = estimatedSizeBytes;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(BigtableSource.class)
          .add("config", config)
          .add("readOptions", readOptions)
          .add("estimatedSizeBytes", estimatedSizeBytes)
          .toString();
    }

    ////// Private state and internal implementation details //////
    private final BigtableConfig config;
    private final BigtableReadOptions readOptions;
    private @Nullable Long estimatedSizeBytes;
    private transient boolean reportedLineage;

    private final BigtableServiceFactory.ConfigId configId;

    private final BigtableServiceFactory factory;

    /** Creates a new {@link BigtableSource} with just one {@link ByteKeyRange}. */
    protected BigtableSource withSingleRange(ByteKeyRange range) {
      checkArgument(range != null, "range can not be null");
      return new BigtableSource(
          factory, configId, config, readOptions.withKeyRange(range), estimatedSizeBytes);
    }

    protected BigtableSource withEstimatedSizeBytes(Long estimatedSizeBytes) {
      checkArgument(estimatedSizeBytes != null, "estimatedSizeBytes can not be null");
      return new BigtableSource(factory, configId, config, readOptions, estimatedSizeBytes);
    }

    /**
     * Makes an API call to the Cloud Bigtable service that gives information about tablet key
     * boundaries and estimated sizes. We can use these samples to ensure that splits are on
     * different tablets, and possibly generate sub-splits within tablets.
     */
    private List<KeyOffset> getSampleRowKeys(PipelineOptions pipelineOptions) throws IOException {
      try (BigtableServiceFactory.BigtableServiceEntry serviceEntry =
          factory.getServiceForReading(configId, config, readOptions, pipelineOptions)) {
        return serviceEntry.getService().getSampleRowKeys(this);
      }
    }

    private static final long MAX_SPLIT_COUNT = 15_360L;

    @Override
    public List<BigtableSource> split(long desiredBundleSizeBytes, PipelineOptions options)
        throws Exception {
      // Update the desiredBundleSizeBytes in order to limit the
      // number of splits to maximumNumberOfSplits.
      long maximumNumberOfSplits = 4000;
      long sizeEstimate = getEstimatedSizeBytes(options);
      desiredBundleSizeBytes =
          Math.max(
              sizeEstimate / maximumNumberOfSplits,
              // BoundedReadEvaluatorFactory may provide us with a desiredBundleSizeBytes of 0
              // https://github.com/apache/beam/issues/28793
              Math.max(1, desiredBundleSizeBytes));

      // Delegate to testable helper.
      List<BigtableSource> splits =
          splitBasedOnSamples(desiredBundleSizeBytes, getSampleRowKeys(options));

      // Reduce the splits.
      List<BigtableSource> reduced = reduceSplits(splits, options, MAX_SPLIT_COUNT);
      // Randomize the result before returning an immutable copy of the splits, the default behavior
      // may lead to multiple workers hitting the same tablet.
      Collections.shuffle(reduced);
      return ImmutableList.copyOf(reduced);
    }

    /** Returns a mutable list of reduced splits. */
    @VisibleForTesting
    protected List<BigtableSource> reduceSplits(
        List<BigtableSource> splits, PipelineOptions options, long maxSplitCounts)
        throws IOException {
      int numberToCombine = (int) ((splits.size() + maxSplitCounts - 1) / maxSplitCounts);
      if (splits.size() < maxSplitCounts || numberToCombine < 2) {
        return new ArrayList<>(splits);
      }
      List<BigtableSource> reducedSplits = new ArrayList<>();
      List<ByteKeyRange> previousSourceRanges = new ArrayList<>();
      int counter = 0;
      long size = 0;
      for (BigtableSource source : splits) {
        if (counter == numberToCombine
            || !checkRangeAdjacency(previousSourceRanges, source.getRanges())) {
          reducedSplits.add(
              new BigtableSource(
                  factory,
                  configId,
                  config,
                  readOptions.withKeyRanges(previousSourceRanges),
                  size));
          counter = 0;
          size = 0;
          previousSourceRanges = new ArrayList<>();
        }
        previousSourceRanges.addAll(source.getRanges());
        previousSourceRanges = mergeRanges(previousSourceRanges);
        size += source.getEstimatedSizeBytes(options);
        counter++;
      }
      if (size > 0) {
        reducedSplits.add(
            new BigtableSource(
                factory, configId, config, readOptions.withKeyRanges(previousSourceRanges), size));
      }
      return reducedSplits;
    }

    /**
     * Helper to validate range Adjacency. Ranges are considered adjacent if
     * [1..100][100..200][200..300]
     */
    private static boolean checkRangeAdjacency(
        List<ByteKeyRange> ranges, List<ByteKeyRange> otherRanges) {
      checkArgument(ranges != null || otherRanges != null, "Both ranges cannot be null.");
      ImmutableList.Builder<ByteKeyRange> mergedRanges = ImmutableList.builder();
      if (ranges != null) {
        mergedRanges.addAll(ranges);
      }
      if (otherRanges != null) {
        mergedRanges.addAll(otherRanges);
      }
      return checkRangeAdjacency(mergedRanges.build());
    }

    /**
     * Helper to validate range Adjacency. Ranges are considered adjacent if
     * [1..100][100..200][200..300]
     */
    private static boolean checkRangeAdjacency(List<ByteKeyRange> ranges) {
      int index = 0;
      if (ranges.size() < 2) {
        return true;
      }
      ByteKey lastEndKey = ranges.get(index++).getEndKey();
      while (index < ranges.size()) {
        ByteKeyRange currentKeyRange = ranges.get(index++);
        if (!lastEndKey.equals(currentKeyRange.getStartKey())) {
          return false;
        }
        lastEndKey = currentKeyRange.getEndKey();
      }
      return true;
    }

    /**
     * Helper to combine/merge ByteKeyRange Ranges should only be merged if they are adjacent ex.
     * [1..100][100..200][200..300] will result in [1..300] Note: this method will not check for
     * adjacency see {@link #checkRangeAdjacency(List)}
     */
    private static List<ByteKeyRange> mergeRanges(List<ByteKeyRange> ranges) {
      List<ByteKeyRange> response = new ArrayList<>();
      if (ranges.size() < 2) {
        response.add(ranges.get(0));
      } else {
        response.add(
            ByteKeyRange.of(
                ranges.get(0).getStartKey(), ranges.get(ranges.size() - 1).getEndKey()));
      }
      return response;
    }

    /** Helper that splits this source into bundles based on Cloud Bigtable sampled row keys. */
    private List<BigtableSource> splitBasedOnSamples(
        long desiredBundleSizeBytes, List<KeyOffset> sampleRowKeys) {
      // There are no regions, or no samples available. Just scan the entire range.
      if (sampleRowKeys.isEmpty()) {
        LOG.info("Not splitting source {} because no sample row keys are available.", this);
        return Collections.singletonList(this);
      }
      LOG.info(
          "About to split into bundles of size {} with sampleRowKeys length {} first element {}",
          desiredBundleSizeBytes,
          sampleRowKeys.size(),
          sampleRowKeys.get(0));

      ImmutableList.Builder<BigtableSource> splits = ImmutableList.builder();
      for (ByteKeyRange range : getRanges()) {
        splits.addAll(splitRangeBasedOnSamples(desiredBundleSizeBytes, sampleRowKeys, range));
      }
      return splits.build();
    }

    /**
     * Helper that splits a {@code ByteKeyRange} into bundles based on Cloud Bigtable sampled row
     * keys.
     */
    private List<BigtableSource> splitRangeBasedOnSamples(
        long desiredBundleSizeBytes, List<KeyOffset> sampleRowKeys, ByteKeyRange range) {

      // Loop through all sampled responses and generate splits from the ones that overlap the
      // scan range. The main complication is that we must track the end range of the previous
      // sample to generate good ranges.
      ByteKey lastEndKey = ByteKey.EMPTY;
      long lastOffset = 0;
      ImmutableList.Builder<BigtableSource> splits = ImmutableList.builder();
      for (KeyOffset keyOffset : sampleRowKeys) {
        ByteKey responseEndKey = makeByteKey(keyOffset.getKey());
        long responseOffset = keyOffset.getOffsetBytes();
        checkState(
            responseOffset >= lastOffset,
            "Expected response byte offset %s to come after the last offset %s",
            responseOffset,
            lastOffset);

        if (!range.overlaps(ByteKeyRange.of(lastEndKey, responseEndKey))) {
          // This region does not overlap the scan, so skip it.
          lastOffset = responseOffset;
          lastEndKey = responseEndKey;
          continue;
        }

        // Calculate the beginning of the split as the larger of startKey and the end of the last
        // split. Unspecified start is smallest key so is correctly treated as earliest key.
        ByteKey splitStartKey = lastEndKey;
        if (splitStartKey.compareTo(range.getStartKey()) < 0) {
          splitStartKey = range.getStartKey();
        }

        // Calculate the end of the split as the smaller of endKey and the end of this sample. Note
        // that range.containsKey handles the case when range.getEndKey() is empty.
        ByteKey splitEndKey = responseEndKey;
        if (!range.containsKey(splitEndKey)) {
          splitEndKey = range.getEndKey();
        }

        // We know this region overlaps the desired key range, and we know a rough estimate of its
        // size. Split the key range into bundle-sized chunks and then add them all as splits.
        long sampleSizeBytes = responseOffset - lastOffset;
        List<BigtableSource> subSplits =
            splitKeyRangeIntoBundleSizedSubranges(
                sampleSizeBytes,
                desiredBundleSizeBytes,
                ByteKeyRange.of(splitStartKey, splitEndKey));
        splits.addAll(subSplits);

        // Move to the next region.
        lastEndKey = responseEndKey;
        lastOffset = responseOffset;
      }

      // We must add one more region after the end of the samples if both these conditions hold:
      //  1. we did not scan to the end yet (lastEndKey is concrete, not 0-length).
      //  2. we want to scan to the end (endKey is empty) or farther (lastEndKey < endKey).
      if (!lastEndKey.isEmpty()
          && (range.getEndKey().isEmpty() || lastEndKey.compareTo(range.getEndKey()) < 0)) {
        splits.add(this.withSingleRange(ByteKeyRange.of(lastEndKey, range.getEndKey())));
      }

      List<BigtableSource> ret = splits.build();
      LOG.info("Generated {} splits. First split: {}", ret.size(), ret.get(0));
      return ret;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws IOException {
      // Delegate to testable helper.
      if (estimatedSizeBytes == null) {
        estimatedSizeBytes = getEstimatedSizeBytesBasedOnSamples(getSampleRowKeys(options));
      }
      return estimatedSizeBytes;
    }

    /**
     * Computes the estimated size in bytes based on the total size of all samples that overlap the
     * key ranges this source will scan.
     */
    private long getEstimatedSizeBytesBasedOnSamples(List<KeyOffset> samples) {
      long estimatedSizeBytes = 0;
      long lastOffset = 0;
      ByteKey currentStartKey = ByteKey.EMPTY;
      // Compute the total estimated size as the size of each sample that overlaps the scan range.
      // TODO: In future, Bigtable service may provide finer grained APIs, e.g., to sample given a
      // filter or to sample on a given key range.
      for (KeyOffset keyOffset : samples) {
        ByteKey currentEndKey = makeByteKey(keyOffset.getKey());
        long currentOffset = keyOffset.getOffsetBytes();
        if (!currentStartKey.isEmpty() && currentStartKey.equals(currentEndKey)) {
          // Skip an empty region.
          lastOffset = currentOffset;
          continue;
        } else {
          for (ByteKeyRange range : getRanges()) {
            if (range.overlaps(ByteKeyRange.of(currentStartKey, currentEndKey))) {
              estimatedSizeBytes += currentOffset - lastOffset;
              // We don't want to double our estimated size if two ranges overlap this sample
              // region, so exit early.
              break;
            }
          }
        }
        currentStartKey = currentEndKey;
        lastOffset = currentOffset;
      }
      return estimatedSizeBytes;
    }

    @Override
    public BoundedReader<Row> createReader(PipelineOptions options) throws IOException {
      return new BigtableReader(
          this, factory.getServiceForReading(configId, config, readOptions, options));
    }

    @Override
    public void validate() {
      if (!config.getValidate()) {
        LOG.debug("Validation is disabled");
        return;
      }

      ValueProvider<String> tableId = readOptions.getTableId();
      checkArgument(
          tableId != null && tableId.isAccessible() && !tableId.get().isEmpty(),
          "tableId was not supplied");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      builder.add(DisplayData.item("tableId", readOptions.getTableId()).withLabel("Table ID"));

      if (getRowFilter() != null) {
        builder.add(
            DisplayData.item("rowFilter", getRowFilter().toString()).withLabel("Table Row Filter"));
      }
    }

    @Override
    public Coder<Row> getOutputCoder() {
      return ProtoCoder.of(Row.class);
    }

    /** Helper that splits the specified range in this source into bundles. */
    private List<BigtableSource> splitKeyRangeIntoBundleSizedSubranges(
        long sampleSizeBytes, long desiredBundleSizeBytes, ByteKeyRange range) {
      // Catch the trivial cases. Split is small enough already, or this is the last region.
      LOG.debug(
          "Subsplit for sampleSizeBytes {} and desiredBundleSizeBytes {}",
          sampleSizeBytes,
          desiredBundleSizeBytes);
      if (sampleSizeBytes <= desiredBundleSizeBytes) {
        return Collections.singletonList(
            this.withSingleRange(ByteKeyRange.of(range.getStartKey(), range.getEndKey())));
      }

      checkArgument(
          sampleSizeBytes > 0, "Sample size %s bytes must be greater than 0.", sampleSizeBytes);
      checkArgument(
          desiredBundleSizeBytes > 0,
          "Desired bundle size %s bytes must be greater than 0.",
          desiredBundleSizeBytes);

      int splitCount = (int) Math.ceil(((double) sampleSizeBytes) / desiredBundleSizeBytes);
      List<ByteKey> splitKeys = range.split(splitCount);
      ImmutableList.Builder<BigtableSource> splits = ImmutableList.builder();
      Iterator<ByteKey> keys = splitKeys.iterator();
      ByteKey prev = keys.next();
      while (keys.hasNext()) {
        ByteKey next = keys.next();
        splits.add(
            this.withSingleRange(ByteKeyRange.of(prev, next))
                .withEstimatedSizeBytes(sampleSizeBytes / splitCount));
        prev = next;
      }
      return splits.build();
    }

    public BigtableReadOptions getReadOptions() {
      return readOptions;
    }

    public List<ByteKeyRange> getRanges() {
      return readOptions.getKeyRanges().get();
    }

    public @Nullable RowFilter getRowFilter() {
      ValueProvider<RowFilter> rowFilter = readOptions.getRowFilter();
      return rowFilter != null && rowFilter.isAccessible() ? rowFilter.get() : null;
    }

    public @Nullable Integer getMaxBufferElementCount() {
      return readOptions.getMaxBufferElementCount();
    }

    public ValueProvider<String> getTableId() {
      return readOptions.getTableId();
    }

    void reportLineageOnce(BigtableService.Reader reader) {
      if (!reportedLineage) {
        reader.reportLineage();
        reportedLineage = true;
      }
    }
  }

  private static class BigtableReader extends BoundedReader<Row> {
    // Thread-safety: source is protected via synchronization and is only accessed or modified
    // inside a synchronized block (or constructor, which is the same).
    private BigtableSource source;

    // Assign serviceEntry at construction time and clear it in close().
    @Nullable private BigtableServiceEntry serviceEntry;
    private BigtableService.Reader reader;
    private final ByteKeyRangeTracker rangeTracker;
    private long recordsReturned;

    public BigtableReader(BigtableSource source, BigtableServiceEntry service) {
      checkArgument(source.getRanges().size() == 1, "source must have exactly one key range");
      this.source = source;
      this.serviceEntry = service;
      rangeTracker = ByteKeyRangeTracker.of(source.getRanges().get(0));
    }

    @Override
    public boolean start() throws IOException {
      reader = serviceEntry.getService().createReader(getCurrentSource());
      boolean hasRecord =
          (reader.start()
                  && rangeTracker.tryReturnRecordAt(
                      true, makeByteKey(reader.getCurrentRow().getKey())))
              || rangeTracker.markDone();
      if (hasRecord) {
        ++recordsReturned;
        source.reportLineageOnce(reader);
      }
      return hasRecord;
    }

    @Override
    public synchronized BigtableSource getCurrentSource() {
      return source;
    }

    @Override
    public boolean advance() throws IOException {
      boolean hasRecord =
          (reader.advance()
                  && rangeTracker.tryReturnRecordAt(
                      true, makeByteKey(reader.getCurrentRow().getKey())))
              || rangeTracker.markDone();
      if (hasRecord) {
        ++recordsReturned;
      }
      return hasRecord;
    }

    @Override
    public Row getCurrent() throws NoSuchElementException {
      return reader.getCurrentRow();
    }

    @Override
    public void close() throws IOException {
      LOG.info("Closing reader after reading {} records.", recordsReturned);
      if (reader != null) {
        reader.close();
        reader = null;
      }
      if (serviceEntry != null) {
        serviceEntry.close();
        serviceEntry = null;
      }
    }

    @Override
    public final Double getFractionConsumed() {
      return rangeTracker.getFractionConsumed();
    }

    @Override
    public final long getSplitPointsConsumed() {
      return rangeTracker.getSplitPointsConsumed();
    }

    @Override
    public final @Nullable synchronized BigtableSource splitAtFraction(double fraction) {
      ByteKey splitKey;
      ByteKeyRange range = rangeTracker.getRange();
      try {
        splitKey = range.interpolateKey(fraction);
      } catch (RuntimeException e) {
        LOG.info("{}: Failed to interpolate key for fraction {}.", range, fraction, e);
        return null;
      }
      LOG.info("Proposing to split {} at fraction {} (key {})", rangeTracker, fraction, splitKey);
      BigtableSource primary;
      BigtableSource residual;
      try {
        primary = source.withSingleRange(ByteKeyRange.of(range.getStartKey(), splitKey));
        residual = source.withSingleRange(ByteKeyRange.of(splitKey, range.getEndKey()));
      } catch (RuntimeException e) {
        LOG.info(
            "{}: Interpolating for fraction {} yielded invalid split key {}.",
            rangeTracker.getRange(),
            fraction,
            splitKey,
            e);
        return null;
      }
      if (!rangeTracker.trySplitAtPosition(splitKey)) {
        return null;
      }
      this.source = primary;
      return residual;
    }
  }

  /** An exception that puts information about the failed record being written in its message. */
  static class BigtableWriteException extends IOException {

    private final KV<ByteString, Iterable<Mutation>> record;

    public BigtableWriteException(KV<ByteString, Iterable<Mutation>> record, Throwable cause) {
      super(
          String.format(
              "Error mutating row %s with mutations %s",
              record.getKey().toStringUtf8(), StringUtils.leftTruncate(record.getValue(), 100)),
          cause);
      this.record = record;
    }

    public KV<ByteString, Iterable<Mutation>> getRecord() {
      return record;
    }
  }
  /**
   * Overwrite options to determine what to do if change stream name is being reused and there
   * exists metadata of the same change stream name.
   */
  public enum ExistingPipelineOptions {
    // Don't start if there exists metadata of the same change stream name.
    FAIL_IF_EXISTS,
    // Pick up from where the previous pipeline left off. This will perform resumption at best
    // effort guaranteeing, at-least-once delivery. So it's likely that duplicate data, seen before
    // the pipeline was stopped, will be outputted. If previous pipeline doesn't exist, start a new
    // pipeline.
    RESUME_OR_NEW,
    // Same as RESUME_OR_NEW except if previous pipeline doesn't exist, don't start.
    RESUME_OR_FAIL,
    // This skips cleaning up previous pipeline metadata and starts a new pipeline. This should
    // only be used to skip cleanup in tests
    @VisibleForTesting
    SKIP_CLEANUP,
  }

  @AutoValue
  public abstract static class ReadChangeStream
      extends PTransform<PBegin, PCollection<KV<ByteString, ChangeStreamMutation>>> {

    private static final Duration DEFAULT_BACKLOG_REPLICATION_ADJUSTMENT =
        Duration.standardSeconds(30);

    static ReadChangeStream create() {
      BigtableConfig config = BigtableConfig.builder().setValidate(true).build();
      BigtableConfig metadataTableconfig = BigtableConfig.builder().setValidate(true).build();

      return new AutoValue_BigtableIO_ReadChangeStream.Builder()
          .setBigtableConfig(config)
          .setMetadataTableBigtableConfig(metadataTableconfig)
          .setValidateConfig(true)
          .build();
    }

    abstract BigtableConfig getBigtableConfig();

    abstract @Nullable String getTableId();

    abstract @Nullable Instant getStartTime();

    abstract @Nullable Instant getEndTime();

    abstract @Nullable String getChangeStreamName();

    abstract @Nullable ExistingPipelineOptions getExistingPipelineOptions();

    abstract BigtableConfig getMetadataTableBigtableConfig();

    abstract @Nullable String getMetadataTableId();

    abstract @Nullable Boolean getCreateOrUpdateMetadataTable();

    abstract @Nullable Duration getBacklogReplicationAdjustment();

    abstract @Nullable Boolean getValidateConfig();

    abstract ReadChangeStream.Builder toBuilder();

    /**
     * Returns a new {@link BigtableIO.ReadChangeStream} that will stream from the Cloud Bigtable
     * project indicated by given parameter, requires {@link #withInstanceId} to be called to
     * determine the instance.
     *
     * <p>Does not modify this object.
     */
    public ReadChangeStream withProjectId(String projectId) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder()
          .setBigtableConfig(config.withProjectId(StaticValueProvider.of(projectId)))
          .build();
    }

    /**
     * Returns a new {@link BigtableIO.ReadChangeStream} that will stream from the Cloud Bigtable
     * instance indicated by given parameter, requires {@link #withProjectId} to be called to
     * determine the project.
     *
     * <p>Does not modify this object.
     */
    public ReadChangeStream withInstanceId(String instanceId) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder()
          .setBigtableConfig(config.withInstanceId(StaticValueProvider.of(instanceId)))
          .build();
    }

    /**
     * Returns a new {@link BigtableIO.ReadChangeStream} that will stream from the specified table.
     *
     * <p>Does not modify this object.
     */
    public ReadChangeStream withTableId(String tableId) {
      return toBuilder().setTableId(tableId).build();
    }

    /**
     * Returns a new {@link BigtableIO.ReadChangeStream} that will stream from the cluster specified
     * by app profile id.
     *
     * <p>This must use single-cluster routing policy. If not setting a separate app profile for the
     * metadata table with {@link BigtableIO.ReadChangeStream#withMetadataTableAppProfileId}, this
     * app profile also needs to enable allow single-row transactions.
     *
     * <p>Does not modify this object.
     */
    public ReadChangeStream withAppProfileId(String appProfileId) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder()
          .setBigtableConfig(config.withAppProfileId(StaticValueProvider.of(appProfileId)))
          .build();
    }

    /**
     * Returns a new {@link BigtableIO.ReadChangeStream} that will start streaming at the specified
     * start time.
     *
     * <p>Does not modify this object.
     */
    public ReadChangeStream withStartTime(Instant startTime) {
      return toBuilder().setStartTime(startTime).build();
    }

    /** Used only for integration tests. Unsafe to use in production. */
    @VisibleForTesting
    ReadChangeStream withEndTime(Instant endTime) {
      return toBuilder().setEndTime(endTime).build();
    }

    /**
     * Returns a new {@link BigtableIO.ReadChangeStream} that uses changeStreamName as prefix for
     * the metadata table.
     *
     * <p>Does not modify this object.
     */
    public ReadChangeStream withChangeStreamName(String changeStreamName) {
      return toBuilder().setChangeStreamName(changeStreamName).build();
    }

    /**
     * Returns a new {@link BigtableIO.ReadChangeStream} that decides what to do if an existing
     * pipeline exists with the same change stream name.
     *
     * <p>Does not modify this object.
     */
    public ReadChangeStream withExistingPipelineOptions(
        ExistingPipelineOptions existingPipelineOptions) {
      return toBuilder().setExistingPipelineOptions(existingPipelineOptions).build();
    }

    /**
     * Returns a new {@link BigtableIO.ReadChangeStream} that will use the Cloud Bigtable project
     * indicated by given parameter to manage the metadata of the stream.
     *
     * <p>Optional: defaults to value from withProjectId
     *
     * <p>Does not modify this object.
     */
    public ReadChangeStream withMetadataTableProjectId(String projectId) {
      BigtableConfig config = getMetadataTableBigtableConfig();
      return toBuilder()
          .setMetadataTableBigtableConfig(config.withProjectId(StaticValueProvider.of(projectId)))
          .build();
    }

    /**
     * Returns a new {@link BigtableIO.ReadChangeStream} that will use the Cloud Bigtable instance
     * indicated by given parameter to manage the metadata of the stream.
     *
     * <p>Optional: defaults to value from withInstanceId
     *
     * <p>Does not modify this object.
     */
    public ReadChangeStream withMetadataTableInstanceId(String instanceId) {
      BigtableConfig config = getMetadataTableBigtableConfig();
      return toBuilder()
          .setMetadataTableBigtableConfig(config.withInstanceId(StaticValueProvider.of(instanceId)))
          .build();
    }

    /**
     * Returns a new {@link BigtableIO.ReadChangeStream} that will use specified table to store the
     * metadata of the stream.
     *
     * <p>Optional: defaults to value from withTableId
     *
     * <p>Does not modify this object.
     */
    public ReadChangeStream withMetadataTableTableId(String tableId) {
      return toBuilder().setMetadataTableId(tableId).build();
    }

    /**
     * Returns a new {@link BigtableIO.ReadChangeStream} that will use the cluster specified by app
     * profile id to store the metadata of the stream.
     *
     * <p>Optional: defaults to value from withAppProfileId
     *
     * <p>This must use single-cluster routing policy with allow single-row transactions enabled.
     *
     * <p>Does not modify this object.
     */
    public ReadChangeStream withMetadataTableAppProfileId(String appProfileId) {
      BigtableConfig config = getMetadataTableBigtableConfig();
      return toBuilder()
          .setMetadataTableBigtableConfig(
              config.withAppProfileId(StaticValueProvider.of(appProfileId)))
          .build();
    }

    /**
     * Returns a new {@link BigtableIO.ReadChangeStream} that overrides the config of data and/or
     * admin client for streaming changes and for managing the metadata. For testing purposes only.
     * Not intended for use.
     *
     * <p>Does not modify this object.
     */
    @VisibleForTesting
    ReadChangeStream withBigtableClientOverride(BigtableClientOverride clientOverride) {
      BigtableConfig config = getBigtableConfig();
      BigtableConfig metadataTableConfig = getMetadataTableBigtableConfig();
      return toBuilder()
          .setBigtableConfig(config.withBigtableClientOverride(clientOverride))
          .setMetadataTableBigtableConfig(
              metadataTableConfig.withBigtableClientOverride(clientOverride))
          .build();
    }

    /**
     * Returns a new {@link BigtableIO.ReadChangeStream} that, if set to true, will create or update
     * metadata table before launching pipeline. Otherwise, it is expected that a metadata table
     * with correct schema exists.
     *
     * <p>Optional: defaults to true
     *
     * <p>Does not modify this object.
     */
    public ReadChangeStream withCreateOrUpdateMetadataTable(boolean shouldCreate) {
      return toBuilder().setCreateOrUpdateMetadataTable(shouldCreate).build();
    }

    /**
     * Returns a new {@link BigtableIO.ReadChangeStream} that overrides the replication delay
     * adjustment duration with the provided duration.
     *
     * <p>Backlog is calculated for each partition using watermarkLag * throughput. Replication
     * delay holds back the watermark for each partition. This can cause the backlog to stay
     * persistently above dataflow's downscaling threshold (10 seconds) even when a pipeline is
     * caught up.
     *
     * <p>This adjusts the backlog downward to account for this. For unreplicated instances it can
     * be set to zero to upscale as quickly as possible.
     *
     * <p>Optional: defaults to 30 seconds.
     *
     * <p>Does not modify this object.
     */
    public ReadChangeStream withBacklogReplicationAdjustment(Duration adjustment) {
      return toBuilder().setBacklogReplicationAdjustment(adjustment).build();
    }

    /**
     * Disables validation that the table being read and the metadata table exists, and that the app
     * profile used is single cluster and single row transaction enabled. Set this option if the
     * caller does not have additional Bigtable permissions to validate the configurations.
     * <b>NOTE</b> this also disabled creating or updating the metadata table because that also
     * requires additional permissions, essentially setting {@link #withCreateOrUpdateMetadataTable}
     * to false.
     */
    public ReadChangeStream withoutValidation() {
      BigtableConfig config = getBigtableConfig();
      BigtableConfig metadataTableConfig = getMetadataTableBigtableConfig();
      return toBuilder()
          .setBigtableConfig(config.withValidate(false))
          .setMetadataTableBigtableConfig(metadataTableConfig.withValidate(false))
          .setValidateConfig(false)
          .build();
    }

    @Override
    public void validate(PipelineOptions options) {
      if (getBigtableConfig().getValidate()) {
        try (BigtableChangeStreamAccessor bigtableChangeStreamAccessor =
            BigtableChangeStreamAccessor.getOrCreate(getBigtableConfig())) {
          checkArgument(
              bigtableChangeStreamAccessor.getTableAdminClient().exists(getTableId()),
              "Change Stream table %s does not exist",
              getTableId());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    // Validate the app profile is single cluster and allows single row transactions.
    private void validateAppProfile(
        MetadataTableAdminDao metadataTableAdminDao, String appProfileId) {
      checkArgument(metadataTableAdminDao != null);
      checkArgument(
          metadataTableAdminDao.isAppProfileSingleClusterAndTransactional(appProfileId),
          "App profile id '"
              + appProfileId
              + "' provided to access metadata table needs to use single-cluster routing policy"
              + " and allow single-row transactions.");
    }

    // Update metadata table schema if allowed and required.
    private void createOrUpdateMetadataTable(
        MetadataTableAdminDao metadataTableAdminDao, String metadataTableId) {
      boolean shouldCreateOrUpdateMetadataTable = true;
      if (getCreateOrUpdateMetadataTable() != null) {
        shouldCreateOrUpdateMetadataTable = getCreateOrUpdateMetadataTable();
      }
      // Only try to create or update metadata table if option is set to true. Otherwise, just
      // check if the table exists.
      if (shouldCreateOrUpdateMetadataTable && metadataTableAdminDao.createMetadataTable()) {
        LOG.info("Created metadata table: " + metadataTableId);
      }
    }

    @Override
    public PCollection<KV<ByteString, ChangeStreamMutation>> expand(PBegin input) {
      BigtableConfig bigtableConfig = getBigtableConfig();
      checkArgument(
          bigtableConfig != null,
          "BigtableIO ReadChangeStream is missing required configurations fields.");
      bigtableConfig.validate();
      checkArgument(getTableId() != null, "Missing required tableId field.");

      if (bigtableConfig.getAppProfileId() == null
          || bigtableConfig.getAppProfileId().get().isEmpty()) {
        bigtableConfig = bigtableConfig.withAppProfileId(StaticValueProvider.of("default"));
      }

      BigtableConfig metadataTableConfig = getMetadataTableBigtableConfig();
      if (metadataTableConfig.getProjectId() == null
          || metadataTableConfig.getProjectId().get().isEmpty()) {
        metadataTableConfig = metadataTableConfig.withProjectId(bigtableConfig.getProjectId());
      }
      if (metadataTableConfig.getInstanceId() == null
          || metadataTableConfig.getInstanceId().get().isEmpty()) {
        metadataTableConfig = metadataTableConfig.withInstanceId(bigtableConfig.getInstanceId());
      }
      String metadataTableId = getMetadataTableId();
      if (metadataTableId == null || metadataTableId.isEmpty()) {
        metadataTableId = MetadataTableAdminDao.DEFAULT_METADATA_TABLE_NAME;
      }
      if (metadataTableConfig.getAppProfileId() == null
          || metadataTableConfig.getAppProfileId().get().isEmpty()) {
        metadataTableConfig =
            metadataTableConfig.withAppProfileId(bigtableConfig.getAppProfileId());
      }

      Instant startTime = getStartTime();
      if (startTime == null) {
        startTime = Instant.now();
      }
      String changeStreamName = getChangeStreamName();
      if (changeStreamName == null || changeStreamName.isEmpty()) {
        changeStreamName = UniqueIdGenerator.generateRowKeyPrefix();
      }
      ExistingPipelineOptions existingPipelineOptions = getExistingPipelineOptions();
      if (existingPipelineOptions == null) {
        existingPipelineOptions = ExistingPipelineOptions.FAIL_IF_EXISTS;
      }

      Duration backlogReplicationAdjustment = getBacklogReplicationAdjustment();
      if (backlogReplicationAdjustment == null) {
        backlogReplicationAdjustment = DEFAULT_BACKLOG_REPLICATION_ADJUSTMENT;
      }

      ActionFactory actionFactory = new ActionFactory();
      ChangeStreamMetrics metrics = new ChangeStreamMetrics();
      DaoFactory daoFactory =
          new DaoFactory(
              bigtableConfig, metadataTableConfig, getTableId(), metadataTableId, changeStreamName);

      // Validate the configuration is correct before creating the pipeline, if required.
      try {
        MetadataTableAdminDao metadataTableAdminDao = daoFactory.getMetadataTableAdminDao();
        boolean validateConfig = true;
        if (getValidateConfig() != null) {
          validateConfig = getValidateConfig();
        }
        // Validate app profile and create metadata table if validate is required.
        if (validateConfig) {
          createOrUpdateMetadataTable(metadataTableAdminDao, metadataTableId);
          validateAppProfile(metadataTableAdminDao, metadataTableConfig.getAppProfileId().get());
        }
        // Validate metadata table if validate is required. We validate metadata table after
        // createOrUpdateMetadataTable because if the metadata doesn't exist, we have to run
        // createOrUpdateMetadataTable to create the metadata table.
        if (metadataTableConfig.getValidate()) {
          checkArgument(
              metadataTableAdminDao.doesMetadataTableExist(),
              "Metadata table does not exist: " + metadataTableAdminDao.getTableId());
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        daoFactory.close();
      }

      InitializeDoFn initializeDoFn =
          new InitializeDoFn(daoFactory, startTime, existingPipelineOptions);
      DetectNewPartitionsDoFn detectNewPartitionsDoFn =
          new DetectNewPartitionsDoFn(getEndTime(), actionFactory, daoFactory, metrics);
      ReadChangeStreamPartitionDoFn readChangeStreamPartitionDoFn =
          new ReadChangeStreamPartitionDoFn(
              daoFactory, actionFactory, metrics, backlogReplicationAdjustment);

      PCollection<KV<ByteString, ChangeStreamRecord>> readChangeStreamOutput =
          input
              .apply(Impulse.create())
              .apply("Initialize", ParDo.of(initializeDoFn))
              .apply("DetectNewPartition", ParDo.of(detectNewPartitionsDoFn))
              .apply("ReadChangeStreamPartition", ParDo.of(readChangeStreamPartitionDoFn));

      Coder<KV<ByteString, ChangeStreamRecord>> outputCoder = readChangeStreamOutput.getCoder();
      CoderSizeEstimator<KV<ByteString, ChangeStreamRecord>> sizeEstimator =
          new CoderSizeEstimator<>(outputCoder);
      readChangeStreamPartitionDoFn.setSizeEstimator(sizeEstimator);

      return readChangeStreamOutput.apply(
          "FilterForMutation", ParDo.of(new FilterForMutationDoFn()));
    }

    @AutoValue.Builder
    abstract static class Builder {

      abstract ReadChangeStream.Builder setBigtableConfig(BigtableConfig bigtableConfig);

      abstract ReadChangeStream.Builder setTableId(String tableId);

      abstract ReadChangeStream.Builder setMetadataTableBigtableConfig(
          BigtableConfig bigtableConfig);

      abstract ReadChangeStream.Builder setMetadataTableId(String metadataTableId);

      abstract ReadChangeStream.Builder setStartTime(Instant startTime);

      abstract ReadChangeStream.Builder setEndTime(Instant endTime);

      abstract ReadChangeStream.Builder setChangeStreamName(String changeStreamName);

      abstract ReadChangeStream.Builder setExistingPipelineOptions(
          ExistingPipelineOptions existingPipelineOptions);

      abstract ReadChangeStream.Builder setCreateOrUpdateMetadataTable(boolean shouldCreate);

      abstract ReadChangeStream.Builder setBacklogReplicationAdjustment(Duration adjustment);

      abstract ReadChangeStream.Builder setValidateConfig(boolean validateConfig);

      abstract ReadChangeStream build();
    }
  }

  /**
   * Utility method to create or update Read Change Stream metadata table. This requires Bigtable
   * table create permissions. This method is useful if the pipeline isn't granted permissions to
   * create Bigtable tables. Run this method with correct permissions to create the metadata table,
   * which is required to read Bigtable change streams. This method only needs to be run once, and
   * the metadata table can be reused for all pipelines.
   *
   * @param projectId project id of the metadata table, usually the same as the project of the table
   *     being streamed
   * @param instanceId instance id of the metadata table, usually the same as the instance of the
   *     table being streamed
   * @param tableId name of the metadata table, leave it null or empty to use default.
   * @return true if the table was successfully created. Otherwise, false.
   */
  public static boolean createOrUpdateReadChangeStreamMetadataTable(
      String projectId, String instanceId, @Nullable String tableId) throws IOException {
    BigtableConfig bigtableConfig =
        BigtableConfig.builder()
            .setValidate(true)
            .setProjectId(StaticValueProvider.of(projectId))
            .setInstanceId(StaticValueProvider.of(instanceId))
            .setAppProfileId(
                StaticValueProvider.of(
                    "default")) // App profile is not used. It's only required for data API.
            .build();

    if (tableId == null || tableId.isEmpty()) {
      tableId = MetadataTableAdminDao.DEFAULT_METADATA_TABLE_NAME;
    }

    DaoFactory daoFactory = new DaoFactory(null, bigtableConfig, null, tableId, null);

    try {
      MetadataTableAdminDao metadataTableAdminDao = daoFactory.getMetadataTableAdminDao();

      // Only try to create or update metadata table if option is set to true. Otherwise, just
      // check if the table exists.
      if (metadataTableAdminDao.createMetadataTable()) {
        LOG.info("Created metadata table: " + metadataTableAdminDao.getTableId());
      }
      return metadataTableAdminDao.doesMetadataTableExist();
    } finally {
      daoFactory.close();
    }
  }
}
