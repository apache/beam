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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.ServiceFactory;
import com.google.cloud.ServiceOptions;
import com.google.cloud.spanner.AbortedException;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Experimental {@link PTransform Transforms} for reading from and writing to <a
 * href="https://cloud.google.com/spanner">Google Cloud Spanner</a>.
 *
 * <h3>Reading from Cloud Spanner</h3>
 *
 * <p>This functionality is not yet implemented.
 *
 * <h3>Writing to Cloud Spanner</h3>
 *
 * <p>The Cloud Spanner {@link SpannerIO.Write} transform writes to Cloud Spanner by executing a
 * collection of input row {@link Mutation Mutations}. The mutations grouped into batches for
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
 * mutations.apply(
 *     "Write", SpannerIO.write().withInstanceId("instance").withDatabaseId("database"));
 * }</pre>
 *
 * <p>The default size of the batch is set to 1MB, to override this use {@link
 * Write#withBatchSizeBytes(long)}. Setting batch size to a small value or zero practically disables
 * batching.
 *
 * <p>The transform does not provide same transactional guarantees as Cloud Spanner. In particular,
 *
 * <ul>
 *   <li>Mutations are not submitted atomically;
 *   <li>A mutation is applied at least once;
 *   <li>If the pipeline was unexpectedly stopped, mutations that were already applied will not get
 *       rolled back.
 * </ul>
 *
 * <p>Use {@link MutationGroup} to ensure that a small set mutations is bundled together. It is
 * guaranteed that mutations in a group are submitted in the same transaction. Build
 * {@link SpannerIO.Write} transform, and call {@link Write#grouped()} method. It will return a
 * transformation that can be applied to a PCollection of MutationGroup.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class SpannerIO {

  private static final long DEFAULT_BATCH_SIZE_BYTES = 1024 * 1024; // 1 MB

  /**
   * Creates an uninitialized instance of {@link Write}. Before use, the {@link Write} must be
   * configured with a {@link Write#withInstanceId} and {@link Write#withDatabaseId} that identify
   * the Cloud Spanner database being written.
   */
  @Experimental
  public static Write write() {
    return new AutoValue_SpannerIO_Write.Builder()
        .setBatchSizeBytes(DEFAULT_BATCH_SIZE_BYTES)
        .build();
  }

  /**
   * A {@link PTransform} that writes {@link Mutation} objects to Google Cloud Spanner.
   *
   * @see SpannerIO
   */
  @Experimental(Experimental.Kind.SOURCE_SINK)
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<Mutation>, PDone> {

    @Nullable
    abstract ValueProvider<String> getProjectId();

    @Nullable
    abstract ValueProvider<String> getInstanceId();

    @Nullable
    abstract ValueProvider<String> getDatabaseId();

    abstract long getBatchSizeBytes();

    @Nullable
    @VisibleForTesting
    abstract ServiceFactory<Spanner, SpannerOptions> getServiceFactory();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setProjectId(ValueProvider<String> projectId);

      abstract Builder setInstanceId(ValueProvider<String> instanceId);

      abstract Builder setDatabaseId(ValueProvider<String> databaseId);

      abstract Builder setBatchSizeBytes(long batchSizeBytes);

      @VisibleForTesting
      abstract Builder setServiceFactory(ServiceFactory<Spanner, SpannerOptions> serviceFactory);

      abstract Write build();
    }

    /**
     * Returns a new {@link SpannerIO.Write} that will write to the specified Cloud Spanner project.
     *
     * <p>Does not modify this object.
     */
    public Write withProjectId(String projectId) {
      return withProjectId(ValueProvider.StaticValueProvider.of(projectId));
    }

    public Write withProjectId(ValueProvider<String> projectId) {
      return toBuilder().setProjectId(projectId).build();
    }

    /**
     * Returns a new {@link SpannerIO.Write} that will write to the specified Cloud Spanner
     * instance.
     *
     * <p>Does not modify this object.
     */
    public Write withInstanceId(String instanceId) {
      return withInstanceId(ValueProvider.StaticValueProvider.of(instanceId));
    }

    public Write withInstanceId(ValueProvider<String> instanceId) {
      return toBuilder().setInstanceId(instanceId).build();
    }

    /**
     * Returns a new {@link SpannerIO.Write} with a new batch size limit.
     *
     * <p>Does not modify this object.
     */
    public Write withBatchSizeBytes(long batchSizeBytes) {
      return toBuilder().setBatchSizeBytes(batchSizeBytes).build();
    }

    /**
     * Returns a new {@link SpannerIO.Write} that will write to the specified Cloud Spanner
     * database.
     *
     * <p>Does not modify this object.
     */
    public Write withDatabaseId(String databaseId) {
      return withDatabaseId(ValueProvider.StaticValueProvider.of(databaseId));
    }

    public Write withDatabaseId(ValueProvider<String> databaseId) {
      return toBuilder().setDatabaseId(databaseId).build();
    }

    /**
     * Same transform but can be applied to {@link PCollection} of {@link MutationGroup}.
     */
    public WriteGrouped grouped() {
      return new WriteGrouped(this);
    }

    @VisibleForTesting
    Write withServiceFactory(ServiceFactory<Spanner, SpannerOptions> serviceFactory) {
      return toBuilder().setServiceFactory(serviceFactory).build();
    }

    @Override
    public void validate(PipelineOptions options) {
      checkNotNull(
          getInstanceId(),
          "SpannerIO.write() requires instance id to be set with withInstanceId method");
      checkNotNull(
          getDatabaseId(),
          "SpannerIO.write() requires database id to be set with withDatabaseId method");
    }

    @Override
    public PDone expand(PCollection<Mutation> input) {
      input
          .apply("To mutation group", ParDo.of(new ToMutationGroupFn()))
          .apply("Write mutations to Cloud Spanner", ParDo.of(new SpannerWriteGroupFn(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("projectId", getProjectId()).withLabel("Output Project"))
          .addIfNotNull(
              DisplayData.item("instanceId", getInstanceId()).withLabel("Output Instance"))
          .addIfNotNull(
              DisplayData.item("databaseId", getDatabaseId()).withLabel("Output Database"))
          .add(DisplayData.item("batchSizeBytes", getBatchSizeBytes())
              .withLabel("Batch Size in Bytes"));
      if (getServiceFactory() != null) {
        builder.addIfNotNull(
            DisplayData.item("serviceFactory", getServiceFactory().getClass().getName())
                .withLabel("Service Factory"));
      }
    }
  }

  /** Same as {@link Write} but supports grouped mutations. */
  public static class WriteGrouped extends PTransform<PCollection<MutationGroup>, PDone> {
    private final Write spec;

    public WriteGrouped(Write spec) {
      this.spec = spec;
    }

    @Override public PDone expand(PCollection<MutationGroup> input) {
      input.apply("Write mutations to Cloud Spanner", ParDo.of(new SpannerWriteGroupFn(spec)));
      return PDone.in(input.getPipeline());
    }
  }

  private static class ToMutationGroupFn extends DoFn<Mutation, MutationGroup> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Mutation value = c.element();
      c.output(MutationGroup.create(value));
    }
  }

  /** Batches together and writes mutations to Google Cloud Spanner. */
  @VisibleForTesting
  static class SpannerWriteGroupFn extends DoFn<MutationGroup, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(SpannerWriteGroupFn.class);
    private final Write spec;
    private transient Spanner spanner;
    private transient DatabaseClient dbClient;
    // Current batch of mutations to be written.
    private List<MutationGroup> mutations;
    private long batchSizeBytes = 0;

    private static final int MAX_RETRIES = 5;
    private static final FluentBackoff BUNDLE_WRITE_BACKOFF =
        FluentBackoff.DEFAULT
            .withMaxRetries(MAX_RETRIES)
            .withInitialBackoff(Duration.standardSeconds(5));

    @VisibleForTesting SpannerWriteGroupFn(Write spec) {
      this.spec = spec;
    }

    @Setup
    public void setup() throws Exception {
      SpannerOptions spannerOptions = getSpannerOptions();
      spanner = spannerOptions.getService();
      dbClient = spanner.getDatabaseClient(
          DatabaseId.of(projectId(), spec.getInstanceId().get(), spec.getDatabaseId().get()));
      mutations = new ArrayList<>();
      batchSizeBytes = 0;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      MutationGroup m = c.element();
      mutations.add(m);
      batchSizeBytes += MutationSizeEstimator.sizeOf(m);
      if (batchSizeBytes >= spec.getBatchSizeBytes()) {
        flushBatch();
      }
    }

    private String projectId() {
      return spec.getProjectId() == null
          ? ServiceOptions.getDefaultProjectId()
          : spec.getProjectId().get();
    }

    @FinishBundle
    public void finishBundle() throws Exception {
      if (!mutations.isEmpty()) {
        flushBatch();
      }
    }

    @Teardown
    public void teardown() throws Exception {
      if (spanner == null) {
        return;
      }
      spanner.close();
      spanner = null;
    }

    private SpannerOptions getSpannerOptions() {
      SpannerOptions.Builder spannerOptionsBuider = SpannerOptions.newBuilder();
      if (spec.getServiceFactory() != null) {
        spannerOptionsBuider.setServiceFactory(spec.getServiceFactory());
      }
      if (spec.getProjectId() != null) {
        spannerOptionsBuider.setProjectId(spec.getProjectId().get());
      }
      return spannerOptionsBuider.build();
    }

    /**
     * Writes a batch of mutations to Cloud Spanner.
     *
     * <p>If a commit fails, it will be retried up to {@link #MAX_RETRIES} times. If the retry limit
     * is exceeded, the last exception from Cloud Spanner will be thrown.
     *
     * @throws AbortedException if the commit fails or IOException or InterruptedException if
     *     backing off between retries fails.
     */
    private void flushBatch() throws AbortedException, IOException, InterruptedException {
      LOG.debug("Writing batch of {} mutations", mutations.size());
      Sleeper sleeper = Sleeper.DEFAULT;
      BackOff backoff = BUNDLE_WRITE_BACKOFF.backoff();

      while (true) {
        // Batch upsert rows.
        try {
          dbClient.writeAtLeastOnce(Iterables.concat(mutations));

          // Break if the commit threw no exception.
          break;
        } catch (AbortedException exception) {
          // Only log the code and message for potentially-transient errors. The entire exception
          // will be propagated upon the last retry.
          LOG.error(
              "Error writing to Spanner ({}): {}", exception.getCode(), exception.getMessage());
          if (!BackOffUtils.next(sleeper, backoff)) {
            LOG.error("Aborting after {} retries.", MAX_RETRIES);
            throw exception;
          }
        }
      }
      LOG.debug("Successfully wrote {} mutations", mutations.size());
      mutations = new ArrayList<>();
      batchSizeBytes = 0;
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      spec.populateDisplayData(builder);
    }
  }

  private SpannerIO() {} // Prevent construction.
}
