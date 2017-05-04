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

import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;

import com.google.cloud.spanner.AbortedException;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




/**
 *  <a href="https://cloud.google.com/spanner/">Google Cloud Spanner</a> connectors.
 *
 * <h3>Reading from Cloud Spanner</h3>
 * <strong>Status: Not implemented.</strong>
 *
 * <h3>Writing to Cloud Spanner</h3>
 * <strong>Status: Experimental.</strong>
 *
 * <p>{@link SpannerIO#writeTo} batches together and concurrently writes a set of {@link Mutation}s.
 * To configure Cloud Spanner sink, you must apply {@link SpannerIO#writeTo} transform to
 * {@link PCollection} and specify instance and database identifiers.
 * For example, following code sketches out a pipeline that imports data from the CSV file to Cloud
 * Spanner.
 *
 * <pre>{@code
 *
 * Pipeline p = ...;
 * // Read the CSV file.
 * PCollection<String> lines = p.apply("Read CSV file", TextIO.Read.from(options.getInput()));
 * // Parse the line and convert to mutation.
 * PCollection<Mutation> mutations = lines.apply("Parse CSV", parseFromCsv());
 * // Write mutations.
 * mutations.apply("Write", SpannerIO.writeTo(options.getInstanceId(), options.getDatabaseId()));
 * p.run();
 *
 * }</pre>

 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class SpannerIO {

  private SpannerIO() {
  }

  @VisibleForTesting
  static final int SPANNER_MUTATIONS_PER_COMMIT_LIMIT = 20000;

    /**
     * Creates an instance of {@link Writer}. Use {@link Writer#withBatchSize} to limit the batch
     * size.
     */
  public static Writer writeTo(String instanceId, String databaseId) {
    return new Writer(instanceId, databaseId, SPANNER_MUTATIONS_PER_COMMIT_LIMIT);
  }

  /**
   * A {@link PTransform} that writes {@link Mutation} objects to Cloud Spanner.
   *
   * @see SpannerIO
   */
  public static class Writer extends PTransform<PCollection<Mutation>, PDone> {

    private final String instanceId;
    private final String databaseId;
    private int batchSize;

    Writer(String instanceId, String databaseId, int batchSize) {
      this.instanceId = instanceId;
      this.databaseId = databaseId;
      this.batchSize = batchSize;
    }

    /**
     * Returns a new {@link Writer} with a limit on the number of mutations per batch.
     * Defaults to {@link SpannerIO#SPANNER_MUTATIONS_PER_COMMIT_LIMIT}.
     */
    public Writer withBatchSize(Integer batchSize) {
      return new Writer(instanceId, databaseId, batchSize);
    }

    @Override
    public PDone expand(PCollection<Mutation> input) {
      input.apply("Write mutations to Spanner", ParDo.of(
              new SpannerWriterFn(instanceId, databaseId, batchSize)));

      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PCollection<Mutation> input) {
      checkNotNull(instanceId, "instanceId");
      checkNotNull(databaseId, "databaseId");
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("instanceId", instanceId)
          .add("databaseId", databaseId)
          .toString();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("instanceId", instanceId)
              .withLabel("Output Instance"))
          .addIfNotNull(DisplayData.item("databaseId", databaseId)
              .withLabel("Output Database"));
    }

  }


  /**
   * {@link DoFn} that writes {@link Mutation}s to Cloud Spanner. Mutations are written in
   * batches, where the maximum batch size is {@link SpannerIO#SPANNER_MUTATIONS_PER_COMMIT_LIMIT}.
   *
   * <p>See <a href="https://cloud.google.com/spanner"/>
   *
   * <p>Commits are non-transactional.  If a commit fails, it will be retried (up to
   * {@link SpannerIO#MAX_RETRIES}. times). This means that the
   * mutation operation should be idempotent.
   */
  @VisibleForTesting
  static class SpannerWriterFn extends DoFn<Mutation, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(SpannerWriterFn.class);
    private transient Spanner spanner;
    private final String instanceId;
    private final String databaseId;
    private final int batchSize;
    private transient DatabaseClient dbClient;
    // Current batch of mutations to be written.
    private final List<Mutation> mutations = new ArrayList<>();

    private static final int MAX_RETRIES = 5;
    private static final FluentBackoff BUNDLE_WRITE_BACKOFF =
        FluentBackoff.DEFAULT
            .withMaxRetries(MAX_RETRIES).withInitialBackoff(Duration.standardSeconds(5));

    @VisibleForTesting
    SpannerWriterFn(String instanceId, String databaseId, int batchSize) {
      this.instanceId = checkNotNull(instanceId, "instanceId");
      this.databaseId = checkNotNull(databaseId, "databaseId");
      this.batchSize = batchSize;
    }

    @Setup
    public void setup() throws Exception {
        SpannerOptions options = SpannerOptions.newBuilder().build();
        spanner = options.getService();
        dbClient = spanner.getDatabaseClient(
            DatabaseId.of(options.getProjectId(), instanceId, databaseId));
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Mutation m = c.element();
      mutations.add(m);
      int columnCount = m.asMap().size();
      if ((mutations.size() + 1) * columnCount >= batchSize) {
        flushBatch();
      }
    }

    @FinishBundle
    public void finishBundle(Context c) throws Exception {
      if (!mutations.isEmpty()) {
        flushBatch();
      }
    }

    @Teardown
    public void teardown() throws Exception {
      if (spanner == null) {
          return;
      }
      spanner.closeAsync().get();
    }

    /**
     * Writes a batch of mutations to Cloud Spanner.
     *
     * <p>If a commit fails, it will be retried up to {@link #MAX_RETRIES} times.
     * If the retry limit is exceeded, the last exception from Cloud Spanner will be
     * thrown.
     *
     * @throws AbortedException if the commit fails or IOException or InterruptedException if
     * backing off between retries fails.
     */
    private void flushBatch() throws AbortedException, IOException, InterruptedException {
      LOG.debug("Writing batch of {} mutations", mutations.size());
      Sleeper sleeper = Sleeper.DEFAULT;
      BackOff backoff = BUNDLE_WRITE_BACKOFF.backoff();

      while (true) {
        // Batch upsert rows.
        try {
          dbClient.writeAtLeastOnce(mutations);

          // Break if the commit threw no exception.
          break;
        } catch (AbortedException exception) {
          // Only log the code and message for potentially-transient errors. The entire exception
          // will be propagated upon the last retry.
          LOG.error("Error writing to Spanner ({}): {}", exception.getCode(),
              exception.getMessage());
          if (!BackOffUtils.next(sleeper, backoff)) {
            LOG.error("Aborting after {} retries.", MAX_RETRIES);
            throw exception;
          }
        }
      }
      LOG.debug("Successfully wrote {} mutations", mutations.size());
      mutations.clear();
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("instanceId", instanceId)
              .withLabel("Instance"))
          .addIfNotNull(DisplayData.item("databaseId", databaseId)
              .withLabel("Database"));
    }
  }
}
