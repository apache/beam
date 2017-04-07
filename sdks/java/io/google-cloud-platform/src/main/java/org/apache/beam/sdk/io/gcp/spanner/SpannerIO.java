package org.apache.beam.sdk.io.gcp.spanner;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
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
 * {@link SpannerIO} provides an API for reading from and writing to
 * <a href="https://cloud.google.com/spanner/">Google Cloud Spanner</a>.
 *
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class SpannerIO {

  private SpannerIO() { }

  @VisibleForTesting
  public static final int SPANNER_MUTATIONS_PER_COMMIT_LIMIT = 20000;

/*
 * <p>To write a {@link PCollection} to Spanner, use {@link SpannerIO#writeTo} to
 * specify the database location for output.
 *
 * <p>For example:
 *
 * <pre> {@code
 * // Write data to a table.
 * PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
 * options.getInput = <CSV text file location>;
 * PCollection<Mutation> mutations = ...;
 *
 * Pipeline p = Pipeline.create(options);
 *   p.apply(TextIO.Read.from(options.getInput()))
 *       .apply(mutations)
 *       .apply(SpannerIO.writeTo(options.getProjectId(),
 *           options.getInstanceId(), options.getDatabaseId()));
 *   p.run();
 *
 * } </pre>
 */
  public static Writer writeTo(String projectId, String instanceId, String databaseId) {
    return new Writer(projectId, instanceId, databaseId, SPANNER_MUTATIONS_PER_COMMIT_LIMIT);
  }

  /**
   * A {@link PTransform} that writes {@link Mutation} objects to Cloud Spanner.
   *
   * @see SpannerIO
   */
  public static class Writer extends PTransform<PCollection<Mutation>, PDone> {

    private final String projectId;
    private final String instanceId;
    private final String databaseId;
    private int batchSize;

    Writer(String projectId, String instanceId, String databaseId, int batchSize) {
      this.projectId = projectId;
      this.instanceId = instanceId;
      this.databaseId = databaseId;
      this.batchSize = batchSize;
    }

    /**
     * Returns a new {@link Write} with a limit on the number of mutations per batch.
     * Defaults to SPANNER_MUTATIONS_PER_COMMIT_LIMIT.
     */
    public Writer withBatchSize(Integer batchSize) {
      return new Writer(projectId, instanceId, databaseId, batchSize);
    }

    @Override
    public PDone expand(PCollection<Mutation> input) {
      input.apply("Write mutations to Spanner", ParDo.of(
              new SpannerWriterFn(projectId, instanceId, databaseId, batchSize)));

      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PCollection<Mutation> input) {
      checkNotNull(projectId, "projectId");
      checkNotNull(instanceId, "instanceId");
      checkNotNull(databaseId, "databaseId");
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("projectId", projectId)
          .add("instanceId", instanceId)
          .add("databaseId", databaseId)
          .toString();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("projectId", projectId)
              .withLabel("Output Project"))
          .addIfNotNull(DisplayData.item("instanceId", instanceId)
              .withLabel("Output Instance"))
          .addIfNotNull(DisplayData.item("databaseId", databaseId)
              .withLabel("Output Database"));
    }

    public String getProjectId() {
      return projectId;
    }

    public String getInstanceId() {
      return instanceId;
    }

    public String getDatabaseId() {
      return databaseId;
    }

    public int getBatchSize() {
      return batchSize;
    }

  }


  /**
   * {@link DoFn} that writes {@link Mutation}s to Cloud Spanner. Mutations are written in
   * batches, where the maximum batch size is {@link SpannerIO#SPANNER_MUTATIONS_PER_COMMIT_LIMIT}.
   *
   * <p>See <a href="https://cloud.google.com/spanner"/>
   *
   * <p>Commits are non-transactional.  If a commit fails, it will be retried (up to
   * {@link SpannerIO#SPANNER_MUTATIONS_PER_COMMIT_LIMIT}. times). This means that the
   * mutation operation should be idempotent.
   */
  @VisibleForTesting
  static class SpannerWriterFn<T, V> extends DoFn<Mutation, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(SpannerWriterFn.class);
    private transient Spanner spanner;
    private final String projectId;
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
    SpannerWriterFn(String projectId, String instanceId, String databaseId, int batchSize) {
      this.projectId = checkNotNull(projectId, "projectId");
      this.instanceId = checkNotNull(instanceId, "instanceId");
      this.databaseId = checkNotNull(databaseId, "databaseId");
      this.batchSize = batchSize;
    }

    @Setup
    public void setup() throws Exception {
        SpannerOptions options = SpannerOptions.newBuilder().build();
        spanner = options.getService();
        dbClient = getDbClient(spanner, DatabaseId.of(projectId, instanceId, databaseId));
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
     * @throws SpannerException if the commit fails or IOException or InterruptedException if
     * backing off between retries fails.
     */
    private void flushBatch() throws SpannerException, IOException, InterruptedException {
      LOG.debug("Writing batch of {} mutations", mutations.size());
      Sleeper sleeper = Sleeper.DEFAULT;
      BackOff backoff = BUNDLE_WRITE_BACKOFF.backoff();

      while (true) {
        // Batch upsert rows.
        try {
          dbClient.writeAtLeastOnce(mutations);

          // Break if the commit threw no exception.
          break;
        } catch (SpannerException exception) {
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
          .addIfNotNull(DisplayData.item("projectId", projectId)
              .withLabel("Project"))
          .addIfNotNull(DisplayData.item("instanceId", instanceId)
              .withLabel("Instance"))
          .addIfNotNull(DisplayData.item("databaseId", databaseId)
              .withLabel("Database"));
    }
  }

  private static DatabaseClient getDbClient(Spanner spanner, DatabaseId databaseId)
          throws IOException {

    try {
      String clientProject = spanner.getOptions().getProjectId();
      if (!databaseId.getInstanceId().getProject().equals(clientProject)) {
        String err = "Invalid project specified. Project in the database id should match"
            + "the project name set in the environment variable GCLOUD_PROJECT. Expected: "
            + clientProject;
        throw new IllegalArgumentException(err);
      }
      return spanner.getDatabaseClient(databaseId);
    } catch (Exception e) {
        throw new IOException(e);
    }
  }
}
