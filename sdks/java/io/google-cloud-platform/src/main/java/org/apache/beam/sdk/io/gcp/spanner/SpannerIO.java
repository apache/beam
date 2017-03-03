/*
 * Copyright (C) 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.beam.sdk.io.gcp.spanner;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SpannerStructCoder;
import org.apache.beam.sdk.coders.SpannerMutationCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.Sink.WriteOperation;
import org.apache.beam.sdk.io.Sink.Writer;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.values.PCollection;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;


import static com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.cloud.spanner.SpannerException;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * {@link SpannerIO} provides an API to Read and Write {@link PCollection PCollections} of
 * <a href="https://developers.google.com/spanner/">Google Cloud Spanner</a>
 *
 * <p>Google Cloud Spanner is a fully managed NoSQL data storage service.
 * An {@code Entity} is an object in Spanner, analogous to a row in traditional
 * database table.
 *
 * <p>This API currently requires an authentication workaround. To use {@link SpannerIO}, users
 * must use the {@code gcloud} command line tool to get credentials for Spanner:
 * <pre>
 * $ gcloud auth login
 * </pre>
 *
 * <p>To read a {@link PCollection} from a query to Spanner, use {@link SpannerIO#source} and
 * its methods {@link SpannerIO.Source#withDatabase} and {@link SpannerIO.Source#withQuery} to
 * specify the dataset to query and the query to read from. You can optionally provide a namespace
 * to query within using {@link SpannerIO.Source#withNamespace} or a Spanner instance using
 * {@link SpannerIO.Source#withHost}.
 *
 * <p>For example:
 *
 * <pre> {@code
 * // Read a query from Spanner
 * PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
 * Query query = ...;
 * String dataset = "...";
 *
 * Pipeline p = Pipeline.create(options);
 * PCollection<Struct> rows = p.apply(
 *     Read.from(SpannerIO.source()
 *         .withDatabaseId(databaseId)
 *         .withQuery(query));
 * } </pre>
 *
 * <p>or:
 *
 * <pre> {@code
 * // Read a query from Spanner using 
 * PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
 * Query query = ...;
 * String databaseId = "...";
 *
 * Pipeline p = Pipeline.create(options);
 * PCollection<Struct> rows = p.apply(SpannerIO.readFrom(databaseId, query));
 * p.run();
 * } </pre>
 *
 * <p><b>Note:</b> Normally, a Cloud Dataflow job will read from Cloud Spanner in parallel across
 * many workers. However, when the {@link Query} is configured with a limit using
 * {@link com.google.api.services.datastore.SpannerV1.Query.Builder#setLimit(int)}, then
 * all returned results will be read by a single Dataflow worker in order to ensure correct data.
 *
 * <p>To write a {@link PCollection} to a Spanner, use {@link SpannerIO#writeTo},
 * specifying the datastore to write to:
 *
 * <pre> {@code
 * PCollection<Row> rows = ...;
 * rows.apply(SpannerIO.writeTo(databaseId));
 * p.run();
 * } </pre>
 *
 * <p>To optionally change the host that is used to write to the Spanner, use {@link
 * SpannerIO#sink} to build a {@link SpannerIO.Sink} and write to it using the {@link Write}
 * transform:
 *
 * <pre> {@code
 * PCollection<Entity> entities = ...;
 * entities.apply(Write.to(SpannerIO.sink().withDatabaseId(databaseId)));
 * } </pre>
 *
 * <p>{@link Entity Entities} in the {@code PCollection} to be written must have complete
 * {@link Key Keys}. Complete {@code Keys} specify the {@code name} and {@code id} of the
 * {@code Entity}, where incomplete {@code Keys} do not. A {@code namespace} other than the
 * project default may be written to by specifying it in the {@code Entity} {@code Keys}.
 *
 * <pre>{@code
 * Key.Builder keyBuilder = SpannerHelper.makeKey(...);
 * keyBuilder.getPartitionIdBuilder().setNamespace(namespace);
 * }</pre>
 *
 * <p>{@code Entities} will be committed as upsert (update or insert) mutations. Please read
 * <a href="https://cloud.google.com/datastore/docs/concepts/entities">Entities, Properties, and
 * Keys</a> for more information about {@code Entity} keys.
 *
 * <p><h3>Permissions</h3>
 * Permission requirements depend on the {@code PipelineRunner} that is used to execute the
 * Dataflow job. Please refer to the documentation of corresponding {@code PipelineRunner}s for
 * more details.
 *
 * <p>Please see <a href="https://cloud.google.com/datastore/docs/activate">Cloud Spanner Sign Up
 * </a>for security and permission related information specific to Spanner.
 *
 * @see org.apache.beam.sdk.runners.PipelineRunner
 *
 * @deprecated replaced by {@link org.apache.beam.sdk.io.datastore.SpannerIO}
 */

@Deprecated
@Experimental(Experimental.Kind.SOURCE_SINK)
public class SpannerIO {
  /**
   * Spanner has a limit of 500 mutations per batch operation, so we flush
   * changes to Spanner every 500 entities.
   */
  public static final int SPANNER_MUTATIONS_PER_COMMIT_LIMIT = 20000;

  /**
   * Returns an empty {@link SpannerIO.Source} builder with the default {@code host}.
   * Configure the {@code dataset}, {@code query}, and {@code namespace} using
   * {@link SpannerIO.Source#withDatabase}, {@link SpannerIO.Source#withQuery},
   * and {@link SpannerIO.Source#withNamespace}.
   *
   * @deprecated the name and return type do not match. Use {@link #source()}.
   */
  @Deprecated
  public static Source read() {
    return source();
  }

  /**
   * Returns an empty {@link SpannerIO.Source} builder with the default {@code host}.
   * Configure the {@code dataset}, {@code query}, and {@code namespace} using
   * {@link SpannerIO.Source#withDatabase}, {@link SpannerIO.Source#withQuery},
   * and {@link SpannerIO.Source#withNamespace}.
   *
   * <p>The resulting {@link Source} object can be passed to {@link Read} to create a
   * {@code PTransform} that will read from Spanner.
   */
  public static Source source() {
    return new Source(null, null, null, null);
  }

  /**
   * Returns a {@code PTransform} that reads Spanner entities from the query
   * against the given dataset.
   */
  public static Read.Bounded<Struct> readFrom(String projectId, String instanceId, String databaseId, String query) {
    return Read.from(new Source(projectId, instanceId, databaseId, query));
  }

  /**
   * A {@link Source} that reads the result rows of a Spanner query as {@code Entity} objects.
   */
  public static class Source extends BoundedSource<Struct> {

    public String getProjectId() {
      return projectId;
    }

    public String getInstanceId() {
      return instanceId;
    }

    public String getDatabaseId() {
      return databaseId;
    }

    public String getQuery() {
      return query;
    }

    public Source withDatabaseId(String projectdId, String instanceId, String databaseId) {
      //checkNotNull(databaseId, "databaseId");
      return new Source(projectId, instanceId, databaseId, query);
    }

    /**
     * Returns a new {@link Source} that reads the results of the specified query.
     *
     * <p>Does not modify this object.
     *
     * <p><b>Note:</b> Normally, a Cloud Dataflow job will read from Cloud Spanner in parallel
     * across many workers. However, when the {@link Query} is configured with a limit using
     * {@link com.google.api.services.datastore.SpannerV1.Query.Builder#setLimit(int)}, then all
     * returned results will be read by a single Dataflow worker in order to ensure correct data.
     */
    public Source withQuery(String query) {
      //checkNotNull(query, "query");
      //checkArgument(!query.hasLimit() || query.getLimit() > 0,
      //    "Invalid query limit %s: must be positive", query.getLimit());
      return new Source(projectId, instanceId, databaseId, query);
    }

    @Override
    public Coder<Struct> getDefaultOutputCoder() {
      return SpannerStructCoder.of();
    }

    @Override
    public List<Source> splitIntoBundles(long desiredBundleSizeBytes, PipelineOptions options)
        throws Exception {

        return ImmutableList.of(this);
    }

    @Override
    public BoundedReader<Struct> createReader(PipelineOptions pipelineOptions) throws IOException {
      return new SpannerReader(this, getDbClient(DatabaseId.of(projectId, instanceId, databaseId)));
    }

    @Override
    public void validate() {
      //checkNotNull(query, "query");
      //checkNotNull(databaseId, "databaseId");
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
        return 0L;   // TODO implement this.
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("projectId", projectId)
            .withLabel("Input Project"))
          .addIfNotNull(DisplayData.item("instanceId", instanceId)
            .withLabel("Input Instance"))
          .addIfNotNull(DisplayData.item("databaseId", databaseId)
            .withLabel("Input Database"));
      if (query != null) {
        builder.add(DisplayData.item("query", query)
          .withLabel("Query"));
      }
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("projectId", projectId)
          .add("instanceId", instanceId)
          .add("databaseId", databaseId)
          .add("query", query)
          .toString();
    }

    ///////////////////////////////////////////////////////////////////////////////////////////

    private static final Logger LOG = LoggerFactory.getLogger(Source.class);
    private final String  projectId;
    private final String  instanceId;
    private final String  databaseId;
    /** Not really nullable, but it may be {@code null} for in-progress {@code Source}s. */
    @Nullable
    private final String  query;

    /** For testing only. TODO: This could be much cleaner with dependency injection. */
/*
    @Nullable
    private QuerySplitter mockSplitter;
    @Nullable
    private Long mockEstimateSizeBytes;
*/

    /**
     *
     */
    private Source(String projectId, String instanceId, String databaseId, @Nullable String query) {
      this.projectId = projectId;
      this.instanceId = instanceId;
      this.databaseId = databaseId;
      this.query = query;
    }


//    /** For testing only. */
//    Source withMockSplitter(QuerySplitter splitter) {
//      Source res = new Source(host, databaseId, query, namespace);
//      res.mockSplitter = splitter;
//      res.mockEstimateSizeBytes = mockEstimateSizeBytes;
//      return res;
//    }
//
//    /** For testing only. */
//    Source withMockEstimateSizeBytes(Long estimateSizeBytes) {
//      Source res = new Source(host, databaseId, query, namespace);
//      res.mockSplitter = mockSplitter;
//      res.mockEstimateSizeBytes = estimateSizeBytes;
//      return res;
//    }
  }

  ///////////////////// Write Class /////////////////////////////////

  public static Sink sink() {
    return new Sink();
  }

  /**
   * Returns a new {@link Write} transform that will write to a {@link Sink}.
   *
   * <p>For example: {@code p.apply(SpannerIO.writeTo(databaseId));}
   */
  //public static Write.Bound<Mutation> writeTo(String projectId, String instanceId, String databaseId) {
  //  return Write.to(sink().withDatabase(projectId, instanceId, databaseId));
  //}

  /**
   * A {@link Sink} that writes a {@link PCollection} containing
   * {@link Mutation} instances to a Spanner table.
   *
   */
  public static class Sink extends org.apache.beam.sdk.io.Sink<Mutation> {

    String projectId;
    String instanceId;
    String databaseId;

    public Sink() {
    }

    public Sink(String projectId, String instanceId, String  databaseId) {
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.databaseId = databaseId;
    }


    /**
     * Returns a {@link Sink} that is like this one, but will write to the specified database
     */
    public Sink withDatabase(String projectId, String instanceId, String databaseId) {
      //checkNotNull(databaseId, "databaseId");
      return new Sink(projectId, instanceId, databaseId);
    }

    /**
     * Ensures the databaseId is set.
     */
    @Override
    public void validate(PipelineOptions options) {
      //checkNotNull(
      //    databaseId,
      //   "Database ID is a required parameter. Please use withDatabase to to set the databaseId.");
    }

    @Override
    public SpannerWriteOperation createWriteOperation(PipelineOptions options) {
      return new SpannerWriteOperation(this);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("projectId", projectId)
            .withLabel("Output Database"));
    }
  }

  /**
   * A {@link WriteOperation} that will manage a parallel write to a Spanner sink.
   */
  private static class SpannerWriteOperation
      extends WriteOperation<Mutation, SpannerWriteResult> {

    private static final Logger LOG = LoggerFactory.getLogger(SpannerWriteOperation.class);

    private final SpannerIO.Sink sink;

    public SpannerWriteOperation(SpannerIO.Sink sink) {
      this.sink = sink;
    }

    @Override
    public Coder<SpannerWriteResult> getWriterResultCoder() {
      return SerializableCoder.of(SpannerWriteResult.class);
    }

    @Override
    public void initialize(PipelineOptions options) throws Exception {}

    /**
     * Finalizes the write.  Logs the number of entities written to the Spanner.
     */
    @Override
    public void finalize(Iterable<SpannerWriteResult> writerResults, PipelineOptions options)
        throws Exception {
      long totalRows = 0;
      for (SpannerWriteResult result : writerResults) {
        totalRows += result.rowsWritten;
      }
      LOG.info("Wrote {} rows.", totalRows);
    }

    @Override
    public SpannerWriter createWriter(PipelineOptions options) throws Exception {
      return new SpannerWriter(this, getDbClient(DatabaseId.of(sink.projectId, sink.instanceId, sink.databaseId)));
    }

    @Override
    public SpannerIO.Sink getSink() {
      return sink;
    }
  }

  /**
   * {@link Writer} that writes entities to a Spanner Sink.  Entities are written in batches,
   * where the maximum batch size is {@link SpannerIO#SPANNER_MUTATIONS_PER_COMMIT_LIMIT}.  Entities
   * are committed as upsert mutations (either update if the key already exists, or insert if it is
   * a new key).  If an entity does not have a complete key (i.e., it has no name or id), the bundle
   * will fail.
   *
   * <p>See <a
   * href="https://cloud.google.com/datastore/docs/concepts/entities#Spanner_Creating_an_entity">
   * Spanner: Entities, Properties, and Keys</a> for information about entity keys and upsert
   * mutations.
   *
   * <p>Commits are non-transactional.  If a commit fails because of a conflict over an entity
   * group, the commit will be retried (up to {@link SpannerIO#SPANNER_MUTATIONS_PER_COMMIT_LIMIT}
   * times).
   *
   * <p>Visible for testing purposes.
   */
  static class SpannerWriter extends Writer<Mutation, SpannerWriteResult> {
    private static final Logger LOG = LoggerFactory.getLogger(SpannerWriter.class);
    private final SpannerWriteOperation writeOp;
    private final DatabaseClient dbClient;
    private long totalWritten = 0;

    // Visible for testing.
    final List<Mutation> rows = new ArrayList<>();

    /**
     * Since a bundle is written in batches, we should retry the commit of a batch in order to
     * prevent transient errors from causing the bundle to fail.
     */
    private static final int MAX_RETRIES = 5;
    private static final FluentBackoff BUNDLE_WRITE_BACKOFF =
        FluentBackoff.DEFAULT
            .withMaxRetries(MAX_RETRIES).withInitialBackoff(Duration.standardSeconds(5));

    // Visible for testing
    SpannerWriter(SpannerWriteOperation writeOp, DatabaseClient dbClient) {
      this.writeOp = writeOp;
      this.dbClient = dbClient;
    }

    @Override
    public void open(String uId) throws Exception {}

    /**
     * Writes an entity to Spanner.  Writes are batched, up to {@link
     * SpannerIO#SPANNER_MUTATIONS_PER_COMMIT_LIMIT}. 
     */
    @Override
    public void write(Mutation row) throws Exception {

      rows.add(row);

      if (rows.size() * row.asMap().size()  >= SpannerIO.SPANNER_MUTATIONS_PER_COMMIT_LIMIT) {
        flushBatch();
      }
    }

    /**
     * Flushes any pending batch writes and returns a SpannerWriteResult.
     */
    @Override
    public SpannerWriteResult close() throws Exception {
      if (rows.size() > 0) {
        flushBatch();
      }
      return new SpannerWriteResult(totalWritten);
    }

    @Override
    public SpannerWriteOperation getWriteOperation() {
      return writeOp;
    }

    /**
     * Writes a batch of entities to Spanner.
     *
     * <p>If a commit fails, it will be retried (up to {@link SpannerWriter#MAX_RETRIES}
     * times).  All entities in the batch will be committed again, even if the commit was partially
     * successful. If the retry limit is exceeded, the last exception from the Spanner will be
     * thrown.
     *
     * @throws SpannerException if the commit fails or IOException or InterruptedException if
     * backing off between retries fails.
     */
    private void flushBatch() throws SpannerException, IOException, InterruptedException {
      LOG.debug("Writing batch of {} rows", rows.size());
      Sleeper sleeper = Sleeper.DEFAULT;
      BackOff backoff = BUNDLE_WRITE_BACKOFF.backoff();

      while (true) {
        // Batch upsert rows.
        try {
          dbClient.writeAtLeastOnce(rows);

          // Break if the commit threw no exception.
          break;

        } catch (SpannerException exception) {
          // Only log the code and message for potentially-transient errors. The entire exception
          // will be propagated upon the last retry.
          LOG.error("Error writing to the Spanner ({}): {}", exception.getCode(),
              exception.getMessage());
          if (!BackOffUtils.next(sleeper, backoff)) {
            LOG.error("Aborting after {} retries.", MAX_RETRIES);
            throw exception;
          }
        }
      }
      totalWritten += rows.size();
      LOG.debug("Successfully wrote {} rows", rows.size());
      rows.clear();
    }
  }

  private static class SpannerWriteResult implements Serializable {
    final long rowsWritten;

    public SpannerWriteResult(long recordsWritten) {
      this.rowsWritten = recordsWritten;
    }
  }

  /**
   * A {@link Source.Reader} over the records from a query of the datastore.
   *
   * <p>Timestamped records are currently not supported.
   * All records implicitly have the timestamp of {@code BoundedWindow.TIMESTAMP_MIN_VALUE}.
   */
  public static class SpannerReader extends BoundedSource.BoundedReader<Struct> {
    private final Source source;

    /**
     * Database to read from.
     */
    private final DatabaseClient dbClient;

    /**
     * Current batch of query results.
     */
    private ResultSet currentBatch;

    /**
     * Maximum number of results to request per query.
     *
     * <p>Must be set, or it may result in an I/O error when querying
     * Cloud Spanner.
     */
    private static final int QUERY_BATCH_LIMIT = Integer.MAX_VALUE;

    /**
     * Remaining user-requested limit on the number of sources to return. If the user did not set a
     * limit, then this variable will always have the value {@link Integer#MAX_VALUE} and will never
     * be decremented.
     */
    private int userLimit;

    private volatile boolean done = false;

    private Struct currentRow;

    /**
     * Returns a SpannerReader with Source and Spanner object set.
     *
     * @param datastore a datastore connection to use.
     */
    public SpannerReader(Source source, DatabaseClient dbClient) {
      this.source = source;
      this.dbClient = dbClient;
      // If the user set a limit on the query, remember it. Otherwise pin to MAX_VALUE.
      //userLimit = source.query.hasLimit() ? source.query.getLimit() : Integer.MAX_VALUE;
    }

    @Override
    public Struct getCurrent() {
      return currentRow;
    }

    @Override
    public final long getSplitPointsConsumed() {
      return done ? 1 : 0;
    }

    @Override
    public final long getSplitPointsRemaining() {
      return done ? 0 : 1;
    }

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      if (currentBatch == null) {
        if (source.query != null) {
            currentBatch = dbClient.singleUse().executeQuery(Statement.of(source.query));
        }
      }

      if (currentBatch != null) {
          try {
              done = currentBatch.next();
              if (done) {
                  currentRow = null;
                  return false;
              }
              else {
                  currentRow = currentBatch.getCurrentRowAsStruct();
                  return true;
              }
          }
          catch (SpannerException s) {
              currentBatch = null;
              currentRow = null;
              throw new IOException(s);
          }
      }
      return false;
    }

    @Override
    public void close() throws IOException {
       if (currentBatch != null) {
           currentBatch.close();
           currentBatch = null;
       }
    }

    @Override
    public SpannerIO.Source getCurrentSource() {
      return source;
    }

    @Override
    public SpannerIO.Source splitAtFraction(double fraction) {
      // Not supported.
      return null;
    }

    @Override
    public Double getFractionConsumed() {
      // Not supported.
      return null;
    }
  }

  private static DatabaseClient getDbClient(DatabaseId databaseId) throws IOException {

    SpannerOptions options = SpannerOptions.newBuilder().build();
    Spanner spanner = options.getService();
    try {
      String clientProject = spanner.getOptions().getProjectId();
      if (!databaseId.getInstanceId().getProject().equals(clientProject)) {
        String err = "Invalid project specified. Project in the database id should match"
            + "the project name set in the environment variable GCLOUD_PROJECT. Expected: "
            + clientProject;
        throw new IllegalArgumentException(err);
      }
      return spanner.getDatabaseClient(databaseId);
    } 
    catch (Exception e) {
        throw new IOException(e);
    }
    finally {
      try {
          spanner.closeAsync().get();
      }
      catch (Exception e) {
          throw new IOException(e);
      }
    }
  }
   
}
