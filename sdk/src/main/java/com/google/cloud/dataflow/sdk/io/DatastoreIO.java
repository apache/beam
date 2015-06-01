/*
 * Copyright (C) 2015 Google Inc.
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

package com.google.cloud.dataflow.sdk.io;

import static com.google.api.services.datastore.DatastoreV1.PropertyFilter.Operator.EQUAL;
import static com.google.api.services.datastore.DatastoreV1.PropertyOrder.Direction.DESCENDING;
import static com.google.api.services.datastore.DatastoreV1.QueryResultBatch.MoreResultsType.NOT_FINISHED;
import static com.google.api.services.datastore.client.DatastoreHelper.getPropertyMap;
import static com.google.api.services.datastore.client.DatastoreHelper.makeFilter;
import static com.google.api.services.datastore.client.DatastoreHelper.makeOrder;
import static com.google.api.services.datastore.client.DatastoreHelper.makeValue;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.api.services.datastore.DatastoreV1;
import com.google.api.services.datastore.DatastoreV1.CommitRequest;
import com.google.api.services.datastore.DatastoreV1.Entity;
import com.google.api.services.datastore.DatastoreV1.Key;
import com.google.api.services.datastore.DatastoreV1.Key.PathElement;
import com.google.api.services.datastore.DatastoreV1.Query;
import com.google.api.services.datastore.client.Datastore;
import com.google.api.services.datastore.client.DatastoreException;
import com.google.api.services.datastore.client.DatastoreFactory;
import com.google.api.services.datastore.client.DatastoreHelper;
import com.google.api.services.datastore.client.DatastoreOptions;
import com.google.api.services.datastore.client.QuerySplitter;
import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.EntityCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.Sink.WriteOperation;
import com.google.cloud.dataflow.sdk.io.Sink.Writer;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Write;
import com.google.cloud.dataflow.sdk.util.AttemptBoundedExponentialBackOff;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.RetryHttpRequestInitializer;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * {@link com.google.cloud.dataflow.sdk.transforms.PTransform}s for reading and writing
 * <a href="https://developers.google.com/datastore/">Google Cloud Datastore</a>
 * entities.
 *
 * <p> The {@link DatastoreIO} class provides an API to Read and Write a
 * {@link PCollection} of Datastore Entity.  This API currently requires an
 * authentication workaround described below.
 *
 * <p> Datastore is a fully managed NoSQL data storage service.
 * An Entity is an object in Datastore, analogous to a row in traditional
 * database table.  DatastoreIO supports Read/Write from/to Datastore within
 * Dataflow SDK service.
 *
 * <p> To use {@link DatastoreIO}, users must use gcloud to get credential for Datastore:
 * <pre>
  * $ gcloud auth login
 * </pre>
 *
 * <p> Note that the environment variable CLOUDSDK_EXTRA_SCOPES must be set
 * to the same value when executing a Datastore pipeline, as the local auth
 * cache is keyed by the requested scopes.
 *
 * <p> To read a {@link PCollection} from a query to Datastore, use
 * {@link DatastoreIO#read} and its methods {#link DatastoreIO.Read#withDataset}
 * and {#link DatastoreIO.Read#withQuery} to specify dataset to read, the query
 * to read from, and optionally {@link DatastoreIO.Source#withHost} to specify
 * the host of Datastore.
 * For example:
 *
 * <pre> {@code
 * // Read a query from Datastore
 * PipelineOptions options =
 *     PipelineOptionsFactory.fromArgs(args).create();
 * Pipeline p = Pipeline.create(options);
 * PCollection<Entity> entities = p.apply(
 *     Read.from(DatastoreIO.read()
 *         .withDataset(datasetId)
 *         .withQuery(query)
 *         .withHost(host)));
 * p.run();
 * } </pre>
 *
 * <p> or:
 *
 * <pre> {@code
 * // Read a query from Datastore
 * PipelineOptions options =
 *     PipelineOptionsFactory.fromArgs(args).create();
 * Pipeline p = Pipeline.create(options);
 * PCollection<Entity> entities = p.apply(DatastoreIO.readFrom(datasetId, query));
 * p.run();
 * } </pre>
 *
 * <p>To write a {@link PCollection} to a Datastore, use {@link DatastoreIO#writeTo},
 * specifying the datastore to write to:
 *
 * <pre> {@code
 * PCollection<Entity> entities = ...;
 * entities.apply(DatastoreIO.writeTo(dataset));
 * p.run();
 * } </pre>
 *
 * <p>To optionally change the host that is used to write to the Datastore, use {@link
 * DatastoreIO#sink} to build a DatastoreIO {@link Sink} and write to it using the {@link Write}
 * transform:
 *
 * <pre> {@code
 * PCollection<Entity> entities = ...;
 * entities.apply(Write.to(DatastoreIO.sink().withDataset(dataset).withHost(host)));
 * p.run();
 * } </pre>
 *
 * <p>Entities in the PCollection to be written must have complete keys.  Complete keys specify the
 * name/id of the entity, where incomplete keys do not. Entities will be committed as upsert (update
 * or insert) mutations. Please read
 * <a href="https://cloud.google.com/datastore/docs/concepts/entities">Entities, Properties, and
 * Keys</a> for more information about entity keys.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class DatastoreIO {
  public static final String DEFAULT_HOST = "https://www.googleapis.com";

  /**
   * Datastore has a limit of 500 mutations per batch operation, so we flush
   * changes to Datastore every 500 entities.
   */
  public static final int DATASTORE_BATCH_UPDATE_LIMIT = 500;

  /**
   * Returns an empty {@code DatastoreIO.Read} builder with the default host.
   * You'll need to configure the dataset and query using {@link DatastoreIO.Source#withDataset}
   * and {@link DatastoreIO.Source#withQuery}.
   */
  public static Source read() {
    return new Source(DEFAULT_HOST, null, null);
  }

  /**
   * Returns a {@code PTransform} that reads Datastore entities from the query
   * against the given dataset.
   */
  public static Read.Bound<Entity> readFrom(String datasetId, Query query) {
    return Read.from(new Source(DEFAULT_HOST, datasetId, query));
  }

  /**
   * Returns a {@code PTransform} that reads Datastore entities from the query
   * against the given dataset and host.
   */
  public static Read.Bound<Entity> readFrom(String host, String datasetId, Query query) {
    return Read.from(new Source(host, datasetId, query));
  }

  /**
   * A {@link Source} that reads the result rows of a Datastore query as {@code Entity} objects.
   */
  public static class Source extends BoundedSource<Entity> {
    private static final Logger LOG = LoggerFactory.getLogger(Source.class);
    private static final long serialVersionUID = 0;

    String host;
    String datasetId;
    Query query;

    /** For testing only. */
    private QuerySplitter mockSplitter;
    private Long mockEstimateSizeBytes;

    private Source(String host, String datasetId, Query query) {
      this.host = host;
      this.datasetId = datasetId;
      this.query = query;
    }

    public Source withDataset(String datasetId) {
      return new Source(host, datasetId, query);
    }

    public Source withQuery(Query query) {
      return new Source(host, datasetId, query);
    }

    public Source withHost(String host) {
      return new Source(host, datasetId, query);
    }

    @Override
    public Coder<Entity> getDefaultOutputCoder() {
      return EntityCoder.of();
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      // Datastore provides no way to get a good estimate of how large the result of a query
      // will be. As a rough approximation, we attempt to fetch the statistics of the whole
      // entity kind being queried, using the __Stat_Kind__ system table, assuming exactly 1 kind
      // is specified in the query.
      if (mockEstimateSizeBytes != null) {
        return mockEstimateSizeBytes;
      }

      Datastore datastore = getDatastore(options);
      if (query.getKindCount() != 1) {
        throw new UnsupportedOperationException(
            "Can only estimate size for queries specifying exactly 1 kind.");
      }
      String ourKind = query.getKind(0).getName();
      long latestTimestamp = queryLatestStatisticsTimestamp(datastore);
      Query.Builder query = Query.newBuilder();
      query.addKindBuilder().setName("__Stat_Kind__");
      query.setFilter(makeFilter(
          makeFilter("kind_name", EQUAL, makeValue(ourKind)).build(),
          makeFilter("timestamp", EQUAL, makeValue(latestTimestamp)).build()));
      DatastoreV1.RunQueryRequest request =
          DatastoreV1.RunQueryRequest.newBuilder().setQuery(query).build();

      long now = System.currentTimeMillis();
      DatastoreV1.RunQueryResponse response = datastore.runQuery(request);
      LOG.info("Query for per-kind statistics took {}ms", System.currentTimeMillis() - now);

      DatastoreV1.QueryResultBatch batch = response.getBatch();
      if (batch.getEntityResultCount() == 0) {
        throw new NoSuchElementException(
            "Datastore statistics for kind " + ourKind + " unavailable");
      }
      Entity entity = batch.getEntityResult(0).getEntity();
      return getPropertyMap(entity).get("entity_bytes").getIntegerValue();
    }

    /**
     * Datastore system tables with statistics are periodically updated. This method fetches
     * the latest timestamp of statistics update using the __Stat_Total__ table.
     */
    private long queryLatestStatisticsTimestamp(Datastore datastore) throws DatastoreException {
      Query.Builder query = Query.newBuilder();
      query.addKindBuilder().setName("__Stat_Total__");
      query.addOrder(makeOrder("timestamp", DESCENDING));
      query.setLimit(1);
      DatastoreV1.RunQueryRequest request =
          DatastoreV1.RunQueryRequest.newBuilder().setQuery(query).build();

      long now = System.currentTimeMillis();
      DatastoreV1.RunQueryResponse response = datastore.runQuery(request);
      LOG.info("Query for latest stats timestamp of dataset {} took {}ms", datasetId,
          System.currentTimeMillis() - now);
      DatastoreV1.QueryResultBatch batch = response.getBatch();
      if (batch.getEntityResultCount() == 0) {
        throw new NoSuchElementException(
            "Datastore total statistics for dataset " + datasetId + " unavailable");
      }
      Entity entity = batch.getEntityResult(0).getEntity();
      return getPropertyMap(entity).get("timestamp").getTimestampMicrosecondsValue();
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) {
      // TODO: Perhaps this can be implemented by inspecting the query.
      return false;
    }

    @Override
    public List<Source> splitIntoBundles(long desiredBundleSizeBytes, PipelineOptions options)
        throws Exception {
      DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
      long numSplits;
      try {
        numSplits = Math.round(((double) getEstimatedSizeBytes(options)) / desiredBundleSizeBytes);
      } catch (Exception e) {
        LOG.warn("Estimated size unavailable, using number of workers", e);
        // Fallback in case estimated size is unavailable.
        numSplits = dataflowOptions.getNumWorkers();
      }
      numSplits = Math.max(numSplits, 1);
      List<Query> splitQueries;
      if (mockSplitter == null) {
        splitQueries = DatastoreHelper.getQuerySplitter().getSplits(
            query, (int) numSplits, getDatastore(options));
      } else {
        splitQueries = mockSplitter.getSplits(query, (int) numSplits, null);
      }
      List<Source> res = new ArrayList<>();
      for (Query splitQuery : splitQueries) {
        res.add(new Source(host, datasetId, splitQuery));
      }
      return res;
    }

    @Override
    public BoundedReader<Entity> createReader(
        PipelineOptions pipelineOptions, ExecutionContext executionContext) throws IOException {
      return new DatastoreReader(this, getDatastore(pipelineOptions));
    }

    @Override
    public void validate() {
      Preconditions.checkNotNull(host, "host");
      Preconditions.checkNotNull(query, "query");
      Preconditions.checkNotNull(datasetId, "datasetId");
    }

    private Datastore getDatastore(PipelineOptions pipelineOptions) {
      DatastoreOptions.Builder builder =
          new DatastoreOptions.Builder().host(host).dataset(datasetId).initializer(
              new RetryHttpRequestInitializer(null));

      Credential credential = pipelineOptions.as(GcpOptions.class).getGcpCredential();
      if (credential != null) {
        builder.credential(credential);
      }
      return DatastoreFactory.get().create(builder.build());
    }

    /** For testing only. */
    Source withMockSplitter(QuerySplitter splitter) {
      Source res = new Source(host, datasetId, query);
      res.mockSplitter = splitter;
      res.mockEstimateSizeBytes = mockEstimateSizeBytes;
      return res;
    }

    /** For testing only. */
    public Source withMockEstimateSizeBytes(Long estimateSizeBytes) {
      Source res = new Source(host, datasetId, query);
      res.mockSplitter = mockSplitter;
      res.mockEstimateSizeBytes = estimateSizeBytes;
      return res;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("Datastore: ");
      sb.append("host ").append((host == null) ? "null" : host);
      sb.append("; dataset ").append((datasetId == null) ? "null" : datasetId);
      sb.append("; query: ");
      if (query == null) {
        sb.append("null");
      } else {
        sb.append("\n").append(query.toString());
      }
      return sb.toString();
    }
  }

  ///////////////////// Write Class /////////////////////////////////

  /**
   * Returns a new {@link DatastoreIO.Sink} builder using the default host.
   * You need to further configure it using {@link DatastoreIO.Sink#withDataset}, and optionally
   * {@link DatastoreIO.Sink#withHost} before using it in a {@link Write} transform.
   *
   * <p>For example: {@code p.apply(Write.to(DatastoreIO.sink().withDataset(dataset)));}
   */
  public static Sink sink() {
    return new Sink(DEFAULT_HOST, null);
  }

  /**
   * Returns a new {@link Write} transform that will write to a {@link Sink}.
   *
   * <p>For example: {@code p.apply(DatastoreIO.writeTo(dataset));}
   */
  public static Write.Bound<Entity> writeTo(String datasetId) {
    return Write.to(sink().withDataset(datasetId));
  }

  /**
   * A {@link Sink} that writes a {@link PCollection} containing
   * {@link Entity Entities} to a Datastore kind.
   *
   */
  public static class Sink extends com.google.cloud.dataflow.sdk.io.Sink<Entity> {
    private static final long serialVersionUID = 0;

    final String host;
    final String datasetId;

    /**
     * Returns a {@link Sink} that is like this one, but will write to the specified dataset.
     */
    public Sink withDataset(String datasetId) {
      return new Sink(host, datasetId);
    }

    /**
     * Returns a {@link Sink} that is like this one, but will use the given host.  If not specified,
     * the {@link DatastoreIO#DEFAULT_HOST default host} will be used.
     */
    public Sink withHost(String host) {
      return new Sink(host, datasetId);
    }

    /**
     * Constructs a Sink with given host and dataset.
     */
    protected Sink(String host, String datasetId) {
      this.host = host;
      this.datasetId = datasetId;
    }

    /**
     * Ensures the host and dataset are set.
     */
    @Override
    public void validate(PipelineOptions options) {
      Preconditions.checkNotNull(
          host, "Host is a required parameter. Please use withHost to set the host.");
      Preconditions.checkNotNull(
          datasetId,
          "Dataset ID is a required parameter. Please use withDataset to to set the datasetId.");
    }

    @Override
    public DatastoreWriteOperation createWriteOperation(PipelineOptions options) {
      return new DatastoreWriteOperation(this);
    }
  }

  /**
   * A {@link WriteOperation} that will manage a parallel write to a Datastore sink.
   */
  private static class DatastoreWriteOperation
      extends WriteOperation<Entity, DatastoreWriteResult> {
    private static final long serialVersionUID = 0;
    private static final Logger LOG = LoggerFactory.getLogger(DatastoreWriteOperation.class);

    private final DatastoreIO.Sink sink;

    public DatastoreWriteOperation(DatastoreIO.Sink sink) {
      this.sink = sink;
    }

    @Override
    public Coder<DatastoreWriteResult> getWriterResultCoder() {
      return SerializableCoder.of(DatastoreWriteResult.class);
    }

    @Override
    public void initialize(PipelineOptions options) throws Exception {}

    /**
     * Finalizes the write.  Logs the number of entities written to the Datastore.
     */
    @Override
    public void finalize(Iterable<DatastoreWriteResult> writerResults, PipelineOptions options)
        throws Exception {
      long totalEntities = 0;
      for (DatastoreWriteResult result : writerResults) {
        totalEntities += result.entitiesWritten;
      }
      LOG.info("Wrote {} elements.", totalEntities);
    }

    @Override
    public DatastoreWriter createWriter(PipelineOptions options) throws Exception {
      DatastoreOptions.Builder builder =
          new DatastoreOptions.Builder()
              .host(sink.host)
              .dataset(sink.datasetId)
              .initializer(new RetryHttpRequestInitializer(null));
      Credential credential = options.as(GcpOptions.class).getGcpCredential();
      if (credential != null) {
        builder.credential(credential);
      }
      Datastore datastore = DatastoreFactory.get().create(builder.build());

      return new DatastoreWriter(this, datastore);
    }

    @Override
    public DatastoreIO.Sink getSink() {
      return sink;
    }
  }

  /**
   * {@link Writer} that writes entities to a Datastore Sink.  Entities are written in batches,
   * where the maximum batch size is {@link DatastoreIO#DATASTORE_BATCH_UPDATE_LIMIT}.  Entities
   * are committed as upsert mutations (either update if the key already exists, or insert if it is
   * a new key).  If an entity does not have a complete key (i.e., it has no name or id), the bundle
   * will fail.
   *
   * <p>See <a
   * href="https://cloud.google.com/datastore/docs/concepts/entities#Datastore_Creating_an_entity">
   * Datastore: Entities, Properties, and Keys</a> for information about entity keys and upsert
   * mutations.
   *
   * <p>Commits are non-transactional.  If a commit fails because of a conflict over an entity
   * group, the commit will be retried (up to {@link DatastoreIO#DATASTORE_BATCH_UPDATE_LIMIT}
   * times).
   *
   * <p>Visible for testing purposes.
   */
  static class DatastoreWriter extends Writer<Entity, DatastoreWriteResult> {
    private static final Logger LOG = LoggerFactory.getLogger(DatastoreWriter.class);
    private final DatastoreWriteOperation writeOp;
    private final Datastore datastore;
    private long totalWritten = 0;

    // Visible for testing.
    final List<Entity> entities = new ArrayList<>();

    /**
     * Since a bundle is written in batches, we should retry the commit of a batch in order to
     * prevent transient errors from causing the bundle to fail.
     */
    private static final int MAX_RETRIES = 5;

    /**
     * Initial backoff time for exponential backoff for retry attempts.
     */
    private static final int INITIAL_BACKOFF_MILLIS = 5000;

    /**
     * Returns true if a Datastore key is complete.  A key is complete if its last element
     * has either an id or a name.
     */
    static boolean isValidKey(Key key) {
      List<PathElement> elementList = key.getPathElementList();
      if (elementList.isEmpty()) {
        return false;
      }
      PathElement lastElement = elementList.get(elementList.size() - 1);
      return (lastElement.hasId() || lastElement.hasName());
    }

    // Visible for testing
    DatastoreWriter(DatastoreWriteOperation writeOp, Datastore datastore) {
      this.writeOp = writeOp;
      this.datastore = datastore;
    }

    @Override
    public void open(String uId) throws Exception {}

    /**
     * Writes an entity to the Datastore.  Writes are batched, up to {@link
     * DatastoreIO#DATASTORE_BATCH_UPDATE_LIMIT}. If an entity does not have a complete key, an
     * {@link IllegalArgumentException} will be thrown.
     */
    @Override
    public void write(Entity value) throws Exception {
      // Verify that the entity to write has a complete key.
      if (!isValidKey(value.getKey())) {
        throw new IllegalArgumentException(
            "Entities to be written to the Datastore must have complete keys");
      }

      entities.add(value);

      if (entities.size() >= DatastoreIO.DATASTORE_BATCH_UPDATE_LIMIT) {
        flushBatch();
      }
    }

    /**
     * Flushes any pending batch writes and returns a DatastoreWriteResult.
     */
    @Override
    public DatastoreWriteResult close() throws Exception {
      if (entities.size() > 0) {
        flushBatch();
      }
      return new DatastoreWriteResult(totalWritten);
    }

    @Override
    public DatastoreWriteOperation getWriteOperation() {
      return writeOp;
    }

    /**
     * Writes a batch of entities to the Datastore.
     *
     * <p>If a commit fails, it will be retried (up to {@link DatastoreWriter#MAX_RETRIES}
     * times).  All entities in the batch will be committed again, even if the commit was partially
     * successful. If the retry limit is exceeded, the last exception from the Datastore will be
     * thrown.
     *
     * @throws DatastoreException if the commit fails or IOException or InterruptedException if
     * backing off between retries fails.
     */
    private void flushBatch() throws DatastoreException, IOException, InterruptedException {
      LOG.debug("Writing batch of {} entities", entities.size());
      Sleeper sleeper = Sleeper.DEFAULT;
      BackOff backoff = new AttemptBoundedExponentialBackOff(MAX_RETRIES, INITIAL_BACKOFF_MILLIS);

      while (true) {
        // Batch upsert entities.
        try {
          CommitRequest.Builder commitRequest = CommitRequest.newBuilder();
          commitRequest.getMutationBuilder().addAllUpsert(entities);
          commitRequest.setMode(CommitRequest.Mode.NON_TRANSACTIONAL);
          datastore.commit(commitRequest.build());

          // Break if the commit threw no exception.
          break;

        } catch (DatastoreException exception) {
          // Only log the code and message for potentially-transient errors. The entire exception
          // will be propagated upon the last retry.
          LOG.error("Error writing to the Datastore ({}): {}", exception.getCode(),
              exception.getMessage());
          if (!BackOffUtils.next(sleeper, backoff)) {
            LOG.error("Aborting after {} retries.", MAX_RETRIES);
            throw exception;
          }
        }
      }
      totalWritten += entities.size();
      LOG.debug("Successfully wrote {} entities", entities.size());
      entities.clear();
    }
  }

  private static class DatastoreWriteResult implements Serializable {
    private static final long serialVersionUID = 0;

    final long entitiesWritten;

    public DatastoreWriteResult(long recordsWritten) {
      this.entitiesWritten = recordsWritten;
    }
  }

  /**
   * A {@link Source.Reader} over the records from a query of the datastore.

   * <p> Timestamped records are currently not supported.
   * All records implicitly have the timestamp of {@code BoundedWindow.TIMESTAMP_MIN_VALUE}.
   */
  public static class DatastoreReader extends BoundedSource.AbstractBoundedReader<Entity> {
    private final Source source;

    /**
     * Datastore to read from.
     */
    private final Datastore datastore;

    /**
     * True if more results may be available.
     */
    private boolean moreResults;

    /**
     * Iterator over records.
     */
    private java.util.Iterator<DatastoreV1.EntityResult> entities;

    /**
     * Current batch of query results.
     */
    private DatastoreV1.QueryResultBatch currentBatch;

    /**
     * Maximum number of results to request per query.
     *
     * <p> Must be set, or it may result in an I/O error when querying
     * Cloud Datastore.
     */
    private static final int QUERY_LIMIT = 500;

    private Entity currentEntity;

    /**
     * Returns a DatastoreReader with Source and Datastore object set.
     *
     * @param datastore a datastore connection to use.
     */
    public DatastoreReader(Source source, Datastore datastore) {
      this.source = source;
      this.datastore = datastore;
    }

    @Override
    public Entity getCurrent() {
      return currentEntity;
    }

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      if (entities == null || (!entities.hasNext() && moreResults)) {
        try {
          entities = getIteratorAndMoveCursor();
        } catch (DatastoreException e) {
          throw new IOException(e);
        }
      }

      if (entities == null || !entities.hasNext()) {
        currentEntity = null;
        return false;
      }

      currentEntity = entities.next().getEntity();
      return true;
    }

    @Override
    public void close() throws IOException {
      // Nothing
    }

    @Override
    public DatastoreIO.Source getCurrentSource() {
      return source;
    }

    @Override
    public DatastoreIO.Source splitAtFraction(double fraction) {
      // Not supported.
      return null;
    }

    @Override
    public Double getFractionConsumed() {
      // Not supported.
      return null;
    }

    /**
     * Returns an iterator over the next batch of records for the query
     * and updates the cursor to get the next batch as needed.
     * Query has specified limit and offset from InputSplit.
     */
    private Iterator<DatastoreV1.EntityResult> getIteratorAndMoveCursor()
        throws DatastoreException {
      Query.Builder query = this.source.query.toBuilder().clone();
      query.setLimit(QUERY_LIMIT);
      if (currentBatch != null && currentBatch.hasEndCursor()) {
        query.setStartCursor(currentBatch.getEndCursor());
      }

      DatastoreV1.RunQueryRequest request =
          DatastoreV1.RunQueryRequest.newBuilder().setQuery(query).build();
      DatastoreV1.RunQueryResponse response = datastore.runQuery(request);

      currentBatch = response.getBatch();

      // MORE_RESULTS_AFTER_LIMIT is not implemented yet:
      // https://groups.google.com/forum/#!topic/gcd-discuss/iNs6M1jA2Vw, so
      // use result count to determine if more results might exist.
      int numFetch = currentBatch.getEntityResultCount();
      moreResults = (numFetch == QUERY_LIMIT) || (currentBatch.getMoreResults() == NOT_FINISHED);

      // May receive a batch of 0 results if the number of records is a multiple
      // of the request limit.
      if (numFetch == 0) {
        return null;
      }

      return currentBatch.getEntityResultList().iterator();
    }
  }
}
