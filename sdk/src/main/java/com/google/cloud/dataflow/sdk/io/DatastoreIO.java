/*
 * Copyright (C) 2014 Google Inc.
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
import com.google.api.services.datastore.DatastoreV1;
import com.google.api.services.datastore.DatastoreV1.BeginTransactionRequest;
import com.google.api.services.datastore.DatastoreV1.BeginTransactionResponse;
import com.google.api.services.datastore.DatastoreV1.CommitRequest;
import com.google.api.services.datastore.DatastoreV1.Entity;
import com.google.api.services.datastore.DatastoreV1.Query;
import com.google.api.services.datastore.client.Datastore;
import com.google.api.services.datastore.client.DatastoreException;
import com.google.api.services.datastore.client.DatastoreFactory;
import com.google.api.services.datastore.client.DatastoreHelper;
import com.google.api.services.datastore.client.DatastoreOptions;
import com.google.api.services.datastore.client.QuerySplitter;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.EntityCoder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.RetryHttpRequestInitializer;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Transforms for reading and writing
 * <a href="https://developers.google.com/datastore/">Google Cloud Datastore</a>
 * entities.
 *
 * <p> The DatastoreIO class provides an experimental API to Read and Write a
 * {@link PCollection} of Datastore Entity.  Currently the class supports
 * read operations on both the DirectPipelineRunner and DataflowPipelineRunner,
 * and write operations on the DirectPipelineRunner.  This API is subject to
 * change, and currently requires an authentication workaround described below.
 *
 * <p> Datastore is a fully managed NoSQL data storage service.
 * An Entity is an object in Datastore, analogous to the a row in traditional
 * database table.  DatastoreIO supports Read/Write from/to Datastore within
 * Dataflow SDK service.
 *
 * <p> To use DatastoreIO, users must set up the environment and use gcloud
 * to get credential for Datastore:
 * <pre>
 * $ export CLOUDSDK_EXTRA_SCOPES=https://www.googleapis.com/auth/datastore
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
 *     ReadSource.from(DatastoreIO.read()
 *         .withDataset(datasetId)
 *         .withQuery(query)
 *         .withHost(host)));
 * p.run();
 * } </pre>
 *
 * or:
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
 * <p> To write a {@link PCollection} to a datastore, use
 * {@link DatastoreIO.Sink}, specifying {@link DatastoreIO.Sink#to} to specify
 * the datastore to write to, and optionally {@link TextIO.Write#named} to specify
 * the name of the pipeline step.  For example:
 *
 * <pre> {@code
 * // A simple Write to Datastore with DirectPipelineRunner (writing is not
 * // yet implemented for other runners):
 * PCollection<Entity> entities = ...;
 * lines.apply(DatastoreIO.Write.to("Write entities", datastore));
 * p.run();
 *
 * } </pre>
 */

public class DatastoreIO {
  private static final Logger LOG = LoggerFactory.getLogger(DatastoreIO.class);
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
   * Returns a {@code PTransform} which reads Datastore entities from the query
   * against the given dataset.
   */
  public static ReadSource.Bound<Entity> readFrom(String datasetId, Query query) {
    return ReadSource.from(new Source(DEFAULT_HOST, datasetId, query));
  }

  /**
   * Returns a {@code PTransform} which reads Datastore entities from the query
   * against the given dataset and host.
   */
  public static ReadSource.Bound<Entity> readFrom(String host, String datasetId, Query query) {
    return ReadSource.from(new Source(host, datasetId, query));
  }

  /**
   * A source that reads the result rows of a Datastore query as {@code Entity} objects.
   */
  @SuppressWarnings("serial")
  public static class Source extends com.google.cloud.dataflow.sdk.io.Source<Entity> {
    String host;
    String datasetId;
    Query query;
    /** For testing only. */
    private QuerySplitter mockSplitter;
    private Supplier<Long> mockEstimateSizeBytes;

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
        return mockEstimateSizeBytes.get();
      }

      Datastore datastore = getDatastore(options);
      if (query.getKindCount() != 1) {
        throw new UnsupportedOperationException(
            "Can only estimate size for queries specifying exactly 1 kind");
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
      LOG.info("Query for per-kind statistics took " + (System.currentTimeMillis() - now) + "ms");

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
      LOG.info("Query for latest stats timestamp of dataset " + datasetId + " took "
          + (System.currentTimeMillis() - now) + "ms");
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
    public List<Source> splitIntoShards(long desiredShardSizeBytes, PipelineOptions options)
        throws Exception {
      DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
      long numSplits;
      try {
        numSplits = getEstimatedSizeBytes(options) / desiredShardSizeBytes;
      } catch (Exception e) {
        LOG.warn("Estimated size unavailable, using number of workers", e);
        // Fallback in case estimated size is unavailable.
        numSplits = dataflowOptions.getNumWorkers();
      }
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
    public Reader<Entity> createBasicReader(
        PipelineOptions pipelineOptions, Coder<Entity> coder, ExecutionContext executionContext)
        throws IOException {
      return new DatastoreReader(query, getDatastore(pipelineOptions));
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
    public Source withMockEstimateSizeBytes(Supplier<Long> estimateSizeBytes) {
      Source res = new Source(host, datasetId, query);
      res.mockSplitter = mockSplitter;
      res.mockEstimateSizeBytes = estimateSizeBytes;
      return res;
    }
  }

  ///////////////////// Write Class /////////////////////////////////

  /**
   * Returns a new {@link DatastoreIO.Sink} builder using the default host.
   * You need to further configure it using {@link DatastoreIO.Sink#named},
   * {@link DatastoreIO.Sink#to}, and optionally {@link DatastoreIO.Sink#withHost}.
   */
  public static Sink write() {
    return new Sink(DEFAULT_HOST);
  }

  /**
   * Returns a new {@link DatastoreIO.Sink} builder using the default host and given dataset.
   * You need to further configure it using {@link DatastoreIO.Sink#named},
   * and optionally {@link DatastoreIO.Sink#withHost}.
   */
  public static Sink writeTo(String datasetId) {
    return write().to(datasetId);
  }

  /**
   * A {@link PTransform} that writes a {@code PCollection<Entity>} containing
   * entities to a Datastore kind.
   *
   * <p> Current version only supports Write operation running on
   * {@link DirectPipelineRunner}.  If Write is used on {@link DataflowPipelineRunner},
   * it throws {@link UnsupportedOperationException} and won't continue on the
   * operation.
   *
   */
  @SuppressWarnings("serial")
  public static class Sink extends PTransform<PCollection<Entity>, PDone> {
    String host;
    String datasetId;

    /**
     * Returns a DatastoreIO.Write PTransform with given host.
     */
    Sink(String host) {
      this.host = host;
    }

    /**
     * Returns a DatastoreIO.Write.Bound object.
     * Sets the name, datastore agent, and kind associated
     * with this transformation.
     */
    Sink(String name, String host, String datasetId) {
      super(name);
      this.host = host;
      this.datasetId = datasetId;
    }

    /**
     * Returns a DatastoreIO.Write PTransform with the name
     * associated with this PTransform.
     */
    public Sink named(String name) {
      return new Sink(name, host, datasetId);
    }

    /**
     * Returns a DatastoreIO.Write PTransform with given datasetId.
     */
    public Sink to(String datasetId) {
      return new Sink(name, host, datasetId);
    }

    /**
     * Returns a new DatastoreIO.Write PTransform with specified host.
     */
    public Sink withHost(String host) {
      return new Sink(name, host, datasetId);
    }

    @Override
    public PDone apply(PCollection<Entity> input) {
      if (this.host == null || this.datasetId == null) {
        throw new IllegalStateException("need to set Datastore host and dataasetId"
            + "of a DatastoreIO.Write transform");
      }

      return new PDone();
    }

    @Override
    protected String getKindString() {
      return "DatastoreIO.Write";
    }

    @Override
    protected Coder<Void> getDefaultOutputCoder() {
      return VoidCoder.of();
    }

    static {
      DirectPipelineRunner.registerDefaultTransformEvaluator(
          Sink.class, new DirectPipelineRunner.TransformEvaluator<Sink>() {
            @Override
            public void evaluate(
                Sink transform, DirectPipelineRunner.EvaluationContext context) {
              evaluateWriteHelper(transform, context);
            }
          });
    }
  }

  ///////////////////////////////////////////////////////////////////

  /**
   * Direct mode write evaluator.
   * This writes the result to Datastore.
   */
  private static void evaluateWriteHelper(
      Sink transform, DirectPipelineRunner.EvaluationContext context) {
    LOG.info("Writing to Datastore");
    GcpOptions options = context.getPipelineOptions();
    Credential credential = options.getGcpCredential();
    Datastore datastore = DatastoreFactory.get().create(
        new DatastoreOptions.Builder()
            .host(transform.host)
            .dataset(transform.datasetId)
            .credential(credential)
            .initializer(new RetryHttpRequestInitializer(null))
            .build());

    List<Entity> entityList = context.getPCollection(transform.getInput());

    // Create a map to put entities with same ancestor for writing in a batch.
    HashMap<String, List<Entity>> map = new HashMap<>();
    for (Entity e : entityList) {
      String keyOfAncestor =
          e.getKey().getPathElement(0).getKind() + e.getKey().getPathElement(0).getName();
      List<Entity> value = map.get(keyOfAncestor);
      if (value == null) {
        value = new ArrayList<>();
      }
      value.add(e);
      map.put(keyOfAncestor, value);
    }

    // Walk over the map, and write entities bucket by bucket.
    int count = 0;
    for (String k : map.keySet()) {
      List<Entity> entitiesWithSameAncestor = map.get(k);
      List<Entity> toInsert = new ArrayList<>();
      for (Entity e : entitiesWithSameAncestor) {
        toInsert.add(e);
        if (toInsert.size() >= DATASTORE_BATCH_UPDATE_LIMIT) {
          writeBatch(toInsert, datastore);
          toInsert.clear();
        }
      }
      writeBatch(toInsert, datastore);
      count += entitiesWithSameAncestor.size();
    }

    LOG.info("Total number of entities written: {}", count);
  }

  /**
   * A function for batch writing to Datastore.
   */
  private static void writeBatch(List<Entity> listOfEntities, Datastore datastore) {
    try {
      BeginTransactionRequest.Builder treq = BeginTransactionRequest.newBuilder();
      BeginTransactionResponse tres = datastore.beginTransaction(treq.build());
      CommitRequest.Builder creq = CommitRequest.newBuilder();
      creq.setTransaction(tres.getTransaction());
      creq.getMutationBuilder().addAllInsertAutoId(listOfEntities);
      datastore.commit(creq.build());
    } catch (DatastoreException e) {
      LOG.warn("Error while doing datastore operation: {}", e);
      throw new RuntimeException("Datastore exception", e);
    }
  }

  /**
   * An iterator over the records from a query of the datastore.
   *
   * <p> Usage:
   * <pre>{@code
   *   DatastoreIterator iterator = new DatastoreIterator(query, datastore);
   *   while (iterator.advance()) {
   *     Entity e = iterator.getCurrent();
   *     ...
   *   }
   * }</pre>
   */
  public static class DatastoreReader
      implements Source.Reader<Entity> {
    /**
     * Query to select records.
     */
    private Query.Builder query;

    /**
     * Datastore to read from.
     */
    private Datastore datastore;

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
    private static final int QUERY_LIMIT = 5000;

    private Entity currentEntity;

    /**
     * Returns a DatastoreIterator with query and Datastore object set.
     *
     * @param query the query to select records.
     * @param datastore a datastore connection to use.
     */
    public DatastoreReader(Query query, Datastore datastore) {
      this.query = query.toBuilder().clone();
      this.datastore = datastore;
      this.query.setLimit(QUERY_LIMIT);
    }

    @Override
    public Entity getCurrent() {
      return currentEntity;
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

    /**
     * Returns an iterator over the next batch of records for the query
     * and updates the cursor to get the next batch as needed.
     * Query has specified limit and offset from InputSplit.
     */
    private java.util.Iterator getIteratorAndMoveCursor()
        throws DatastoreException {
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
