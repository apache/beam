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
package com.google.cloud.dataflow.sdk.io;

import static com.google.api.services.datastore.DatastoreV1.PropertyFilter.Operator.EQUAL;
import static com.google.api.services.datastore.DatastoreV1.PropertyOrder.Direction.DESCENDING;
import static com.google.api.services.datastore.DatastoreV1.QueryResultBatch.MoreResultsType.NOT_FINISHED;
import static com.google.api.services.datastore.client.DatastoreHelper.getPropertyMap;
import static com.google.api.services.datastore.client.DatastoreHelper.makeFilter;
import static com.google.api.services.datastore.client.DatastoreHelper.makeOrder;
import static com.google.api.services.datastore.client.DatastoreHelper.makeValue;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Verify.verify;

import org.apache.beam.sdk.annotations.Experimental;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.api.services.datastore.DatastoreV1.CommitRequest;
import com.google.api.services.datastore.DatastoreV1.Entity;
import com.google.api.services.datastore.DatastoreV1.EntityResult;
import com.google.api.services.datastore.DatastoreV1.Key;
import com.google.api.services.datastore.DatastoreV1.Key.PathElement;
import com.google.api.services.datastore.DatastoreV1.PartitionId;
import com.google.api.services.datastore.DatastoreV1.Query;
import com.google.api.services.datastore.DatastoreV1.QueryResultBatch;
import com.google.api.services.datastore.DatastoreV1.RunQueryRequest;
import com.google.api.services.datastore.DatastoreV1.RunQueryResponse;
import com.google.api.services.datastore.client.Datastore;
import com.google.api.services.datastore.client.DatastoreException;
import com.google.api.services.datastore.client.DatastoreFactory;
import com.google.api.services.datastore.client.DatastoreHelper;
import com.google.api.services.datastore.client.DatastoreOptions;
import com.google.api.services.datastore.client.QuerySplitter;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.EntityCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.Sink.WriteOperation;
import com.google.cloud.dataflow.sdk.io.Sink.Writer;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.AttemptBoundedExponentialBackOff;
import com.google.cloud.dataflow.sdk.util.RetryHttpRequestInitializer;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

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
 * <p>{@link DatastoreIO} provides an API to Read and Write {@link PCollection PCollections} of
 * <a href="https://developers.google.com/datastore/">Google Cloud Datastore</a>
 * {@link Entity} objects.
 *
 * <p>Google Cloud Datastore is a fully managed NoSQL data storage service.
 * An {@code Entity} is an object in Datastore, analogous to a row in traditional
 * database table.
 *
 * <p>This API currently requires an authentication workaround. To use {@link DatastoreIO}, users
 * must use the {@code gcloud} command line tool to get credentials for Datastore:
 * <pre>
 * $ gcloud auth login
 * </pre>
 *
 * <p>To read a {@link PCollection} from a query to Datastore, use {@link DatastoreIO#source} and
 * its methods {@link DatastoreIO.Source#withDataset} and {@link DatastoreIO.Source#withQuery} to
 * specify the dataset to query and the query to read from. You can optionally provide a namespace
 * to query within using {@link DatastoreIO.Source#withNamespace} or a Datastore host using
 * {@link DatastoreIO.Source#withHost}.
 *
 * <p>For example:
 *
 * <pre> {@code
 * // Read a query from Datastore
 * PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
 * Query query = ...;
 * String dataset = "...";
 *
 * Pipeline p = Pipeline.create(options);
 * PCollection<Entity> entities = p.apply(
 *     Read.from(DatastoreIO.source()
 *         .withDataset(datasetId)
 *         .withQuery(query)
 *         .withHost(host)));
 * } </pre>
 *
 * <p>or:
 *
 * <pre> {@code
 * // Read a query from Datastore using the default namespace and host
 * PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
 * Query query = ...;
 * String dataset = "...";
 *
 * Pipeline p = Pipeline.create(options);
 * PCollection<Entity> entities = p.apply(DatastoreIO.readFrom(datasetId, query));
 * p.run();
 * } </pre>
 *
 * <p><b>Note:</b> Normally, a Cloud Dataflow job will read from Cloud Datastore in parallel across
 * many workers. However, when the {@link Query} is configured with a limit using
 * {@link com.google.api.services.datastore.DatastoreV1.Query.Builder#setLimit(int)}, then
 * all returned results will be read by a single Dataflow worker in order to ensure correct data.
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
 * DatastoreIO#sink} to build a {@link DatastoreIO.Sink} and write to it using the {@link Write}
 * transform:
 *
 * <pre> {@code
 * PCollection<Entity> entities = ...;
 * entities.apply(Write.to(DatastoreIO.sink().withDataset(dataset).withHost(host)));
 * } </pre>
 *
 * <p>{@link Entity Entities} in the {@code PCollection} to be written must have complete
 * {@link Key Keys}. Complete {@code Keys} specify the {@code name} and {@code id} of the
 * {@code Entity}, where incomplete {@code Keys} do not. A {@code namespace} other than the
 * project default may be written to by specifying it in the {@code Entity} {@code Keys}.
 *
 * <pre>{@code
 * Key.Builder keyBuilder = DatastoreHelper.makeKey(...);
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
 * <p>Please see <a href="https://cloud.google.com/datastore/docs/activate">Cloud Datastore Sign Up
 * </a>for security and permission related information specific to Datastore.
 *
 * @see com.google.cloud.dataflow.sdk.runners.PipelineRunner
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
   * Returns an empty {@link DatastoreIO.Source} builder with the default {@code host}.
   * Configure the {@code dataset}, {@code query}, and {@code namespace} using
   * {@link DatastoreIO.Source#withDataset}, {@link DatastoreIO.Source#withQuery},
   * and {@link DatastoreIO.Source#withNamespace}.
   *
   * @deprecated the name and return type do not match. Use {@link #source()}.
   */
  @Deprecated
  public static Source read() {
    return source();
  }

  /**
   * Returns an empty {@link DatastoreIO.Source} builder with the default {@code host}.
   * Configure the {@code dataset}, {@code query}, and {@code namespace} using
   * {@link DatastoreIO.Source#withDataset}, {@link DatastoreIO.Source#withQuery},
   * and {@link DatastoreIO.Source#withNamespace}.
   *
   * <p>The resulting {@link Source} object can be passed to {@link Read} to create a
   * {@code PTransform} that will read from Datastore.
   */
  public static Source source() {
    return new Source(DEFAULT_HOST, null, null, null);
  }

  /**
   * Returns a {@code PTransform} that reads Datastore entities from the query
   * against the given dataset.
   */
  public static Read.Bounded<Entity> readFrom(String datasetId, Query query) {
    return Read.from(new Source(DEFAULT_HOST, datasetId, query, null));
  }

  /**
   * Returns a {@code PTransform} that reads Datastore entities from the query
   * against the given dataset and host.
   *
   * @deprecated prefer {@link #source()} with {@link Source#withHost}, {@link Source#withDataset},
   *    {@link Source#withQuery}s.
   */
  @Deprecated
  public static Read.Bounded<Entity> readFrom(String host, String datasetId, Query query) {
    return Read.from(new Source(host, datasetId, query, null));
  }

  /**
   * A {@link Source} that reads the result rows of a Datastore query as {@code Entity} objects.
   */
  public static class Source extends BoundedSource<Entity> {
    public String getHost() {
      return host;
    }

    public String getDataset() {
      return datasetId;
    }

    public Query getQuery() {
      return query;
    }

    @Nullable
    public String getNamespace() {
      return namespace;
    }

    public Source withDataset(String datasetId) {
      checkNotNull(datasetId, "datasetId");
      return new Source(host, datasetId, query, namespace);
    }

    /**
     * Returns a new {@link Source} that reads the results of the specified query.
     *
     * <p>Does not modify this object.
     *
     * <p><b>Note:</b> Normally, a Cloud Dataflow job will read from Cloud Datastore in parallel
     * across many workers. However, when the {@link Query} is configured with a limit using
     * {@link com.google.api.services.datastore.DatastoreV1.Query.Builder#setLimit(int)}, then all
     * returned results will be read by a single Dataflow worker in order to ensure correct data.
     */
    public Source withQuery(Query query) {
      checkNotNull(query, "query");
      checkArgument(!query.hasLimit() || query.getLimit() > 0,
          "Invalid query limit %s: must be positive", query.getLimit());
      return new Source(host, datasetId, query, namespace);
    }

    public Source withHost(String host) {
      checkNotNull(host, "host");
      return new Source(host, datasetId, query, namespace);
    }

    public Source withNamespace(@Nullable String namespace) {
      return new Source(host, datasetId, query, namespace);
    }

    @Override
    public Coder<Entity> getDefaultOutputCoder() {
      return EntityCoder.of();
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) {
      // TODO: Perhaps this can be implemented by inspecting the query.
      return false;
    }

    @Override
    public List<Source> splitIntoBundles(long desiredBundleSizeBytes, PipelineOptions options)
        throws Exception {
      // Users may request a limit on the number of results. We can currently support this by
      // simply disabling parallel reads and using only a single split.
      if (query.hasLimit()) {
        return ImmutableList.of(this);
      }

      long numSplits;
      try {
        numSplits = Math.round(((double) getEstimatedSizeBytes(options)) / desiredBundleSizeBytes);
      } catch (Exception e) {
        // Fallback in case estimated size is unavailable. TODO: fix this, it's horrible.
        numSplits = 12;
      }

      // If the desiredBundleSize or number of workers results in 1 split, simply return
      // a source that reads from the original query.
      if (numSplits <= 1) {
        return ImmutableList.of(this);
      }

      List<Query> datastoreSplits;
      try {
        datastoreSplits = getSplitQueries(Ints.checkedCast(numSplits), options);
      } catch (IllegalArgumentException | DatastoreException e) {
        LOG.warn("Unable to parallelize the given query: {}", query, e);
        return ImmutableList.of(this);
      }

      ImmutableList.Builder<Source> splits = ImmutableList.builder();
      for (Query splitQuery : datastoreSplits) {
        splits.add(new Source(host, datasetId, splitQuery, namespace));
      }
      return splits.build();
    }

    @Override
    public BoundedReader<Entity> createReader(PipelineOptions pipelineOptions) throws IOException {
      return new DatastoreReader(this, getDatastore(pipelineOptions));
    }

    @Override
    public void validate() {
      Preconditions.checkNotNull(host, "host");
      Preconditions.checkNotNull(query, "query");
      Preconditions.checkNotNull(datasetId, "datasetId");
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      // Datastore provides no way to get a good estimate of how large the result of a query
      // will be. As a rough approximation, we attempt to fetch the statistics of the whole
      // entity kind being queried, using the __Stat_Kind__ system table, assuming exactly 1 kind
      // is specified in the query.
      //
      // See https://cloud.google.com/datastore/docs/concepts/stats
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
      if (namespace == null) {
        query.addKindBuilder().setName("__Stat_Kind__");
      } else {
        query.addKindBuilder().setName("__Ns_Stat_Kind__");
      }
      query.setFilter(makeFilter(
          makeFilter("kind_name", EQUAL, makeValue(ourKind)).build(),
          makeFilter("timestamp", EQUAL, makeValue(latestTimestamp)).build()));
      RunQueryRequest request = makeRequest(query.build());

      long now = System.currentTimeMillis();
      RunQueryResponse response = datastore.runQuery(request);
      LOG.info("Query for per-kind statistics took {}ms", System.currentTimeMillis() - now);

      QueryResultBatch batch = response.getBatch();
      if (batch.getEntityResultCount() == 0) {
        throw new NoSuchElementException(
            "Datastore statistics for kind " + ourKind + " unavailable");
      }
      Entity entity = batch.getEntityResult(0).getEntity();
      return getPropertyMap(entity).get("entity_bytes").getIntegerValue();
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("host", host)
          .add("dataset", datasetId)
          .add("query", query)
          .add("namespace", namespace)
          .toString();
    }

    ///////////////////////////////////////////////////////////////////////////////////////////

    private static final Logger LOG = LoggerFactory.getLogger(Source.class);
    private final String host;
    /** Not really nullable, but it may be {@code null} for in-progress {@code Source}s. */
    @Nullable
    private final String datasetId;
    /** Not really nullable, but it may be {@code null} for in-progress {@code Source}s. */
    @Nullable
    private final Query query;
    @Nullable
    private final String namespace;

    /** For testing only. TODO: This could be much cleaner with dependency injection. */
    @Nullable
    private QuerySplitter mockSplitter;
    @Nullable
    private Long mockEstimateSizeBytes;

    /**
     * Note that only {@code namespace} is really {@code @Nullable}. The other parameters may be
     * {@code null} as a matter of build order, but if they are {@code null} at instantiation time,
     * an error will be thrown.
     */
    private Source(
        String host, @Nullable String datasetId, @Nullable Query query,
        @Nullable String namespace) {
      this.host = checkNotNull(host, "host");
      this.datasetId = datasetId;
      this.query = query;
      this.namespace = namespace;
    }

    /**
     * A helper function to get the split queries, taking into account the optional
     * {@code namespace} and whether there is a mock splitter.
     */
    private List<Query> getSplitQueries(int numSplits, PipelineOptions options)
        throws DatastoreException {
      // If namespace is set, include it in the split request so splits are calculated accordingly.
      PartitionId.Builder partitionBuilder = PartitionId.newBuilder();
      if (namespace != null) {
        partitionBuilder.setNamespace(namespace);
      }

      if (mockSplitter != null) {
        // For testing.
        return mockSplitter.getSplits(query, partitionBuilder.build(), numSplits, null);
      }

      return DatastoreHelper.getQuerySplitter().getSplits(
          query, partitionBuilder.build(), numSplits, getDatastore(options));
    }

    /**
     * Builds a {@link RunQueryRequest} from the {@code query}, using the properties set on this
     * {@code Source}. For example, sets the {@code namespace} for the request.
     */
    private RunQueryRequest makeRequest(Query query) {
      RunQueryRequest.Builder requestBuilder = RunQueryRequest.newBuilder().setQuery(query);
      if (namespace != null) {
        requestBuilder.getPartitionIdBuilder().setNamespace(namespace);
      }
      return requestBuilder.build();
    }

    /**
     * Datastore system tables with statistics are periodically updated. This method fetches
     * the latest timestamp of statistics update using the {@code __Stat_Total__} table.
     */
    private long queryLatestStatisticsTimestamp(Datastore datastore) throws DatastoreException {
      Query.Builder query = Query.newBuilder();
      query.addKindBuilder().setName("__Stat_Total__");
      query.addOrder(makeOrder("timestamp", DESCENDING));
      query.setLimit(1);
      RunQueryRequest request = makeRequest(query.build());

      long now = System.currentTimeMillis();
      RunQueryResponse response = datastore.runQuery(request);
      LOG.info("Query for latest stats timestamp of dataset {} took {}ms", datasetId,
          System.currentTimeMillis() - now);
      QueryResultBatch batch = response.getBatch();
      if (batch.getEntityResultCount() == 0) {
        throw new NoSuchElementException(
            "Datastore total statistics for dataset " + datasetId + " unavailable");
      }
      Entity entity = batch.getEntityResult(0).getEntity();
      return getPropertyMap(entity).get("timestamp").getTimestampMicrosecondsValue();
    }

    private Datastore getDatastore(PipelineOptions pipelineOptions) {
      DatastoreOptions.Builder builder =
          new DatastoreOptions.Builder().host(host).dataset(datasetId).initializer(
              new RetryHttpRequestInitializer());

      Credential credential = pipelineOptions.as(GcpOptions.class).getGcpCredential();
      if (credential != null) {
        builder.credential(credential);
      }
      return DatastoreFactory.get().create(builder.build());
    }

    /** For testing only. */
    Source withMockSplitter(QuerySplitter splitter) {
      Source res = new Source(host, datasetId, query, namespace);
      res.mockSplitter = splitter;
      res.mockEstimateSizeBytes = mockEstimateSizeBytes;
      return res;
    }

    /** For testing only. */
    Source withMockEstimateSizeBytes(Long estimateSizeBytes) {
      Source res = new Source(host, datasetId, query, namespace);
      res.mockSplitter = mockSplitter;
      res.mockEstimateSizeBytes = estimateSizeBytes;
      return res;
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
    final String host;
    final String datasetId;

    /**
     * Returns a {@link Sink} that is like this one, but will write to the specified dataset.
     */
    public Sink withDataset(String datasetId) {
      checkNotNull(datasetId, "datasetId");
      return new Sink(host, datasetId);
    }

    /**
     * Returns a {@link Sink} that is like this one, but will use the given host.  If not specified,
     * the {@link DatastoreIO#DEFAULT_HOST default host} will be used.
     */
    public Sink withHost(String host) {
      checkNotNull(host, "host");
      return new Sink(host, datasetId);
    }

    /**
     * Constructs a Sink with given host and dataset.
     */
    protected Sink(String host, String datasetId) {
      this.host = checkNotNull(host, "host");
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
              .initializer(new RetryHttpRequestInitializer());
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
    final long entitiesWritten;

    public DatastoreWriteResult(long recordsWritten) {
      this.entitiesWritten = recordsWritten;
    }
  }

  /**
   * A {@link Source.Reader} over the records from a query of the datastore.
   *
   * <p>Timestamped records are currently not supported.
   * All records implicitly have the timestamp of {@code BoundedWindow.TIMESTAMP_MIN_VALUE}.
   */
  public static class DatastoreReader extends BoundedSource.BoundedReader<Entity> {
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
    private java.util.Iterator<EntityResult> entities;

    /**
     * Current batch of query results.
     */
    private QueryResultBatch currentBatch;

    /**
     * Maximum number of results to request per query.
     *
     * <p>Must be set, or it may result in an I/O error when querying
     * Cloud Datastore.
     */
    private static final int QUERY_BATCH_LIMIT = 500;

    /**
     * Remaining user-requested limit on the number of sources to return. If the user did not set a
     * limit, then this variable will always have the value {@link Integer#MAX_VALUE} and will never
     * be decremented.
     */
    private int userLimit;

    private Entity currentEntity;

    /**
     * Returns a DatastoreReader with Source and Datastore object set.
     *
     * @param datastore a datastore connection to use.
     */
    public DatastoreReader(Source source, Datastore datastore) {
      this.source = source;
      this.datastore = datastore;
      // If the user set a limit on the query, remember it. Otherwise pin to MAX_VALUE.
      userLimit = source.query.hasLimit() ? source.query.getLimit() : Integer.MAX_VALUE;
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
    private Iterator<EntityResult> getIteratorAndMoveCursor() throws DatastoreException {
      Query.Builder query = source.query.toBuilder().clone();
      query.setLimit(Math.min(userLimit, QUERY_BATCH_LIMIT));
      if (currentBatch != null && currentBatch.hasEndCursor()) {
        query.setStartCursor(currentBatch.getEndCursor());
      }

      RunQueryRequest request = source.makeRequest(query.build());
      RunQueryResponse response = datastore.runQuery(request);

      currentBatch = response.getBatch();

      // MORE_RESULTS_AFTER_LIMIT is not implemented yet:
      // https://groups.google.com/forum/#!topic/gcd-discuss/iNs6M1jA2Vw, so
      // use result count to determine if more results might exist.
      int numFetch = currentBatch.getEntityResultCount();
      if (source.query.hasLimit()) {
        verify(userLimit >= numFetch,
            "Expected userLimit %s >= numFetch %s, because query limit %s should be <= userLimit",
            userLimit, numFetch, query.getLimit());
        userLimit -= numFetch;
      }
      moreResults =
          // User-limit does not exist (so userLimit == MAX_VALUE) and/or has not been satisfied.
          (userLimit > 0)
          // All indications from the API are that there are/may be more results.
          && ((numFetch == QUERY_BATCH_LIMIT) || (currentBatch.getMoreResults() == NOT_FINISHED));

      // May receive a batch of 0 results if the number of records is a multiple
      // of the request limit.
      if (numFetch == 0) {
        return null;
      }

      return currentBatch.getEntityResultList().iterator();
    }
  }
}
