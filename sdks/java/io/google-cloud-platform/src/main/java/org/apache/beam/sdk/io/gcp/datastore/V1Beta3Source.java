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

package org.apache.beam.sdk.io.gcp.datastore;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Verify.verify;
import static com.google.datastore.v1beta3.PropertyFilter.Operator.EQUAL;
import static com.google.datastore.v1beta3.PropertyOrder.Direction.DESCENDING;
import static com.google.datastore.v1beta3.QueryResultBatch.MoreResultsType.NOT_FINISHED;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeAndFilter;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeFilter;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeOrder;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeValue;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.RetryHttpRequestInitializer;

import com.google.api.client.auth.oauth2.Credential;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.datastore.v1beta3.Entity;
import com.google.datastore.v1beta3.EntityResult;
import com.google.datastore.v1beta3.PartitionId;
import com.google.datastore.v1beta3.Query;
import com.google.datastore.v1beta3.QueryResultBatch;
import com.google.datastore.v1beta3.RunQueryRequest;
import com.google.datastore.v1beta3.RunQueryResponse;
import com.google.datastore.v1beta3.client.Datastore;
import com.google.datastore.v1beta3.client.DatastoreException;
import com.google.datastore.v1beta3.client.DatastoreFactory;
import com.google.datastore.v1beta3.client.DatastoreHelper;
import com.google.datastore.v1beta3.client.DatastoreOptions;
import com.google.datastore.v1beta3.client.QuerySplitter;
import com.google.protobuf.Int32Value;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * A Datstore Source for v1beta3 API.
 */
public class V1Beta3Source extends BoundedSource<Entity>{
  private static final Logger LOG = LoggerFactory.getLogger(V1Beta3Source.class);
  private final String projectId;
  private final Query query;
  @Nullable
  private final String namespace;

  /**
   * These classes do not extend {@link java.io.Serializable}. Hence they need to be created
   * every time a this source is instantiated from its serialized form.
   *
   * They need to be accessed through {@link V1Beta3Source#getDatastoreFactory()} and
   * {@link V1Beta3Source#getQuerySplitter()}
   */
  private transient DatastoreFactory datastoreFactory;
  private transient QuerySplitter querySplitter;

  V1Beta3Source(String projectId, Query query, @Nullable String namespace) {
    this.projectId = projectId;
    this.query = query;
    this.namespace = namespace;
  }

  private Datastore getDatastore(PipelineOptions pipelineOptions) {
    DatastoreOptions.Builder builder =
        new DatastoreOptions.Builder()
            .projectId(projectId)
            .initializer(
                new RetryHttpRequestInitializer()
            );

    Credential credential = pipelineOptions.as(GcpOptions.class).getGcpCredential();
    if (credential != null) {
      builder.credential(credential);
    }
    return getDatastoreFactory().get().create(builder.build());
  }

  // Creates (if not already) and returns a datastore factory.
  private DatastoreFactory getDatastoreFactory() {
    if (datastoreFactory == null) {
      datastoreFactory = DatastoreFactory.get();
    }
    return datastoreFactory;
  }

  // Creates (if not already) and returns a query splitter.
  private QuerySplitter getQuerySplitter() {
    if (querySplitter == null) {
      querySplitter = DatastoreHelper.getQuerySplitter();
    }
    return querySplitter;
  }

  /**
   * Builds a {@link RunQueryRequest} from the {@code query}, using the properties set on this
   * {@code V1Beta3Source}. For example, sets the {@code namespace} for the request.
   */
  private RunQueryRequest makeRequest(Query query) {
    RunQueryRequest.Builder requestBuilder = RunQueryRequest.newBuilder().setQuery(query);
    if (namespace != null) {
      requestBuilder.getPartitionIdBuilder().setNamespaceId(namespace);
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
    query.setLimit(Int32Value.newBuilder().setValue(1));
    RunQueryRequest request = makeRequest(query.build());

    long now = System.currentTimeMillis();
    RunQueryResponse response = datastore.runQuery(request);
    LOG.info("Query for latest stats timestamp of project {} took {}ms", projectId,
        System.currentTimeMillis() - now);
    QueryResultBatch batch = response.getBatch();
    if (batch.getEntityResultsCount() == 0) {
      throw new NoSuchElementException(
          "Datastore total statistics for project " + projectId + " unavailable");
    }
    Entity entity = batch.getEntityResults(0).getEntity();
    return entity.getProperties().get("timestamp").getTimestampValue().getNanos();
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    // Datastore provides no way to get a good estimate of how large the result of a query
    // will be. As a rough approximation, we attempt to fetch the statistics of the whole
    // entity kind being queried, using the __Stat_Kind__ system table, assuming exactly 1 kind
    // is specified in the query.
    //
    // See https://cloud.google.com/datastore/docs/concepts/stats
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
    query.setFilter(makeAndFilter(
        makeFilter("kind_name", EQUAL, makeValue(ourKind)).build(),
        makeFilter("timestamp", EQUAL, makeValue(latestTimestamp)).build()));
    RunQueryRequest request = makeRequest(query.build());

    long now = System.currentTimeMillis();
    RunQueryResponse response = datastore.runQuery(request);
    LOG.info("Query for per-kind statistics took {}ms", System.currentTimeMillis() - now);

    QueryResultBatch batch = response.getBatch();
    if (batch.getEntityResultsCount() == 0) {
      throw new NoSuchElementException(
          "Datastore statistics for kind " + ourKind + " unavailable");
    }
    Entity entity = batch.getEntityResults(0).getEntity();
    return entity.getProperties().get("entity_bytes").getIntegerValue();
  }

  /**
   * A helper function to get the split queries, taking into account the optional
   * {@code namespace} and whether there is a mock splitter.
   */
  private List<Query> splitQuery(int numSplits, PipelineOptions options)
      throws DatastoreException {
    // If namespace is set, include it in the split request so splits are calculated accordingly.
    PartitionId.Builder partitionBuilder = PartitionId.newBuilder();
    if (namespace != null) {
      partitionBuilder.setNamespaceId(namespace);
    }

    return getQuerySplitter().getSplits(
        query, partitionBuilder.build(), numSplits, getDatastore(options));
  }

  @Override
  public List<V1Beta3Source> splitIntoBundles(long desiredBundleSizeBytes,
      PipelineOptions options) throws Exception {
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
      datastoreSplits = splitQuery(Ints.checkedCast(numSplits), options);
    } catch (IllegalArgumentException | DatastoreException e) {
      LOG.warn("Unable to parallelize the given query: {}", query, e);
      return ImmutableList.of(this);
    }

    ImmutableList.Builder<V1Beta3Source> splits = ImmutableList.builder();
    for (Query splitQuery : datastoreSplits) {
      splits.add(new V1Beta3Source(projectId, splitQuery, namespace));
    }
    return splits.build();
  }

  @Override
  public Coder<Entity> getDefaultOutputCoder() {
    return ProtoCoder.of(Entity.class);
  }

  @Override
  public boolean producesSortedKeys(PipelineOptions options) {
    return false;
  }

  @Override
  public BoundedReader<Entity> createReader(PipelineOptions pipelineOptions) throws IOException {
    return new V1Beta3Reader(this, getDatastore(pipelineOptions));
  }

  @Override
  public void validate() {
    checkNotNull(query, "query");
    checkNotNull(projectId, "projectId");
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder
        .addIfNotNull(DisplayData.item("projectId", projectId)
            .withLabel("ProjectId"))
        .addIfNotNull(DisplayData.item("namespace", namespace)
            .withLabel("Namespace"))
        .addIfNotNull(DisplayData.item("query", query.toString())
            .withLabel("Query"));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("projectId", projectId)
        .add("query", query)
        .add("namespace", namespace)
        .toString();
  }

  @VisibleForTesting
  Query getQuery() {
    return query;
  }

  @VisibleForTesting
  static class V1Beta3Reader extends BoundedSource.BoundedReader<Entity> {
    private final V1Beta3Source source;

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

    private volatile boolean done = false;

    private Entity currentEntity;

    /**
     * Returns a V1Beta3Reader with V1Beta3Source and Datastore object set.
     *
     * @param datastore a datastore connection to use.
     */
    public V1Beta3Reader(V1Beta3Source source, Datastore datastore) {
      this.source = source;
      this.datastore = datastore;
      // If the user set a limit on the query, remember it. Otherwise pin to MAX_VALUE.
      userLimit = source.query.hasLimit()
          ? source.query.getLimit().getValue() : Integer.MAX_VALUE;
    }

    @Override
    public Entity getCurrent() {
      return currentEntity;
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
      if (entities == null || (!entities.hasNext() && moreResults)) {
        try {
          entities = getIteratorAndMoveCursor();
        } catch (DatastoreException e) {
          throw new IOException(e);
        }
      }

      if (entities == null || !entities.hasNext()) {
        currentEntity = null;
        done = true;
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
    public V1Beta3Source getCurrentSource() {
      return source;
    }

    @Override
    public V1Beta3Source splitAtFraction(double fraction) {
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
      query.setLimit(Int32Value.newBuilder().setValue(Math.min(userLimit, QUERY_BATCH_LIMIT)));
      if (currentBatch != null && !currentBatch.getEndCursor().isEmpty()) {
        query.setStartCursor(currentBatch.getEndCursor());
      }

      RunQueryRequest request = source.makeRequest(query.build());
      RunQueryResponse response = datastore.runQuery(request);

      currentBatch = response.getBatch();

      // MORE_RESULTS_AFTER_LIMIT is not implemented yet:
      // https://groups.google.com/forum/#!topic/gcd-discuss/iNs6M1jA2Vw, so
      // use result count to determine if more results might exist.
      int numFetch = currentBatch.getEntityResultsCount();
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
              && ((numFetch == QUERY_BATCH_LIMIT)
              || (currentBatch.getMoreResults() == NOT_FINISHED));

      // May receive a batch of 0 results if the number of records is a multiple
      // of the request limit.
      if (numFetch == 0) {
        return null;
      }

      return currentBatch.getEntityResultsList().iterator();
    }
  }
}
