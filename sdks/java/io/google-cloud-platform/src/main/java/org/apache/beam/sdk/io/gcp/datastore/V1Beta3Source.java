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
import com.google.common.collect.ImmutableList;
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
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * A Datstore Source for v1beta3 API.
 */
class V1Beta3Source extends BoundedSource<Entity> {
  private static final Logger LOG = LoggerFactory.getLogger(V1Beta3Source.class);

  /**
   * A limit on the maximum number of sub-queries a query can be split into.
   * TODO: Figure out an appropriate value
   */
  static final int MAX_QUERY_SPLITS = 100000;

  /**
   * Specifies the factor by which we oversplit queries assigned to bundles.
   */
  static final int QUERY_SPLIT_FACTOR = 100;

  private final String projectId;

  /**
   * The datastore query provided during creation of this source. It remains same
   * across all sub-sources created either through initial splits or dynamic rebalance.
   */
  private final Query mainQuery;

  /**
   * List of queries that this source needs to read from.
   */
  private final ImmutableList<Query> querySplits;

  @Nullable
  private final String namespace;

  /**
   * True if this source has been created after an initial split through
   * {@link V1Beta3Source#splitIntoBundles}.
   */
  private final boolean isSubSource;

  /**
   * These classes do not extend {@link java.io.Serializable}. Hence they need to be created
   * every time a this source is instantiated from its serialized form.
   *
   * They need to be accessed through {@link V1Beta3Source#getDatastoreFactory()} and
   * {@link V1Beta3Source#getQuerySplitter()}
   */
  private transient DatastoreFactory datastoreFactory;
  private transient QuerySplitter querySplitter;

  public V1Beta3Source(String projectId, Query query, @Nullable String namespace) {
    this(projectId, query, ImmutableList.of(query), namespace, false);
  }

  private V1Beta3Source(String projectId, Query query, ImmutableList<Query> querySplits,
      @Nullable String namespace, boolean isSubSource) {
    this.mainQuery = query;
    this.querySplits = querySplits;
    this.projectId = projectId;
    this.namespace = namespace;
    this.isSubSource = isSubSource;
  }

  /**
   * Only used for testing purposes to inject {@param datastoreFactory} and {@param querySplitter}.
   */
  @VisibleForTesting
  V1Beta3Source(String projectId, Query query, ImmutableList<Query> querySplits,
      @Nullable String namespace, boolean isSubSource, DatastoreFactory datastoreFactory,
      QuerySplitter querySplitter) {
    this.mainQuery = query;
    this.querySplits = querySplits;
    this.projectId = projectId;
    this.namespace = namespace;
    this.isSubSource = isSubSource;
    this.datastoreFactory = datastoreFactory;
    this.querySplitter = querySplitter;
  }

  // Returns a new instance of a Datastore client.
  private static Datastore getDatastore(PipelineOptions options, String projectId,
      DatastoreFactory datastoreFactory) {
    DatastoreOptions.Builder builder =
        new DatastoreOptions.Builder()
            .projectId(projectId)
            .initializer(
                new RetryHttpRequestInitializer()
            );

    Credential credential = options.as(GcpOptions.class).getGcpCredential();
    if (credential != null) {
      builder.credential(credential);
    }
    return datastoreFactory.create(builder.build());
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
   * {@code DatastoreSource}. For example, sets the {@code namespace} for the request.
   */
  @VisibleForTesting
  RunQueryRequest makeRequest(Query query) {
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
  long queryLatestStatisticsTimestamp(Datastore datastore) throws DatastoreException {
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
    Datastore datastore = getDatastore(options, projectId, getDatastoreFactory());

    if (mainQuery.getKindCount() != 1) {
      throw new UnsupportedOperationException(
          "Can only estimate size for queries specifying exactly 1 kind.");
    }
    String ourKind = mainQuery.getKind(0).getName();
    long latestTimestamp = queryLatestStatisticsTimestamp(datastore);
    Query.Builder statQuery = Query.newBuilder();
    if (namespace == null) {
      statQuery.addKindBuilder().setName("__Stat_Kind__");
    } else {
      statQuery.addKindBuilder().setName("__Ns_Stat_Kind__");
    }
    statQuery.setFilter(makeAndFilter(
        makeFilter("kind_name", EQUAL, makeValue(ourKind)).build(),
        makeFilter("timestamp", EQUAL, makeValue(latestTimestamp)).build()));
    RunQueryRequest request = makeRequest(statQuery.build());

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
   * A helper function to get the split queries, taking into account the optional {@code namespace}.
   */
  private List<Query> splitQuery(Query query, int numSplits, PipelineOptions options)
      throws DatastoreException {
    // If namespace is set, include it in the split request so splits are calculated accordingly.
    PartitionId.Builder partitionBuilder = PartitionId.newBuilder();
    if (namespace != null) {
      partitionBuilder.setNamespaceId(namespace);
    }

    Datastore datastore = getDatastore(options, projectId, getDatastoreFactory());

    return getQuerySplitter().getSplits(
        query, partitionBuilder.build(), numSplits, datastore);
  }

  @Override
  public List<V1Beta3Source> splitIntoBundles(long desiredBundleSizeBytes,
      PipelineOptions options) throws Exception {

    // Users may request a limit on the number of results. We can currently support this by
    // simply disabling parallel reads and using only a single split.
    if (mainQuery.hasLimit()) {
      return ImmutableList.of(this);
    }

    // Queries underlying a sub-source have already been oversplit, so don't split further.
    if (isSubSource) {
      return ImmutableList.of(this);
    }

    long numSubSources;
    try {
      numSubSources =
          Math.round(((double) getEstimatedSizeBytes(options)) / desiredBundleSizeBytes);
    } catch (Exception e) {
      // Fallback in case estimated size is unavailable. TODO: fix this, it's horrible.
      numSubSources = 12;
    }

    // If the desiredBundleSize or number of workers results in 1 split, simply return
    // a source that reads from the original query.
    if (numSubSources <= 1) {
      return ImmutableList.of(this);
    }

    List<Query> splitQueries;
    try {
      // safe to cast as MAX_QUERY_SPLIT is an int
      int numQuerySplits = (int) Math.min(numSubSources * QUERY_SPLIT_FACTOR, MAX_QUERY_SPLITS);
      splitQueries = splitQuery(mainQuery, numQuerySplits, options);
    } catch (IllegalArgumentException | DatastoreException e) {
      LOG.warn("Unable to parallelize the given query: {}", mainQuery, e);
      return ImmutableList.of(this);
    }

    return subSourcesFromQueries(splitQueries, numSubSources);
  }

  // Group the queries into sub-sources
  private List<V1Beta3Source> subSourcesFromQueries(List<Query> queries, long numSubSources) {
    int numQueries = queries.size();
    // If number of sub-sources is more than queries, limit it to number of queries
    if (numSubSources > numQueries) {
      numSubSources = numQueries;
    }

    List<V1Beta3Source> subSources = new LinkedList<>();

    /*
     * The problem here is to split a list of N items into K buckets, such that,
     *  1. All buckets have almost equal items (minimize the maximum difference)
     *  2. All the items in a bucket are adjacent to each other and in the same order
     *     as in the list. (Not a strict requirement but nice to have)
     *
     *  To achieve this we need to have the first (N % K) buckets contain (1 + N/K) items and
     *  remaining buckets to have N/K items.
     *  In this case, N is numQueries, K is numSubSources.
     */
    int start = 0;
    for (int i = 0; i < numSubSources; i++) {
      int numQueriesForIndex = (int) (numQueries / numSubSources)
          + Math.min(1, (int) (numQueries % numSubSources) / (i + 1));

      int end = start + numQueriesForIndex;
      subSources.add(createSubSource(queries, start, end));
      start = end;
    }

    return subSources;
  }

  private V1Beta3Source createSubSource(List<Query> queries, int start, int end) {
    ImmutableList<Query> subQueries = ImmutableList.copyOf(queries.subList(start, end));
    return new V1Beta3Source(projectId, mainQuery, subQueries, namespace, true);
  }

  @VisibleForTesting
  boolean isSubSource() {
    return isSubSource;
  }

  @Override
  public boolean producesSortedKeys(PipelineOptions options) throws Exception {
    return false;
  }

  @Override
  public BoundedReader<Entity> createReader(PipelineOptions options) throws IOException {
    return new V1Beta3Reader(this, getDatastore(options, projectId, getDatastoreFactory()));
  }

  @Override
  public void validate() {
    checkNotNull(mainQuery, "query");
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
        .addIfNotNull(DisplayData.item("query", mainQuery.toString())
            .withLabel("Query"));
  }

  @Override
  public Coder<Entity> getDefaultOutputCoder() {
    return ProtoCoder.of(Entity.class);
  }

  @VisibleForTesting
  ImmutableList<Query> getQueries() {
    return querySplits;
  }

  private boolean hasQueryLimit() {
    return mainQuery.hasLimit();
  }

  /**
   * A {@link V1Beta3Source.Reader} over the records from a query of the datastore.
   *
   * <p>Timestamped records are currently not supported.
   * All records implicitly have the timestamp of {@code BoundedWindow.TIMESTAMP_MIN_VALUE}.
   */
  static class V1Beta3Reader extends BoundedReader<Entity> {
    private V1Beta3Source source;
    // Datastore client
    private final Datastore datastore;
    private Entity currentEntity;

    /**
     * The index of sub-query that is currently being processed.
     * Each sub-query here is a split point
     */
    private int currentSplitPointIndex = 0;

    /**
     * Remaining user-requested limit on the number of entities to return. If the user did not set a
     * limit, then this variable will always have the value {@link Integer#MAX_VALUE} and will never
     * be decremented.
     */
    private int userLimit;

    // True if more results may be available.
    private boolean moreResults;
    // Iterator over records
    private java.util.Iterator<EntityResult> entities;
    // Current batch of query results
    private QueryResultBatch currentBatch;

    /**
     * Maximum number of results to request per query.
     *
     * <p>Must be set, or it may result in an I/O error when querying
     * Cloud Datastore.
     */
    static final int QUERY_BATCH_LIMIT = 500;

    V1Beta3Reader(V1Beta3Source source, Datastore datastore) {
      this.source = source;
      this.datastore = datastore;
      // If the user set a limit on the query, remember it. Otherwise pin to MAX_VALUE.
      userLimit = source.hasQueryLimit()
          ? source.mainQuery.getLimit().getValue() : Integer.MAX_VALUE;
    }

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      while (!advancePerSplitPoint()) {
        // Synchronize the split point update with dynamic splitting threads.
        synchronized (this) {
          currentSplitPointIndex++;
        }

        currentBatch = null;
        entities = null;

        if (currentSplitPointIndex >= numSplitPoints()) {
          return false;
        }
      }

      return true;
    }

    // Advance method for each sub-query (split point)
    private boolean advancePerSplitPoint() throws IOException {
      if (entities == null || (!entities.hasNext() && moreResults)) {
        try {
          entities = getIteratorAndMoveCursor(getCurrentQuery());
        } catch (DatastoreException e) {
          throw new IOException(e);
        }
      }

      if (entities == null || !entities.hasNext()) {
        return false;
      } else {
        currentEntity = entities.next().getEntity();
        return true;
      }
    }

    /**
     * Returns an iterator over the next batch of records for the {@param query}
     * and updates the cursor to get the next batch as needed.
     * Query has specified limit and offset from InputSplit.
     */
    @VisibleForTesting
    Iterator<EntityResult> getIteratorAndMoveCursor(Query query) throws DatastoreException {
      Query.Builder queryBuilder = query.toBuilder().clone();
      queryBuilder.setLimit(Int32Value.newBuilder().setValue(
          Math.min(userLimit, QUERY_BATCH_LIMIT)));

      if (currentBatch != null && !currentBatch.getEndCursor().isEmpty()) {
        queryBuilder.setStartCursor(currentBatch.getEndCursor());
      }

      RunQueryRequest request = getCurrentSource().makeRequest(queryBuilder.build());
      RunQueryResponse response = datastore.runQuery(request);

      currentBatch = response.getBatch();

      // MORE_RESULTS_AFTER_LIMIT is not implemented yet:
      // https://groups.google.com/forum/#!topic/gcd-discuss/iNs6M1jA2Vw, so
      // use result count to determine if more results might exist.
      int numFetch = currentBatch.getEntityResultsCount();
      if (getCurrentSource().hasQueryLimit()) {
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

    // Returns the total number of split points assigned to this source
    private int numSplitPoints() {
      return getCurrentSource().getQueries().size();
    }

    // Returns the query representing the current split point
    private Query getCurrentQuery() {
      return getCurrentSource().getQueries().get(currentSplitPointIndex);
    }

    @Override
    public void close() throws IOException {
      // Nothing
    }

    @Override
    public Entity getCurrent() {
      return currentEntity;
    }

    @Override
    public synchronized V1Beta3Source getCurrentSource() {
      return source;
    }

    @Override
    public synchronized V1Beta3Source splitAtFraction(double fraction) {
      V1Beta3Source source = getCurrentSource();

      // If the query has a limit set, then do not split.
      if (source.hasQueryLimit()) {
        return null;
      }

      int splitIndex = (int) Math.round(numSplitPoints() * fraction);
      int startIndex = 0;
      int endIndex = numSplitPoints();

      if (splitIndex >= endIndex) {
        return null;
      }

      // Already past the split index
      if (splitIndex <= currentSplitPointIndex) {
        return null;
      }

      V1Beta3Source primary = source.createSubSource(getCurrentSource().getQueries(), startIndex,
          splitIndex);

      V1Beta3Source residual = source.createSubSource(getCurrentSource().getQueries(),
          splitIndex, endIndex);

      this.source = primary;

      return residual;
    }

    @Override
    public synchronized Double getFractionConsumed() {
      double fractionConsumed = currentSplitPointIndex / numSplitPoints();
      return Math.min(1.0, fractionConsumed);
    }

    @Override
    public synchronized long getSplitPointsConsumed() {
      return currentSplitPointIndex;
    }

    @Override
    public synchronized long getSplitPointsRemaining() {
      return numSplitPoints() - currentSplitPointIndex;
    }
  }
}
