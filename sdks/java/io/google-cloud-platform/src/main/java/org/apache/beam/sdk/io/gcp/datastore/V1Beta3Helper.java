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
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeFilter;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeOrder;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeValue;

import org.apache.beam.sdk.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
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
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.Nullable;

/**
 * A helper class for v1beta3 datastore related functionality.
 */
class V1Beta3Helper {
  private static final Logger LOG = LoggerFactory.getLogger(V1Beta3Helper.class);

  // Default bundle size of 64MB
  static final long DEFAULT_BUNDLE_SIZE_BYTES = 64 * 1024 * 1024;

  // A lower bound on the number of splits for a query
  static final int NUM_QUERY_SPLITS_MIN = 12;

  /**
   * Builds a datastore client for the given {@param pipelineOptions} and {@param projectId}.
   */
  static Datastore getDatastore(DatastoreFactory datastoreFactory, PipelineOptions pipelineOptions,
      String projectId) {
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
    return datastoreFactory.create(builder.build());
  }

  /**
   * Computes the number of splits to be performed on the given {@param query} by
   * querying the estimated size from datastore. We use {@code DEFAULT_BUNDLE_SIZE_BYTES} as the
   * bundle size.
   */
  static int getEstimatedNumSplits(Datastore datastore, Query query, String namespace) {
    int numSplits;
    try {
      long estimatedSizeBytes = getEstimatedSizeBytes(datastore, query, namespace);
      numSplits = (int) Math.min(Integer.MAX_VALUE,
          Math.round(((double) estimatedSizeBytes) / DEFAULT_BUNDLE_SIZE_BYTES));
    } catch (Exception e) {
      LOG.warn("Failed the fetch estimatedSizeBytes for query: {}", query, e);
      // Fallback in case estimated size is unavailable.
      numSplits = NUM_QUERY_SPLITS_MIN;
    }
    return Math.max(numSplits, NUM_QUERY_SPLITS_MIN);
  }

  /**
   * Get the estimated size of the data returned by the given query.
   *
   * Datastore provides no way to get a good estimate of how large the result of a query
   * entity kind being queried, using the __Stat_Kind__ system table, assuming exactly 1 kind
   * is specified in the query.
   *
   * See https://cloud.google.com/datastore/docs/concepts/stats
   */
  static long getEstimatedSizeBytes(Datastore datastore, Query query, String namespace)
      throws DatastoreException {
    String ourKind = query.getKind(0).getName();
    Query.Builder queryBuilder = Query.newBuilder();
    if (namespace == null) {
      queryBuilder.addKindBuilder().setName("__Stat_Kind__");
    } else {
      queryBuilder.addKindBuilder().setName("__Ns_Stat_Kind__");
    }
    queryBuilder.setFilter(makeFilter("kind_name", EQUAL, makeValue(ourKind).build()));

    // Get the latest statistics
    queryBuilder.addOrder(makeOrder("timestamp", DESCENDING));
    queryBuilder.setLimit(Int32Value.newBuilder().setValue(1));

    RunQueryRequest request = makeRequest(queryBuilder.build(), namespace);

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
   * Builds a {@link RunQueryRequest} from the {@code query} and {@code namespace}.
   */
  static RunQueryRequest makeRequest(Query query, String namespace) {
    RunQueryRequest.Builder requestBuilder = RunQueryRequest.newBuilder().setQuery(query);
    if (namespace != null) {
      requestBuilder.getPartitionIdBuilder().setNamespaceId(namespace);
    }
    return requestBuilder.build();
  }

  /**
   * A helper function to get the split queries, taking into account the optional
   * {@code namespace}.
   */
  private static List<Query> splitQuery(Query query, String namespace, Datastore datastore,
      QuerySplitter querySplitter, int numSplits) throws DatastoreException {
    // If namespace is set, include it in the split request so splits are calculated accordingly.
    PartitionId.Builder partitionBuilder = PartitionId.newBuilder();
    if (namespace != null) {
      partitionBuilder.setNamespaceId(namespace);
    }

    return querySplitter.getSplits(query, partitionBuilder.build(), numSplits, datastore);
  }

  /**
   * A class that applies random integer key to a given element of type {@param T}.
   */
  static class ApplyRandomIntegerKey<T> implements SerializableFunction<T, Integer> {
    @Override
    public Integer apply(T input) {
        return ThreadLocalRandom.current().nextInt();
    }
  }

  /**
   * A class for v1beta3 datastore related options.
   */
  static class V1Beta3Options implements Serializable {
    private final Query query;
    private final String projectId;
    @Nullable
    private final String namespace;

    public V1Beta3Options(String projectId, Query query, String namespace) {
      checkNotNull(projectId, "projectId");
      checkNotNull(query, "query");
      this.projectId = projectId;
      this.query = query;
      this.namespace = namespace;
    }

    public static V1Beta3Options from(String projectId, Query query, String namespace) {
      return new V1Beta3Options(projectId, query, namespace);
    }

    public Query getQuery() {
      return query;
    }

    public String getProjectId() {
      return projectId;
    }

    @Nullable
    public String getNamespace() {
      return namespace;
    }
  }

  /**
   * A {@link DoFn} that splits a given query into multiple sub-queries.
   */
  static class SplitQueryFn extends DoFn<Query, Query> {
    private final V1Beta3Options options;
    // number of splits to make for a given query
    private final int numSplits;

    // Datastore client
    private transient Datastore datastore;
    // Query splitter
    private transient QuerySplitter querySplitter;

    public SplitQueryFn(V1Beta3Options options, int numSplits) {
      this.options = options;
      this.numSplits = numSplits;
    }

    @VisibleForTesting
    SplitQueryFn(V1Beta3Options options, int numSplits, Datastore datastore,
        QuerySplitter querySplitter) {
      this.options = options;
      this.numSplits = numSplits;
      this.datastore = datastore;
      this.querySplitter = querySplitter;
    }

    @Override
    public void startBundle(Context c) throws Exception {
      datastore = getDatastore(DatastoreFactory.get(), c.getPipelineOptions(),
          options.getProjectId());
      querySplitter = DatastoreHelper.getQuerySplitter();
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      Query query = c.element();

      // If query has a user set limit, then do not split.
      if (query.hasLimit()) {
        c.output(query);
        return;
      }

      int estimatedNumSplits;
      // Compute the estimated numSplits if numSplits is not specified by the user.
      if (numSplits == 0) {
        estimatedNumSplits = getEstimatedNumSplits(datastore, query, options.getNamespace());
      } else {
        estimatedNumSplits = numSplits;
      }

      List<Query> querySplits;
      try {
        querySplits = splitQuery(query, options.getNamespace(), datastore, querySplitter,
            estimatedNumSplits);
      } catch (Exception e) {
        LOG.warn("Unable to parallelize the given query: {}", query, e);
        querySplits = ImmutableList.of(query);
      }

      for (Query subquery : querySplits) {
        c.output(subquery);
      }
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("projectId", options.getProjectId())
              .withLabel("ProjectId"))
          .addIfNotNull(DisplayData.item("namespace", options.getNamespace())
              .withLabel("Namespace"))
          .addIfNotNull(DisplayData.item("query", options.getQuery().toString())
              .withLabel("Query"));
    }
  }

  /**
   * A {@link DoFn} that reads entities from datastore for each query.
   */
  static class ReadFn extends DoFn<Query, Entity> {
    private final V1Beta3Options options;
    // Datastore client
    private transient Datastore datastore;

    public ReadFn(V1Beta3Options options) {
      this.options = options;
    }

    @VisibleForTesting
    ReadFn(V1Beta3Options options, Datastore datastore) {
      this.options = options;
      this.datastore = datastore;
    }

    @Override
    public void startBundle(Context c) throws Exception {
      datastore = getDatastore(DatastoreFactory.get(), c.getPipelineOptions(),
          options.getProjectId());
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      Reader reader = createReader(c.element());
      if (reader.start()) {
        do {
          c.output(reader.getCurrent());
        } while(reader.advance());
      }
    }

    // Creates a new reader for the given query
    @VisibleForTesting
    Reader createReader(Query query) {
      return new Reader(query, options.getNamespace(), datastore);
    }
  }

  /**
   * A {@link org.apache.beam.sdk.io.Source.Reader} style reader to read entities
   * from datastore.
   */
  static class Reader {
    private final Query query;

    private final String namespace;

    // Datastore client
    private final Datastore datastore;

    // True if more results may be available.
    private boolean moreResults;

    // Iterator over records
    private java.util.Iterator<EntityResult> entities;

    // Current batch of query results.
    private QueryResultBatch currentBatch;

    /**
     * Maximum number of results to request per query.
     *
     * <p>Must be set, or it may result in an I/O error when querying Cloud Datastore.
     */
    static final int QUERY_BATCH_LIMIT = 500;

    /**
     * Remaining user-requested limit on the number of queries to return. If the user did not set a
     * limit, then this variable will always have the value {@link Integer#MAX_VALUE} and will never
     * be decremented.
     */
    private int userLimit;

    // The most recently read entity from a call to either start or advance method.
    private Entity currentEntity;

    Reader(Query query, String namespace, Datastore datastore) {
      this.query = query;
      this.namespace = namespace;
      this.datastore = datastore;
      userLimit = query.hasLimit()
          ? query.getLimit().getValue() : Integer.MAX_VALUE;
    }

    /**
     * Initializes the reader and advances the reader to the first record.
     *
     * <p>This method should be called exactly once. The invocation should occur prior to calling
     * {@link #advance} or {@link #getCurrent}.
     *
     * @return {@code true} if a record was read, {@code false} if there is no more input available.
     */
    public boolean start() throws IOException {
      return advance();
    }

    /**
     * Advances the reader to the next valid record.
     *
     * <p>It is an error to call this without having called {@link #start} first.
     *
     * @return {@code true} if a record was read, {@code false} if there is no more input available.
     */
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

    /**
     * Returns an iterator over the next batch of records for the query
     * and updates the cursor to get the next batch as needed.
     * Query has specified limit and offset from InputSplit.
     */
    private Iterator<EntityResult> getIteratorAndMoveCursor() throws DatastoreException {
      Query.Builder queryBuilder = query.toBuilder().clone();
      queryBuilder.setLimit(Int32Value.newBuilder().setValue(
          Math.min(userLimit, QUERY_BATCH_LIMIT)));

      if (currentBatch != null && !currentBatch.getEndCursor().isEmpty()) {
        queryBuilder.setStartCursor(currentBatch.getEndCursor());
      }

      RunQueryRequest request = makeRequest(queryBuilder.build(), namespace);
      RunQueryResponse response = datastore.runQuery(request);

      currentBatch = response.getBatch();

      // MORE_RESULTS_AFTER_LIMIT is not implemented yet:
      // https://groups.google.com/forum/#!topic/gcd-discuss/iNs6M1jA2Vw, so
      // use result count to determine if more results might exist.
      int numFetch = currentBatch.getEntityResultsCount();
      if (query.hasLimit()) {
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

    /**
     * Returns the value of the data item that was read by the last {@link #start} or
     * {@link #advance} call.
     *
     * <p>Multiple calls to this method without an intervening call to {@link #advance} should
     * return the same result.
     */
    public Entity getCurrent() {
      return currentEntity;
    }
  }
}
