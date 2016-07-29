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

import static org.apache.beam.sdk.io.gcp.datastore.V1Beta3.Read.NUM_QUERY_SPLITS_MAX;
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
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.values.KV;

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

import java.io.Serializable;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * A helper class for v1beta3 version of Google Cloud Datastore related functionality.
 */
class V1Beta3Helper {
  private static final Logger LOG = LoggerFactory.getLogger(V1Beta3Helper.class);

  /** Default bundle size of 64MB. */
  static final long DEFAULT_BUNDLE_SIZE_BYTES = 64 * 1024 * 1024;

  /** A lower bound on the number of splits for a query. */
  static final int NUM_QUERY_SPLITS_MIN = 12;

  /**
   * Maximum number of results to request per query.
   *
   * <p>Must be set, or it may result in an I/O error when querying Cloud Datastore.
   */
  static final int QUERY_BATCH_LIMIT = 500;

  /**
   * Computes the number of splits to be performed on the given query by querying the estimated size
   * from Datastore.
   */
  static int getEstimatedNumSplits(Datastore datastore, Query query, String namespace) {
    int numSplits;
    try {
      long estimatedSizeBytes = getEstimatedSizeBytes(datastore, query, namespace);
      numSplits = (int) Math.min(NUM_QUERY_SPLITS_MAX,
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
   * <p>Datastore provides no way to get a good estimate of how large the result of a query
   * entity kind being queried, using the __Stat_Kind__ system table, assuming exactly 1 kind
   * is specified in the query.
   *
   * <p>See https://cloud.google.com/datastore/docs/concepts/stats.
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
    LOG.debug("Query for per-kind statistics took {}ms", System.currentTimeMillis() - now);

    QueryResultBatch batch = response.getBatch();
    if (batch.getEntityResultsCount() == 0) {
      throw new NoSuchElementException(
          "Datastore statistics for kind " + ourKind + " unavailable");
    }
    Entity entity = batch.getEntityResults(0).getEntity();
    return entity.getProperties().get("entity_bytes").getIntegerValue();
  }

  /** Builds a {@link RunQueryRequest} from the {@code query} and {@code namespace}. */
  static RunQueryRequest makeRequest(Query query, String namespace) {
    RunQueryRequest.Builder requestBuilder = RunQueryRequest.newBuilder().setQuery(query);
    if (namespace != null) {
      requestBuilder.getPartitionIdBuilder().setNamespaceId(namespace);
    }
    return requestBuilder.build();
  }

  /**
   * A helper function to get the split queries, taking into account the optional {@code namespace}.
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
   * A class for v1beta3 Datastore related options.
   */
  static class V1Beta3Options implements Serializable {
    private final Query query;
    private final String projectId;
    @Nullable
    private final String namespace;

    public V1Beta3Options(String projectId, Query query, String namespace) {
      this.projectId = checkNotNull(projectId, "projectId");
      this.query = checkNotNull(query, "query");
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
   * A {@link DoFn} that splits a given query into multiple sub-queries, assigns them unique keys
   * and outputs a {@link KV<Integer, Query>}.
   */
  static class SplitQueryFn extends DoFn<Query, KV<Integer, Query>> {
    private final V1Beta3Options options;
    // number of splits to make for a given query
    private final int numSplits;

    private final V1Beta3DatastoreFactory datastoreFactory;
    // Datastore client
    private transient Datastore datastore;
    // Query splitter
    private transient QuerySplitter querySplitter;

    public SplitQueryFn(V1Beta3Options options, int numSplits) {
      this(options, numSplits, new V1Beta3DatastoreFactory());
    }

    @VisibleForTesting
    SplitQueryFn(V1Beta3Options options, int numSplits, V1Beta3DatastoreFactory datastoreFactory) {
      this.options = options;
      this.numSplits = numSplits;
      this.datastoreFactory = datastoreFactory;
    }

    @Override
    public void startBundle(Context c) throws Exception {
      datastore = datastoreFactory.getDatastore(c.getPipelineOptions(), options.projectId);
      querySplitter = datastoreFactory.getQuerySplitter();
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      int key = 1;
      Query query = c.element();

      // If query has a user set limit, then do not split.
      if (query.hasLimit()) {
        c.output(KV.of(key, query));
        return;
      }

      int estimatedNumSplits;
      // Compute the estimated numSplits if numSplits is not specified by the user.
      if (numSplits <= 0) {
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

      // assign unique keys to query splits.
      for (Query subquery : querySplits) {
        c.output(KV.of(key++, subquery));
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
   * A {@link DoFn} that reads entities from Datastore for each query.
   */
  static class ReadFn extends DoFn<Query, Entity> {
    private final V1Beta3Options options;
    private final V1Beta3DatastoreFactory datastoreFactory;
    // Datastore client
    private transient Datastore datastore;

    public ReadFn(V1Beta3Options options) {
      this(options, new V1Beta3DatastoreFactory());
    }

    @VisibleForTesting
    ReadFn(V1Beta3Options options, V1Beta3DatastoreFactory datastoreFactory) {
      this.options = options;
      this.datastoreFactory = datastoreFactory;
    }

    @Override
    public void startBundle(Context c) throws Exception {
      datastore = datastoreFactory.getDatastore(c.getPipelineOptions(), options.getProjectId());
    }

    /** Read and output entities for the given query. */
    private void processQuery(ProcessContext context,
        Query query, String namespace, Datastore datastore) throws Exception {
      int userLimit = query.hasLimit()
          ? query.getLimit().getValue() : Integer.MAX_VALUE;

      boolean moreResults = true;
      QueryResultBatch currentBatch = null;

      while (moreResults) {
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

        // output all the entities from the current batch.
        for (EntityResult entityResult: currentBatch.getEntityResultsList()) {
          context.output(entityResult.getEntity());
        }

        // Check if we have more entities to be read.
        moreResults =
            // User-limit does not exist (so userLimit == MAX_VALUE) and/or has not been satisfied.
            (userLimit > 0)
                // All indications from the API are that there are/may be more results.
                && ((numFetch == QUERY_BATCH_LIMIT)
                || (currentBatch.getMoreResults() == NOT_FINISHED));
      }
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      processQuery(c, c.element(), options.getNamespace(), datastore);
    }
  }

  /**
   * A wrapper factory class for Datastore singleton classes {@link DatastoreFactory} and
   * {@link QuerySplitter}
   *
   * <p>{@link DatastoreFactory} and {@link QuerySplitter} are not java serializable, hence wrapping
   * them under this class, which implements {@link Serializable}.
   */
  static class V1Beta3DatastoreFactory implements Serializable {

    /** Builds a datastore client for the given {@param pipelineOptions} and {@param projectId}. */
    public Datastore getDatastore(PipelineOptions pipelineOptions, String projectId) {
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

      return DatastoreFactory.get().create(builder.build());
    }

    /** Builds a Datastore {@link QuerySplitter}. */
    public QuerySplitter getQuerySplitter() {
      return DatastoreHelper.getQuerySplitter();
    }
  }
}
