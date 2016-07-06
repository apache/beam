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

package org.apache.beam.sdk.io.datastore;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Verify.verify;
import static com.google.datastore.v1beta3.PropertyFilter.Operator.EQUAL;
import static com.google.datastore.v1beta3.PropertyOrder.Direction.DESCENDING;
import static com.google.datastore.v1beta3.QueryResultBatch.MoreResultsType.NOT_FINISHED;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeAndFilter;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeFilter;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeOrder;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeUpsert;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeValue;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Sink.WriteOperation;
import org.apache.beam.sdk.io.Sink.Writer;
import org.apache.beam.sdk.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.AttemptBoundedExponentialBackOff;
import org.apache.beam.sdk.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.datastore.v1beta3.CommitRequest;
import com.google.datastore.v1beta3.Entity;
import com.google.datastore.v1beta3.EntityResult;
import com.google.datastore.v1beta3.Key;
import com.google.datastore.v1beta3.Key.PathElement;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * <p>{@link V1Beta3} provides an API to Read and Write {@link PCollection PCollections} of
 * <a href="https://developers.google.com/datastore/">Google Cloud Datastore</a> version v1beta3
 * {@link Entity} objects.
 *
 * <p>This API currently requires an authentication workaround. To use {@link V1Beta3}, users
 * must use the {@code gcloud} command line tool to get credentials for Datastore:
 * <pre>
 * $ gcloud auth login
 * </pre>
 *
 * <p>To read a {@link PCollection} from a query to Datastore, use {@link V1Beta3#read} and
 * its methods {@link V1Beta3.Read#withProjectId} and {@link V1Beta3.Read#withQuery} to
 * specify the project to query and the query to read from. You can optionally provide a namespace
 * to query within using {@link V1Beta3.Read#withNamespace}.
 *
 * <p>For example:
 *
 * <pre> {@code
 * // Read a query from Datastore
 * PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
 * Query query = ...;
 * String projectId = "...";
 *
 * Pipeline p = Pipeline.create(options);
 * PCollection<Entity> entities = p.apply(
 *     DatastoreIO.v1beta3().read()
 *         .withProjectId(projectId)
 *         .withQuery(query));
 * } </pre>
 *
 * <p><b>Note:</b> Normally, a Cloud Dataflow job will read from Cloud Datastore in parallel across
 * many workers. However, when the {@link Query} is configured with a limit using
 * {@link com.google.datastore.v1beta3.Query.Builder#setLimit(Int32Value)}, then
 * all returned results will be read by a single Dataflow worker in order to ensure correct data.
 *
 * <p>To write a {@link PCollection} to a Datastore, use {@link V1Beta3#write},
 * specifying the Cloud Datastore project to write to:
 *
 * <pre> {@code
 * PCollection<Entity> entities = ...;
 * entities.apply(DatastoreIO.v1beta3().write().withProjectId(projectId));
 * p.run();
 * } </pre>
 *
 * <p>{@link Entity Entities} in the {@code PCollection} to be written must have complete
 * {@link Key Keys}. Complete {@code Keys} specify the {@code name} and {@code id} of the
 * {@code Entity}, where incomplete {@code Keys} do not. A {@code namespace} other than
 * {@code projectId} default may be used by specifying it in the {@code Entity} {@code Keys}.
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
 * @see org.apache.beam.sdk.runners.PipelineRunner
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class V1Beta3 {

  // A package-private constructor to prevent direct instantiation from outside of this package
  V1Beta3() {}

  /**
   * Datastore has a limit of 500 mutations per batch operation, so we flush
   * changes to Datastore every 500 entities.
   */
  private static final int DATASTORE_BATCH_UPDATE_LIMIT = 500;

  /**
   * Returns an empty {@link V1Beta3.Read} builder. Configure the source {@code projectId},
   * {@code query}, and optionally {@code namespace} using {@link V1Beta3.Read#withProjectId},
   * {@link V1Beta3.Read#withQuery}, and {@link V1Beta3.Read#withNamespace}.
   */
  public V1Beta3.Read read() {
    return new V1Beta3.Read(null, null, null);
  }

  /**
   * A {@link PTransform} that reads the result rows of a Datastore query as {@code Entity}
   * objects.
   *
   * @see DatastoreIO
   */
  public static class Read extends PTransform<PBegin, PCollection<Entity>> {
    @Nullable
    private final String projectId;

    @Nullable
    private final Query query;

    @Nullable
    private final String namespace;

    /**
     * Note that only {@code namespace} is really {@code @Nullable}. The other parameters may be
     * {@code null} as a matter of build order, but if they are {@code null} at instantiation time,
     * an error will be thrown.
     */
    private Read(@Nullable String projectId, @Nullable Query query, @Nullable String namespace) {
      this.projectId = projectId;
      this.query = query;
      this.namespace = namespace;
    }

    /**
     * Returns a new {@link V1Beta3.Read} that reads from the Datastore for the specified project.
     */
    public V1Beta3.Read withProjectId(String projectId) {
      checkNotNull(projectId, "projectId");
      return new V1Beta3.Read(projectId, query, namespace);
    }

    /**
     * Returns a new {@link V1Beta3.Read} that reads the results of the specified query.
     *
     * <p><b>Note:</b> Normally, {@code DatastoreIO} will read from Cloud Datastore in parallel
     * across many workers. However, when the {@link Query} is configured with a limit using
     * {@link Query.Builder#setLimit}, then all results will be read by a single worker in order
     * to ensure correct results.
     */
    public V1Beta3.Read withQuery(Query query) {
      checkNotNull(query, "query");
      checkArgument(!query.hasLimit() || query.getLimit().getValue() > 0,
          "Invalid query limit %s: must be positive", query.getLimit().getValue());
      return new V1Beta3.Read(projectId, query, namespace);
    }

    /**
     * Returns a new {@link V1Beta3.Read} that reads from the given namespace.
     */
    public V1Beta3.Read withNamespace(String namespace) {
      return new V1Beta3.Read(projectId, query, namespace);
    }

    @Nullable
    public Query getQuery() {
      return query;
    }

    @Nullable
    public String getProjectId() {
      return projectId;
    }

    @Nullable
    public String getNamespace() {
      return namespace;
    }

    @Override
    public PCollection<Entity> apply(PBegin input) {
      return input.apply(org.apache.beam.sdk.io.Read.from(getSource()));
    }

    @Override
    public void validate(PBegin input) {
      checkNotNull(projectId, "projectId");
      checkNotNull(query, "query");
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
    DatastoreSource getSource() {
      return new DatastoreSource(projectId, query, namespace);
    }
  }

  /**
   * Returns an empty {@link V1Beta3.Write} builder. Configure the destination
   * {@code projectId} using {@link V1Beta3.Write#withProjectId}.
   */
  public Write write() {
    return new Write(null);
  }

  /**
   * A {@link PTransform} that writes {@link Entity} objects to Cloud Datastore.
   *
   * @see DatastoreIO
   */
  public static class Write extends PTransform<PCollection<Entity>, PDone> {
    @Nullable
    private final String projectId;

    /**
     * Note that {@code projectId} is only {@code @Nullable} as a matter of build order, but if
     * it is {@code null} at instantiation time, an error will be thrown.
     */
    public Write(@Nullable String projectId) {
      this.projectId = projectId;
    }

    /**
     * Returns a new {@link Write} that writes to the Cloud Datastore for the specified project.
     */
    public Write withProjectId(String projectId) {
      checkNotNull(projectId, "projectId");
      return new Write(projectId);
    }

    @Override
    public PDone apply(PCollection<Entity> input) {
      return input.apply(
          org.apache.beam.sdk.io.Write.to(new DatastoreSink(projectId)));
    }

    @Override
    public void validate(PCollection<Entity> input) {
      checkNotNull(projectId, "projectId");
    }

    @Nullable
    public String getProjectId() {
      return projectId;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("projectId", projectId)
          .toString();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("projectId", projectId)
              .withLabel("Output Project"));
    }
  }

  /**
   * A {@link org.apache.beam.sdk.io.Source} that reads data from Datastore.
   */
  static class DatastoreSource extends BoundedSource<Entity> {

    @Override
    public Coder<Entity> getDefaultOutputCoder() {
      return ProtoCoder.of(Entity.class);
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) {
      return false;
    }

    @Override
    public List<DatastoreSource> splitIntoBundles(long desiredBundleSizeBytes,
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
        datastoreSplits = getSplitQueries(Ints.checkedCast(numSplits), options);
      } catch (IllegalArgumentException | DatastoreException e) {
        LOG.warn("Unable to parallelize the given query: {}", query, e);
        return ImmutableList.of(this);
      }

      ImmutableList.Builder<DatastoreSource> splits = ImmutableList.builder();
      for (Query splitQuery : datastoreSplits) {
        splits.add(new DatastoreSource(projectId, splitQuery, namespace));
      }
      return splits.build();
    }

    @Override
    public BoundedReader<Entity> createReader(PipelineOptions pipelineOptions) throws IOException {
      return new DatastoreReader(this, getDatastore(pipelineOptions));
    }

    @Override
    public void validate() {
      checkNotNull(query, "query");
      checkNotNull(projectId, "projectId");
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

    private static final Logger LOG = LoggerFactory.getLogger(DatastoreSource.class);
    private final String projectId;
    private final Query query;
    @Nullable
    private final String namespace;

    /** For testing only. TODO: This could be much cleaner with dependency injection. */
    @Nullable
    private QuerySplitter mockSplitter;
    @Nullable
    private Long mockEstimateSizeBytes;

    DatastoreSource(String projectId, Query query, @Nullable String namespace) {
      this.projectId = projectId;
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
        partitionBuilder.setNamespaceId(namespace);
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
     * {@code DatastoreSource}. For example, sets the {@code namespace} for the request.
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
      return DatastoreFactory.get().create(builder.build());
    }

    /** For testing only. */
    DatastoreSource withMockSplitter(QuerySplitter splitter) {
      DatastoreSource res = new DatastoreSource(projectId, query, namespace);
      res.mockSplitter = splitter;
      res.mockEstimateSizeBytes = mockEstimateSizeBytes;
      return res;
    }

    /** For testing only. */
    DatastoreSource withMockEstimateSizeBytes(Long estimateSizeBytes) {
      DatastoreSource res = new DatastoreSource(projectId, query, namespace);
      res.mockSplitter = mockSplitter;
      res.mockEstimateSizeBytes = estimateSizeBytes;
      return res;
    }

    @VisibleForTesting
    Query getQuery() {
      return query;
    }
  }

  /**
   * A {@link DatastoreSource.Reader} over the records from a query of the datastore.
   *
   * <p>Timestamped records are currently not supported.
   * All records implicitly have the timestamp of {@code BoundedWindow.TIMESTAMP_MIN_VALUE}.
   */
  @VisibleForTesting
  static class DatastoreReader extends BoundedSource.BoundedReader<Entity> {
    private final DatastoreSource source;

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
     * Returns a DatastoreReader with DatastoreSource and Datastore object set.
     *
     * @param datastore a datastore connection to use.
     */
    public DatastoreReader(DatastoreSource source, Datastore datastore) {
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
    public DatastoreSource getCurrentSource() {
      return source;
    }

    @Override
    public DatastoreSource splitAtFraction(double fraction) {
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

  /**
   * A {@link org.apache.beam.sdk.io.Sink} that writes data to Datastore.
   */
  static class DatastoreSink extends org.apache.beam.sdk.io.Sink<Entity> {
    final String projectId;

    public DatastoreSink(String projectId) {
      this.projectId = projectId;
    }

    @Override
    public void validate(PipelineOptions options) {
      checkNotNull(projectId, "projectId");
    }

    @Override
    public DatastoreWriteOperation createWriteOperation(PipelineOptions options) {
      return new DatastoreWriteOperation(this);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("projectId", projectId)
              .withLabel("Output Project"));
    }
  }

  /**
   * A {@link WriteOperation} that will manage a parallel write to a Datastore sink.
   */
  private static class DatastoreWriteOperation
      extends WriteOperation<Entity, DatastoreWriteResult> {
    private static final Logger LOG = LoggerFactory.getLogger(DatastoreWriteOperation.class);

    private final DatastoreSink sink;

    public DatastoreWriteOperation(DatastoreSink sink) {
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
              .projectId(sink.projectId)
              .initializer(new RetryHttpRequestInitializer());
      Credential credential = options.as(GcpOptions.class).getGcpCredential();
      if (credential != null) {
        builder.credential(credential);
      }
      Datastore datastore = DatastoreFactory.get().create(builder.build());

      return new DatastoreWriter(this, datastore);
    }

    @Override
    public DatastoreSink getSink() {
      return sink;
    }
  }

  /**
   * {@link Writer} that writes entities to a Datastore Sink.  Entities are written in batches,
   * where the maximum batch size is {@link V1Beta3#DATASTORE_BATCH_UPDATE_LIMIT}.  Entities
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
   * group, the commit will be retried (up to {@link V1Beta3#DATASTORE_BATCH_UPDATE_LIMIT}
   * times).
   *
   * <p>Visible for testing purposes.
   */
  @VisibleForTesting
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
      List<PathElement> elementList = key.getPathList();
      if (elementList.isEmpty()) {
        return false;
      }
      PathElement lastElement = elementList.get(elementList.size() - 1);
      return (lastElement.getId() != 0 || !lastElement.getName().isEmpty());
    }

    DatastoreWriter(DatastoreWriteOperation writeOp, Datastore datastore) {
      this.writeOp = writeOp;
      this.datastore = datastore;
    }

    @Override
    public void open(String uId) throws Exception {}

    /**
     * Writes an entity to the Datastore.  Writes are batched, up to {@link
     * V1Beta3#DATASTORE_BATCH_UPDATE_LIMIT}. If an entity does not have a complete key, an
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

      if (entities.size() >= V1Beta3.DATASTORE_BATCH_UPDATE_LIMIT) {
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
          for (Entity entity: entities) {
            commitRequest.addMutations(makeUpsert(entity));
          }
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
}
