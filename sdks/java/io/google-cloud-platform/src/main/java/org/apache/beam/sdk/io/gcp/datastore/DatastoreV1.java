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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Verify.verify;
import static com.google.datastore.v1.PropertyFilter.Operator.EQUAL;
import static com.google.datastore.v1.PropertyOrder.Direction.DESCENDING;
import static com.google.datastore.v1.QueryResultBatch.MoreResultsType.NOT_FINISHED;
import static com.google.datastore.v1.client.DatastoreHelper.makeAndFilter;
import static com.google.datastore.v1.client.DatastoreHelper.makeDelete;
import static com.google.datastore.v1.client.DatastoreHelper.makeFilter;
import static com.google.datastore.v1.client.DatastoreHelper.makeOrder;
import static com.google.datastore.v1.client.DatastoreHelper.makeUpsert;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auto.value.AutoValue;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.EntityResult;
import com.google.datastore.v1.GqlQuery;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Key.PathElement;
import com.google.datastore.v1.Mutation;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.RunQueryResponse;
import com.google.datastore.v1.client.Datastore;
import com.google.datastore.v1.client.DatastoreException;
import com.google.datastore.v1.client.DatastoreFactory;
import com.google.datastore.v1.client.DatastoreHelper;
import com.google.datastore.v1.client.DatastoreOptions;
import com.google.datastore.v1.client.QuerySplitter;
import com.google.protobuf.Int32Value;
import com.google.rpc.Code;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link DatastoreV1} provides an API to Read, Write and Delete {@link PCollection PCollections}
 * of <a href="https://developers.google.com/datastore/">Google Cloud Datastore</a> version v1
 * {@link Entity} objects. Read is only supported for Bounded PCollections while Write and Delete
 * are supported for both Bounded and Unbounded PCollections.
 *
 * <p>This API currently requires an authentication workaround. To use {@link DatastoreV1}, users
 * must use the {@code gcloud} command line tool to get credentials for Cloud Datastore:
 * <pre>
 * $ gcloud auth login
 * </pre>
 *
 * <p>To read a {@link PCollection} from a query to Cloud Datastore, use {@link DatastoreV1#read}
 * and its methods {@link DatastoreV1.Read#withProjectId} and {@link DatastoreV1.Read#withQuery} to
 * specify the project to query and the query to read from. You can optionally provide a namespace
 * to query within using {@link DatastoreV1.Read#withNamespace}. You could also optionally specify
 * how many splits you want for the query using {@link DatastoreV1.Read#withNumQuerySplits}.
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
 *     DatastoreIO.v1().read()
 *         .withProjectId(projectId)
 *         .withQuery(query));
 * } </pre>
 *
 * <p><b>Note:</b> A runner may read from Cloud Datastore in parallel across
 * many workers. However, when the {@link Query} is configured with a limit using
 * {@link com.google.datastore.v1.Query.Builder#setLimit(Int32Value)} or if the Query contains
 * inequality filters like {@code GREATER_THAN, LESS_THAN} etc., then all returned results
 * will be read by a single worker in order to ensure correct data. Since data is read from
 * a single worker, this could have a significant impact on the performance of the job.
 *
 * <p>To write a {@link PCollection} to a Cloud Datastore, use {@link DatastoreV1#write},
 * specifying the Cloud Datastore project to write to:
 *
 * <pre> {@code
 * PCollection<Entity> entities = ...;
 * entities.apply(DatastoreIO.v1().write().withProjectId(projectId));
 * p.run();
 * } </pre>
 *
 * <p>To delete a {@link PCollection} of {@link Entity Entities} from Cloud Datastore, use
 * {@link DatastoreV1#deleteEntity()}, specifying the Cloud Datastore project to write to:
 *
 * <pre> {@code
 * PCollection<Entity> entities = ...;
 * entities.apply(DatastoreIO.v1().deleteEntity().withProjectId(projectId));
 * p.run();
 * } </pre>
 *
 * <p>To delete entities associated with a {@link PCollection} of {@link Key Keys} from Cloud
 * Datastore, use {@link DatastoreV1#deleteKey}, specifying the Cloud Datastore project to write to:
 *
 * <pre> {@code
 * PCollection<Entity> entities = ...;
 * entities.apply(DatastoreIO.v1().deleteKey().withProjectId(projectId));
 * p.run();
 * } </pre>
 *
 * <p>{@link Entity Entities} in the {@code PCollection} to be written or deleted must have complete
 * {@link Key Keys}. Complete {@code Keys} specify the {@code name} and {@code id} of the
 * {@code Entity}, where incomplete {@code Keys} do not. A {@code namespace} other than
 * {@code projectId} default may be used by specifying it in the {@code Entity} {@code Keys}.
 *
 * <pre>{@code
 * Key.Builder keyBuilder = DatastoreHelper.makeKey(...);
 * keyBuilder.getPartitionIdBuilder().setNamespace(namespace);
 * }</pre>
 *
 * <p>{@code Entities} will be committed as upsert (update or insert) or delete mutations. Please
 * read <a href="https://cloud.google.com/datastore/docs/concepts/entities">Entities, Properties,
 * and Keys</a> for more information about {@code Entity} keys.
 *
 * <h3>Permissions</h3>
 * Permission requirements depend on the {@code PipelineRunner} that is used to execute the
 * pipeline. Please refer to the documentation of corresponding {@code PipelineRunner}s for
 * more details.
 *
 * <p>Please see <a href="https://cloud.google.com/datastore/docs/activate">Cloud Datastore Sign Up
 * </a>for security and permission related information specific to Cloud Datastore.
 *
 * <p>Optionally, Cloud Datastore V1 Emulator, running locally, could be used for testing purposes
 * by providing the host port information through {@code withLocalhost("host:port"} for all the
 * above transforms. In such a case, all the Cloud Datastore API calls are directed to the Emulator.
 *
 * @see org.apache.beam.sdk.runners.PipelineRunner
 */
public class DatastoreV1 {

  // A package-private constructor to prevent direct instantiation from outside of this package
  DatastoreV1() {}

  /**
   * Cloud Datastore has a limit of 500 mutations per batch operation, so we flush
   * changes to Datastore every 500 entities.
   */
  @VisibleForTesting
  static final int DATASTORE_BATCH_UPDATE_LIMIT = 500;

  /**
   * Returns an empty {@link DatastoreV1.Read} builder. Configure the source {@code projectId},
   * {@code query}, and optionally {@code namespace} and {@code numQuerySplits} using
   * {@link DatastoreV1.Read#withProjectId}, {@link DatastoreV1.Read#withQuery},
   * {@link DatastoreV1.Read#withNamespace}, {@link DatastoreV1.Read#withNumQuerySplits}.
   */
  public DatastoreV1.Read read() {
    return new AutoValue_DatastoreV1_Read.Builder().setNumQuerySplits(0).build();
  }

  /**
   * A {@link PTransform} that reads the result rows of a Cloud Datastore query as {@code Entity}
   * objects.
   *
   * @see DatastoreIO
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<Entity>> {
    private static final Logger LOG = LoggerFactory.getLogger(Read.class);

    /** An upper bound on the number of splits for a query. */
    public static final int NUM_QUERY_SPLITS_MAX = 50000;

    /** A lower bound on the number of splits for a query. */
    static final int NUM_QUERY_SPLITS_MIN = 12;

    /** Default bundle size of 64MB. */
    static final long DEFAULT_BUNDLE_SIZE_BYTES = 64 * 1024 * 1024;

    /**
     * Maximum number of results to request per query.
     *
     * <p>Must be set, or it may result in an I/O error when querying Cloud Datastore.
     */
    static final int QUERY_BATCH_LIMIT = 500;

    @Nullable public abstract ValueProvider<String> getProjectId();
    @Nullable public abstract Query getQuery();
    @Nullable public abstract ValueProvider<String> getLiteralGqlQuery();
    @Nullable public abstract ValueProvider<String> getNamespace();
    public abstract int getNumQuerySplits();
    @Nullable public abstract String getLocalhost();

    @Override
    public abstract String toString();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setProjectId(ValueProvider<String> projectId);
      abstract Builder setQuery(Query query);
      abstract Builder setLiteralGqlQuery(ValueProvider<String> literalGqlQuery);
      abstract Builder setNamespace(ValueProvider<String> namespace);
      abstract Builder setNumQuerySplits(int numQuerySplits);
      abstract Builder setLocalhost(String localhost);
      abstract Read build();
    }

    /**
     * Computes the number of splits to be performed on the given query by querying the estimated
     * size from Cloud Datastore.
     */
    static int getEstimatedNumSplits(Datastore datastore, Query query, @Nullable String namespace) {
      int numSplits;
      try {
        long estimatedSizeBytes = getEstimatedSizeBytes(datastore, query, namespace);
        LOG.info("Estimated size bytes for the query is: {}", estimatedSizeBytes);
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
     * Cloud Datastore system tables with statistics are periodically updated. This method fetches
     * the latest timestamp (in microseconds) of statistics update using the {@code __Stat_Total__}
     * table.
     */
    private static long queryLatestStatisticsTimestamp(Datastore datastore,
        @Nullable String namespace)  throws DatastoreException {
      Query.Builder query = Query.newBuilder();
      // Note: namespace either being null or empty represents the default namespace, in which
      // case we treat it as not provided by the user.
      if (Strings.isNullOrEmpty(namespace)) {
        query.addKindBuilder().setName("__Stat_Total__");
      } else {
        query.addKindBuilder().setName("__Stat_Ns_Total__");
      }
      query.addOrder(makeOrder("timestamp", DESCENDING));
      query.setLimit(Int32Value.newBuilder().setValue(1));
      RunQueryRequest request = makeRequest(query.build(), namespace);

      RunQueryResponse response = datastore.runQuery(request);
      QueryResultBatch batch = response.getBatch();
      if (batch.getEntityResultsCount() == 0) {
        throw new NoSuchElementException(
            "Datastore total statistics unavailable");
      }
      Entity entity = batch.getEntityResults(0).getEntity();
      return entity.getProperties().get("timestamp").getTimestampValue().getSeconds() * 1000000;
    }

    /**
     * Get the estimated size of the data returned by the given query.
     *
     * <p>Cloud Datastore provides no way to get a good estimate of how large the result of a query
     * entity kind being queried, using the __Stat_Kind__ system table, assuming exactly 1 kind
     * is specified in the query.
     *
     * <p>See https://cloud.google.com/datastore/docs/concepts/stats.
     */
    static long getEstimatedSizeBytes(Datastore datastore, Query query, @Nullable String namespace)
        throws DatastoreException {
      String ourKind = query.getKind(0).getName();
      long latestTimestamp = queryLatestStatisticsTimestamp(datastore, namespace);
      LOG.info("Latest stats timestamp for kind {} is {}", ourKind, latestTimestamp);

      Query.Builder queryBuilder = Query.newBuilder();
      if (Strings.isNullOrEmpty(namespace)) {
        queryBuilder.addKindBuilder().setName("__Stat_Kind__");
      } else {
        queryBuilder.addKindBuilder().setName("__Stat_Ns_Kind__");
      }

      queryBuilder.setFilter(makeAndFilter(
          makeFilter("kind_name", EQUAL, makeValue(ourKind).build()).build(),
          makeFilter("timestamp", EQUAL, makeValue(latestTimestamp).build()).build()));

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

    private static PartitionId.Builder forNamespace(@Nullable String namespace) {
      PartitionId.Builder partitionBuilder = PartitionId.newBuilder();
      // Namespace either being null or empty represents the default namespace.
      // Datastore Client libraries expect users to not set the namespace proto field in
      // either of these cases.
      if (!Strings.isNullOrEmpty(namespace)) {
        partitionBuilder.setNamespaceId(namespace);
      }
      return partitionBuilder;
    }

    /** Builds a {@link RunQueryRequest} from the {@code query} and {@code namespace}. */
    static RunQueryRequest makeRequest(Query query, @Nullable String namespace) {
      return RunQueryRequest.newBuilder()
          .setQuery(query)
          .setPartitionId(forNamespace(namespace)).build();
    }

    @VisibleForTesting
    /** Builds a {@link RunQueryRequest} from the {@code GqlQuery} and {@code namespace}. */
    static RunQueryRequest makeRequest(GqlQuery gqlQuery, @Nullable String namespace) {
      return RunQueryRequest.newBuilder()
          .setGqlQuery(gqlQuery)
          .setPartitionId(forNamespace(namespace)).build();
    }

    /**
     * A helper function to get the split queries, taking into account the optional
     * {@code namespace}.
     */
    private static List<Query> splitQuery(Query query, @Nullable String namespace,
        Datastore datastore, QuerySplitter querySplitter, int numSplits) throws DatastoreException {
      // If namespace is set, include it in the split request so splits are calculated accordingly.
      return querySplitter.getSplits(query, forNamespace(namespace).build(), numSplits, datastore);
    }

    /**
     * Translates a Cloud Datastore gql query string to {@link Query}.
     *
     * <p>Currently, the only way to translate a gql query string to a Query is to run the query
     * against Cloud Datastore and extract the {@code Query} from the response. To prevent reading
     * any data, we set the {@code LIMIT} to 0 but if the gql query already has a limit set, we
     * catch the exception with {@code INVALID_ARGUMENT} error code and retry the translation
     * without the zero limit.
     *
     * <p>Note: This may result in reading actual data from Cloud Datastore but the service has a
     * cap on the number of entities returned for a single rpc request, so this should not be a
     * problem in practice.
     */
    @VisibleForTesting
    static Query translateGqlQueryWithLimitCheck(String gql, Datastore datastore,
        String namespace) throws DatastoreException {
      String gqlQueryWithZeroLimit = gql + " LIMIT 0";
      try {
        Query translatedQuery = translateGqlQuery(gqlQueryWithZeroLimit, datastore, namespace);
        // Clear the limit that we set.
        return translatedQuery.toBuilder().clearLimit().build();
      } catch (DatastoreException e) {
        // Note: There is no specific error code or message to detect if the query already has a
        // limit, so we just check for INVALID_ARGUMENT and assume that that the query might have
        // a limit already set.
        if (e.getCode() == Code.INVALID_ARGUMENT) {
          LOG.warn("Failed to translate Gql query '{}': {}", gqlQueryWithZeroLimit, e.getMessage());
          LOG.warn("User query might have a limit already set, so trying without zero limit");
          // Retry without the zero limit.
          return translateGqlQuery(gql, datastore, namespace);
        } else {
          throw e;
        }
      }
    }

    /** Translates a gql query string to {@link Query}.*/
    private static Query translateGqlQuery(String gql, Datastore datastore, String namespace)
        throws DatastoreException {
      GqlQuery gqlQuery = GqlQuery.newBuilder().setQueryString(gql)
          .setAllowLiterals(true).build();
      RunQueryRequest req = makeRequest(gqlQuery, namespace);
      return datastore.runQuery(req).getQuery();
    }

    /**
     * Returns a new {@link DatastoreV1.Read} that reads from the Cloud Datastore for the specified
     * project.
     */
    public DatastoreV1.Read withProjectId(String projectId) {
      checkNotNull(projectId, "projectId");
      return toBuilder().setProjectId(StaticValueProvider.of(projectId)).build();
    }

    /**
     * Same as {@link Read#withProjectId(String)} but with a {@link ValueProvider}.
     */
    public DatastoreV1.Read withProjectId(ValueProvider<String> projectId) {
      checkNotNull(projectId, "projectId");
      return toBuilder().setProjectId(projectId).build();
    }

    /**
     * Returns a new {@link DatastoreV1.Read} that reads the results of the specified query.
     *
     * <p><b>Note:</b> Normally, {@code DatastoreIO} will read from Cloud Datastore in parallel
     * across many workers. However, when the {@link Query} is configured with a limit using
     * {@link Query.Builder#setLimit}, then all results will be read by a single worker in order
     * to ensure correct results.
     */
    public DatastoreV1.Read withQuery(Query query) {
      checkNotNull(query, "query");
      checkArgument(!query.hasLimit() || query.getLimit().getValue() > 0,
          "Invalid query limit %s: must be positive", query.getLimit().getValue());
      return toBuilder().setQuery(query).build();
    }

    /**
     * Returns a new {@link DatastoreV1.Read} that reads the results of the specified GQL query.
     * See <a href="https://cloud.google.com/datastore/docs/reference/gql_reference">GQL Reference
     * </a> to know more about GQL grammar.
     *
     * <p><b><i>Note:</i></b> This query is executed with literals allowed, so the users should
     * ensure that the query is originated from trusted sources to avoid any security
     * vulnerabilities via SQL Injection.
     *
     * <p><b><i>Experimental</i></b>: Cloud Datastore does not a provide a clean way to translate
     * a gql query string to {@link Query}, so we end up making a query to the service for
     * translation but this may read the actual data, although it will be a small amount.
     * It needs more validation through production use cases before marking it as stable.
     */
    @Experimental(Kind.SOURCE_SINK)
    public DatastoreV1.Read withLiteralGqlQuery(String gqlQuery) {
      checkNotNull(gqlQuery, "gqlQuery");
      return toBuilder().setLiteralGqlQuery(StaticValueProvider.of(gqlQuery)).build();
    }

    /**
     * Same as {@link Read#withLiteralGqlQuery(String)} but with a {@link ValueProvider}.
     */
    @Experimental(Kind.SOURCE_SINK)
    public DatastoreV1.Read withLiteralGqlQuery(ValueProvider<String> gqlQuery) {
      checkNotNull(gqlQuery, "gqlQuery");
      return toBuilder().setLiteralGqlQuery(gqlQuery).build();
    }

    /**
     * Returns a new {@link DatastoreV1.Read} that reads from the given namespace.
     */
    public DatastoreV1.Read withNamespace(String namespace) {
      return toBuilder().setNamespace(StaticValueProvider.of(namespace)).build();
    }

    /**
     * Same as {@link Read#withNamespace(String)} but with a {@link ValueProvider}.
     */
    public DatastoreV1.Read withNamespace(ValueProvider<String> namespace) {
      return toBuilder().setNamespace(namespace).build();
    }

    /**
     * Returns a new {@link DatastoreV1.Read} that reads by splitting the given {@code query} into
     * {@code numQuerySplits}.
     *
     * <p>The semantics for the query splitting is defined below:
     * <ul>
     *   <li>Any value less than or equal to 0 will be ignored, and the number of splits will be
     *   chosen dynamically at runtime based on the query data size.
     *   <li>Any value greater than {@link Read#NUM_QUERY_SPLITS_MAX} will be capped at
     *   {@code NUM_QUERY_SPLITS_MAX}.
     *   <li>If the {@code query} has a user limit set, then {@code numQuerySplits} will be
     *   ignored and no split will be performed.
     *   <li>Under certain cases Cloud Datastore is unable to split query to the requested number of
     *   splits. In such cases we just use whatever the Cloud Datastore returns.
     * </ul>
     */
    public DatastoreV1.Read withNumQuerySplits(int numQuerySplits) {
      return toBuilder()
          .setNumQuerySplits(Math.min(Math.max(numQuerySplits, 0), NUM_QUERY_SPLITS_MAX))
          .build();
    }

    /**
     * Returns a new {@link DatastoreV1.Read} that reads from a Datastore Emulator running at the
     * given localhost address.
     */
    public DatastoreV1.Read withLocalhost(String localhost) {
      return toBuilder()
          .setLocalhost(localhost)
          .build();
    }

    @Override
    public PCollection<Entity> expand(PBegin input) {
      V1Options v1Options = V1Options.from(getProjectId(), getNamespace(), getLocalhost());

      /*
       * This composite transform involves the following steps:
       *   1. Create a singleton of the user provided {@code query} or if {@code gqlQuery} is
       *   provided apply a {@link ParDo} that translates the {@code gqlQuery} into a {@code query}.
       *
       *   2. A {@link ParDo} splits the resulting query into {@code numQuerySplits} and
       *   assign each split query a unique {@code Integer} as the key. The resulting output is
       *   of the type {@code PCollection<KV<Integer, Query>>}.
       *
       *   If the value of {@code numQuerySplits} is less than or equal to 0, then the number of
       *   splits will be computed dynamically based on the size of the data for the {@code query}.
       *
       *   3. The resulting {@code PCollection} is sharded using a {@link GroupByKey} operation. The
       *   queries are extracted from they {@code KV<Integer, Iterable<Query>>} and flattened to
       *   output a {@code PCollection<Query>}.
       *
       *   4. In the third step, a {@code ParDo} reads entities for each query and outputs
       *   a {@code PCollection<Entity>}.
       */

      PCollection<Query> inputQuery;
      if (getQuery() != null) {
        inputQuery = input.apply(Create.of(getQuery()));
      } else {
        inputQuery = input
            .apply(Create.of(getLiteralGqlQuery())
                .withCoder(SerializableCoder.of(new TypeDescriptor<ValueProvider<String>>() {})))
            .apply(ParDo.of(new GqlQueryTranslateFn(v1Options)));
      }

      PCollection<KV<Integer, Query>> splitQueries = inputQuery
          .apply(ParDo.of(new SplitQueryFn(v1Options, getNumQuerySplits())));

      PCollection<Query> shardedQueries = splitQueries
          .apply(GroupByKey.<Integer, Query>create())
          .apply(Values.<Iterable<Query>>create())
          .apply(Flatten.<Query>iterables());

      PCollection<Entity> entities = shardedQueries
          .apply(ParDo.of(new ReadFn(v1Options)));

      return entities;
    }

    @Override
    public void validate(PBegin input) {
      checkNotNull(getProjectId(), "projectId");

      if (getProjectId().isAccessible() && getProjectId().get() == null) {
        throw new IllegalArgumentException("Project id cannot be null");
      }

      if (getQuery() == null && getLiteralGqlQuery() == null) {
        throw new IllegalArgumentException(
            "Either query or gql query ValueProvider should be provided");
      }

      if (getQuery() != null && getLiteralGqlQuery() != null) {
        throw new IllegalArgumentException(
            "Only one of query or gql query ValueProvider should be provided");
      }

      if (getLiteralGqlQuery() != null && getLiteralGqlQuery().isAccessible()) {
        checkNotNull(getLiteralGqlQuery().get(), "gqlQuery");
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      String query = getQuery() == null ? null : getQuery().toString();
      builder
          .addIfNotNull(DisplayData.item("projectId", getProjectId())
              .withLabel("ProjectId"))
          .addIfNotNull(DisplayData.item("namespace", getNamespace())
              .withLabel("Namespace"))
          .addIfNotNull(DisplayData.item("query", query)
              .withLabel("Query"))
          .addIfNotNull(DisplayData.item("gqlQuery", getLiteralGqlQuery())
              .withLabel("GqlQuery"));
    }

    @VisibleForTesting
    static class V1Options implements HasDisplayData, Serializable {
      private final ValueProvider<String> project;
      @Nullable
      private final ValueProvider<String> namespace;
      @Nullable
      private final String localhost;

      private V1Options(ValueProvider<String> project, ValueProvider<String> namespace,
          String localhost) {
        this.project = project;
        this.namespace = namespace;
        this.localhost = localhost;
      }

      public static V1Options from(String projectId, String namespace, String localhost) {
        return from(StaticValueProvider.of(projectId), StaticValueProvider.of(namespace),
            localhost);
      }

      public static V1Options from(ValueProvider<String> project, ValueProvider<String> namespace,
          String localhost) {
        return new V1Options(project, namespace, localhost);
      }

      public String getProjectId() {
        return project.get();
      }

      @Nullable
      public String getNamespace() {
        return namespace == null ? null : namespace.get();
      }

      public ValueProvider<String> getProjectValueProvider() {
        return project;
      }

      @Nullable
      public ValueProvider<String> getNamespaceValueProvider() {
        return namespace;
      }

      @Nullable
      public String getLocalhost() {
        return localhost;
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder
            .addIfNotNull(DisplayData.item("projectId", getProjectValueProvider())
                .withLabel("ProjectId"))
            .addIfNotNull(DisplayData.item("namespace", getNamespaceValueProvider())
                .withLabel("Namespace"));
      }
    }

    /**
     * A DoFn that translates a Cloud Datastore gql query string to {@code Query}.
     */
    static class GqlQueryTranslateFn extends DoFn<ValueProvider<String>, Query> {
      private final V1Options v1Options;
      private transient Datastore datastore;
      private final V1DatastoreFactory datastoreFactory;

      GqlQueryTranslateFn(V1Options options) {
        this(options, new V1DatastoreFactory());
      }

      GqlQueryTranslateFn(V1Options options, V1DatastoreFactory datastoreFactory) {
        this.v1Options = options;
        this.datastoreFactory = datastoreFactory;
      }

      @StartBundle
      public void startBundle(Context c) throws Exception {
        datastore = datastoreFactory.getDatastore(c.getPipelineOptions(), v1Options.getProjectId());
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        ValueProvider<String> gqlQuery = c.element();
        LOG.info("User query: '{}'", gqlQuery.get());
        Query query = translateGqlQueryWithLimitCheck(gqlQuery.get(), datastore,
            v1Options.getNamespace());
        LOG.info("User gql query translated to Query({})", query);
        c.output(query);
      }
    }

    /**
     * A {@link DoFn} that splits a given query into multiple sub-queries, assigns them unique
     * keys and outputs them as {@link KV}.
     */
    @VisibleForTesting
    static class SplitQueryFn extends DoFn<Query, KV<Integer, Query>> {
      private final V1Options options;
      // number of splits to make for a given query
      private final int numSplits;

      private final V1DatastoreFactory datastoreFactory;
      // Datastore client
      private transient Datastore datastore;
      // Query splitter
      private transient QuerySplitter querySplitter;

      public SplitQueryFn(V1Options options, int numSplits) {
        this(options, numSplits, new V1DatastoreFactory());
      }

      @VisibleForTesting
      SplitQueryFn(V1Options options, int numSplits,
          V1DatastoreFactory datastoreFactory) {
        this.options = options;
        this.numSplits = numSplits;
        this.datastoreFactory = datastoreFactory;
      }

      @StartBundle
      public void startBundle(Context c) throws Exception {
        datastore = datastoreFactory.getDatastore(c.getPipelineOptions(), options.getProjectId(),
            options.getLocalhost());
        querySplitter = datastoreFactory.getQuerySplitter();
      }

      @ProcessElement
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

        LOG.info("Splitting the query into {} splits", estimatedNumSplits);
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
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        builder.include("options", options);
        if (numSplits > 0) {
          builder.add(DisplayData.item("numQuerySplits", numSplits)
              .withLabel("Requested number of Query splits"));
        }
      }
    }

    /**
     * A {@link DoFn} that reads entities from Cloud Datastore for each query.
     */
    @VisibleForTesting
    static class ReadFn extends DoFn<Query, Entity> {
      private final V1Options options;
      private final V1DatastoreFactory datastoreFactory;
      // Datastore client
      private transient Datastore datastore;

      public ReadFn(V1Options options) {
        this(options, new V1DatastoreFactory());
      }

      @VisibleForTesting
      ReadFn(V1Options options, V1DatastoreFactory datastoreFactory) {
        this.options = options;
        this.datastoreFactory = datastoreFactory;
      }

      @StartBundle
      public void startBundle(Context c) throws Exception {
        datastore = datastoreFactory.getDatastore(c.getPipelineOptions(), options.getProjectId(),
            options.getLocalhost());
      }

      /** Read and output entities for the given query. */
      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        Query query = context.element();
        String namespace = options.getNamespace();
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
                "Expected userLimit %s >= numFetch %s, because query limit %s must be <= userLimit",
                userLimit, numFetch, query.getLimit());
            userLimit -= numFetch;
          }

          // output all the entities from the current batch.
          for (EntityResult entityResult : currentBatch.getEntityResultsList()) {
            context.output(entityResult.getEntity());
          }

          // Check if we have more entities to be read.
          moreResults =
              // User-limit does not exist (so userLimit == MAX_VALUE) and/or has not been satisfied
              (userLimit > 0)
                  // All indications from the API are that there are/may be more results.
                  && ((numFetch == QUERY_BATCH_LIMIT)
                  || (currentBatch.getMoreResults() == NOT_FINISHED));
        }
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        builder.include("options", options);
      }
    }
  }

  /**
   * Returns an empty {@link DatastoreV1.Write} builder. Configure the destination
   * {@code projectId} using {@link DatastoreV1.Write#withProjectId}.
   */
  public Write write() {
    return new Write(null, null);
  }

  /**
   * Returns an empty {@link DeleteEntity} builder. Configure the destination
   * {@code projectId} using {@link DeleteEntity#withProjectId}.
   */
  public DeleteEntity deleteEntity() {
    return new DeleteEntity(null, null);
  }

  /**
   * Returns an empty {@link DeleteKey} builder. Configure the destination
   * {@code projectId} using {@link DeleteKey#withProjectId}.
   */
  public DeleteKey deleteKey() {
    return new DeleteKey(null, null);
  }

  /**
   * A {@link PTransform} that writes {@link Entity} objects to Cloud Datastore.
   *
   * @see DatastoreIO
   */
  public static class Write extends Mutate<Entity> {
    /**
     * Note that {@code projectId} is only {@code @Nullable} as a matter of build order, but if
     * it is {@code null} at instantiation time, an error will be thrown.
     */
    Write(@Nullable ValueProvider<String> projectId, @Nullable String localhost) {
      super(projectId, localhost, new UpsertFn());
    }

    /**
     * Returns a new {@link Write} that writes to the Cloud Datastore for the specified project.
     */
    public Write withProjectId(String projectId) {
      checkNotNull(projectId, "projectId");
      return withProjectId(StaticValueProvider.of(projectId));
    }

    /**
     * Same as {@link Write#withProjectId(String)} but with a {@link ValueProvider}.
     */
    public Write withProjectId(ValueProvider<String> projectId) {
      checkNotNull(projectId, "projectId ValueProvider");
      return new Write(projectId, localhost);
    }

    /**
     * Returns a new {@link Write} that writes to the Cloud Datastore Emulator running locally on
     * the specified host port.
     */
    public Write withLocalhost(String localhost) {
      checkNotNull(localhost, "localhost");
      return new Write(projectId, localhost);
    }
  }

  /**
   * A {@link PTransform} that deletes {@link Entity Entities} from Cloud Datastore.
   *
   * @see DatastoreIO
   */
  public static class DeleteEntity extends Mutate<Entity> {
    /**
     * Note that {@code projectId} is only {@code @Nullable} as a matter of build order, but if
     * it is {@code null} at instantiation time, an error will be thrown.
     */
    DeleteEntity(@Nullable ValueProvider<String> projectId, @Nullable String localhost) {
      super(projectId, localhost, new DeleteEntityFn());
    }

    /**
     * Returns a new {@link DeleteEntity} that deletes entities from the Cloud Datastore for the
     * specified project.
     */
    public DeleteEntity withProjectId(String projectId) {
      checkNotNull(projectId, "projectId");
      return withProjectId(StaticValueProvider.of(projectId));
    }

    /**
     * Same as {@link DeleteEntity#withProjectId(String)} but with a {@link ValueProvider}.
     */
    public DeleteEntity withProjectId(ValueProvider<String> projectId) {
      checkNotNull(projectId, "projectId ValueProvider");
      return new DeleteEntity(projectId, localhost);
    }

    /**
     * Returns a new {@link DeleteEntity} that deletes entities from the Cloud Datastore Emulator
     * running locally on the specified host port.
     */
    public DeleteEntity withLocalhost(String localhost) {
      checkNotNull(localhost, "localhost");
      return new DeleteEntity(projectId, localhost);
    }
  }

  /**
   * A {@link PTransform} that deletes {@link Entity Entities} associated with the given
   * {@link Key Keys} from Cloud Datastore.
   *
   * @see DatastoreIO
   */
  public static class DeleteKey extends Mutate<Key> {
    /**
     * Note that {@code projectId} is only {@code @Nullable} as a matter of build order, but if
     * it is {@code null} at instantiation time, an error will be thrown.
     */
    DeleteKey(@Nullable ValueProvider<String> projectId, @Nullable String localhost) {
      super(projectId, localhost, new DeleteKeyFn());
    }

    /**
     * Returns a new {@link DeleteKey} that deletes entities from the Cloud Datastore for the
     * specified project.
     */
    public DeleteKey withProjectId(String projectId) {
      checkNotNull(projectId, "projectId");
      return withProjectId(StaticValueProvider.of(projectId));
    }

    /**
     * Returns a new {@link DeleteKey} that deletes entities from the Cloud Datastore Emulator
     * running locally on the specified host port.
     */
    public DeleteKey withLocalhost(String localhost) {
      checkNotNull(localhost, "localhost");
      return new DeleteKey(projectId, localhost);
    }

    /**
     * Same as {@link DeleteKey#withProjectId(String)} but with a {@link ValueProvider}.
     */
    public DeleteKey withProjectId(ValueProvider<String> projectId) {
      checkNotNull(projectId, "projectId ValueProvider");
      return new DeleteKey(projectId, localhost);
    }
  }

  /**
   * A {@link PTransform} that writes mutations to Cloud Datastore.
   *
   * <p>It requires a {@link DoFn} that tranforms an object of type {@code T} to a {@link Mutation}.
   * {@code T} is usually either an {@link Entity} or a {@link Key}
   * <b>Note:</b> Only idempotent Cloud Datastore mutation operations (upsert and delete) should
   * be used by the {@code DoFn} provided, as the commits are retried when failures occur.
   */
  private abstract static class Mutate<T> extends PTransform<PCollection<T>, PDone> {
    protected ValueProvider<String> projectId;
    @Nullable
    protected String localhost;
    /** A function that transforms each {@code T} into a mutation. */
    private final SimpleFunction<T, Mutation> mutationFn;

    /**
     * Note that {@code projectId} is only {@code @Nullable} as a matter of build order, but if
     * it is {@code null} at instantiation time, an error will be thrown.
     */
    Mutate(@Nullable ValueProvider<String> projectId, @Nullable String localhost,
        SimpleFunction<T, Mutation> mutationFn) {
      this.projectId = projectId;
      this.localhost = localhost;
      this.mutationFn = checkNotNull(mutationFn);
    }

    @Override
    public PDone expand(PCollection<T> input) {
      input.apply("Convert to Mutation", MapElements.via(mutationFn))
          .apply("Write Mutation to Datastore", ParDo.of(
              new DatastoreWriterFn(projectId, localhost)));

      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PCollection<T> input) {
      checkNotNull(projectId, "projectId ValueProvider");
      if (projectId.isAccessible()) {
        checkNotNull(projectId.get(), "projectId");
      }
      checkNotNull(mutationFn, "mutationFn");
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("projectId", projectId)
          .add("mutationFn", mutationFn.getClass().getName())
          .toString();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("projectId", projectId)
              .withLabel("Output Project"))
          .include("mutationFn", mutationFn);
    }

    public String getProjectId() {
      return projectId.get();
    }
  }

  /**
   * {@link DoFn} that writes {@link Mutation}s to Cloud Datastore. Mutations are written in
   * batches, where the maximum batch size is {@link DatastoreV1#DATASTORE_BATCH_UPDATE_LIMIT}.
   *
   * <p>See <a
   * href="https://cloud.google.com/datastore/docs/concepts/entities">
   * Datastore: Entities, Properties, and Keys</a> for information about entity keys and mutations.
   *
   * <p>Commits are non-transactional.  If a commit fails because of a conflict over an entity
   * group, the commit will be retried (up to {@link DatastoreV1#DATASTORE_BATCH_UPDATE_LIMIT}
   * times). This means that the mutation operation should be idempotent. Thus, the writer should
   * only be used for {code upsert} and {@code delete} mutation operations, as these are the only
   * two Cloud Datastore mutations that are idempotent.
   */
  @VisibleForTesting
  static class DatastoreWriterFn extends DoFn<Mutation, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(DatastoreWriterFn.class);
    private final ValueProvider<String> projectId;
    @Nullable
    private final String localhost;
    private transient Datastore datastore;
    private final V1DatastoreFactory datastoreFactory;
    // Current batch of mutations to be written.
    private final List<Mutation> mutations = new ArrayList<>();

    private static final int MAX_RETRIES = 5;
    private static final FluentBackoff BUNDLE_WRITE_BACKOFF =
        FluentBackoff.DEFAULT
            .withMaxRetries(MAX_RETRIES).withInitialBackoff(Duration.standardSeconds(5));

    DatastoreWriterFn(String projectId, @Nullable String localhost) {
      this(StaticValueProvider.of(projectId), localhost, new V1DatastoreFactory());
    }

    DatastoreWriterFn(ValueProvider<String> projectId, @Nullable String localhost) {
      this(projectId, localhost, new V1DatastoreFactory());
    }

    @VisibleForTesting
    DatastoreWriterFn(ValueProvider<String> projectId, @Nullable String localhost,
        V1DatastoreFactory datastoreFactory) {
      this.projectId = checkNotNull(projectId, "projectId");
      this.localhost = localhost;
      this.datastoreFactory = datastoreFactory;
    }

    @StartBundle
    public void startBundle(Context c) {
      datastore = datastoreFactory.getDatastore(c.getPipelineOptions(), projectId.get(), localhost);
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      mutations.add(c.element());
      if (mutations.size() >= DatastoreV1.DATASTORE_BATCH_UPDATE_LIMIT) {
        flushBatch();
      }
    }

    @FinishBundle
    public void finishBundle(Context c) throws Exception {
      if (!mutations.isEmpty()) {
        flushBatch();
      }
    }

    /**
     * Writes a batch of mutations to Cloud Datastore.
     *
     * <p>If a commit fails, it will be retried up to {@link #MAX_RETRIES} times. All
     * mutations in the batch will be committed again, even if the commit was partially
     * successful. If the retry limit is exceeded, the last exception from Cloud Datastore will be
     * thrown.
     *
     * @throws DatastoreException if the commit fails or IOException or InterruptedException if
     * backing off between retries fails.
     */
    private void flushBatch() throws DatastoreException, IOException, InterruptedException {
      LOG.debug("Writing batch of {} mutations", mutations.size());
      Sleeper sleeper = Sleeper.DEFAULT;
      BackOff backoff = BUNDLE_WRITE_BACKOFF.backoff();

      while (true) {
        // Batch upsert entities.
        try {
          CommitRequest.Builder commitRequest = CommitRequest.newBuilder();
          commitRequest.addAllMutations(mutations);
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
      LOG.debug("Successfully wrote {} mutations", mutations.size());
      mutations.clear();
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("projectId", projectId)
              .withLabel("Output Project"));
    }
  }

  /**
   * Returns true if a Cloud Datastore key is complete. A key is complete if its last element
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

  /**
   * A function that constructs an upsert {@link Mutation} from an {@link Entity}.
   */
  @VisibleForTesting
  static class UpsertFn extends SimpleFunction<Entity, Mutation> {
    @Override
    public Mutation apply(Entity entity) {
      // Verify that the entity to write has a complete key.
      checkArgument(isValidKey(entity.getKey()),
          "Entities to be written to the Cloud Datastore must have complete keys:\n%s", entity);

      return makeUpsert(entity).build();
    }

    @Override
    public void populateDisplayData(Builder builder) {
      builder.add(DisplayData.item("upsertFn", this.getClass())
          .withLabel("Create Upsert Mutation"));
    }
  }

  /**
   * A function that constructs a delete {@link Mutation} from an {@link Entity}.
   */
  @VisibleForTesting
  static class DeleteEntityFn extends SimpleFunction<Entity, Mutation> {
    @Override
    public Mutation apply(Entity entity) {
      // Verify that the entity to delete has a complete key.
      checkArgument(isValidKey(entity.getKey()),
          "Entities to be deleted from the Cloud Datastore must have complete keys:\n%s", entity);

      return makeDelete(entity.getKey()).build();
    }

    @Override
    public void populateDisplayData(Builder builder) {
      builder.add(DisplayData.item("deleteEntityFn", this.getClass())
          .withLabel("Create Delete Mutation"));
    }
  }

  /**
   * A function that constructs a delete {@link Mutation} from a {@link Key}.
   */
  @VisibleForTesting
  static class DeleteKeyFn extends SimpleFunction<Key, Mutation> {
    @Override
    public Mutation apply(Key key) {
      // Verify that the entity to delete has a complete key.
      checkArgument(isValidKey(key),
          "Keys to be deleted from the Cloud Datastore must be complete:\n%s", key);

      return makeDelete(key).build();
    }

    @Override
    public void populateDisplayData(Builder builder) {
      builder.add(DisplayData.item("deleteKeyFn", this.getClass())
          .withLabel("Create Delete Mutation"));
    }
  }

  /**
   * A wrapper factory class for Cloud Datastore singleton classes {@link DatastoreFactory} and
   * {@link QuerySplitter}
   *
   * <p>{@link DatastoreFactory} and {@link QuerySplitter} are not java serializable, hence
   * wrapping them under this class, which implements {@link Serializable}.
   */
  @VisibleForTesting
  static class V1DatastoreFactory implements Serializable {

    /** Builds a Cloud Datastore client for the given pipeline options and project. */
    public Datastore getDatastore(PipelineOptions pipelineOptions, String projectId) {
      return getDatastore(pipelineOptions, projectId, null);
    }

    /**
     * Builds a Cloud Datastore client for the given pipeline options, project and an optional
     * locahost.
     */
    public Datastore getDatastore(PipelineOptions pipelineOptions, String projectId,
        @Nullable String localhost) {
      Credentials credential = pipelineOptions.as(GcpOptions.class).getGcpCredential();
      HttpRequestInitializer initializer;
      if (credential != null) {
        initializer = new ChainingHttpRequestInitializer(
            new HttpCredentialsAdapter(credential),
            new RetryHttpRequestInitializer());
      } else {
        initializer = new RetryHttpRequestInitializer();
      }

      DatastoreOptions.Builder builder =
          new DatastoreOptions.Builder()
              .projectId(projectId)
              .initializer(initializer);

      if (localhost != null) {
        builder.localHost(localhost);
      } else {
        builder.host("batch-datastore.googleapis.com");
      }

      return DatastoreFactory.get().create(builder.build());
    }

    /** Builds a Cloud Datastore {@link QuerySplitter}. */
    public QuerySplitter getQuerySplitter() {
      return DatastoreHelper.getQuerySplitter();
    }
  }
}
