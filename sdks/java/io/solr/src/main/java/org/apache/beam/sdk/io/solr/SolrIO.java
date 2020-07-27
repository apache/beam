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
package org.apache.beam.sdk.io.solr;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Predicate;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms for reading and writing data from/to Solr.
 *
 * <h3>Reading from Solr</h3>
 *
 * <p>{@link SolrIO#read SolrIO.read()} returns a bounded {@link PCollection
 * PCollection&lt;SolrDocument&gt;} representing Solr documents.
 *
 * <p>To configure the {@link SolrIO#read}, you have to provide a connection configuration
 * containing the Zookeeper address of the Solr cluster, and the collection name. The following
 * example illustrates options for configuring the source:
 *
 * <pre>{@code
 * SolrIO.ConnectionConfiguration conn = SolrIO.ConnectionConfiguration.create("127.0.0.1:9983");
 * // Optionally: .withBasicCredentials(username, password)
 *
 * PCollection<SolrDocument> docs = p.apply(
 *     SolrIO.read().from("my-collection").withConnectionConfiguration(conn));
 *
 * }</pre>
 *
 * <p>You can specify a query on the {@code read()} using {@code withQuery()}.
 *
 * <h3>Writing to Solr</h3>
 *
 * <p>To write documents to Solr, use {@link SolrIO#write SolrIO.write()}, which writes Solr
 * documents from a {@link PCollection PCollection&lt;SolrInputDocument&gt;} (which can be bounded
 * or unbounded).
 *
 * <p>To configure {@link SolrIO#write SolrIO.write()}, similar to the read, you have to provide a
 * connection configuration, and a collection name. For instance:
 *
 * <pre>{@code
 * PCollection<SolrInputDocument> inputDocs = ...;
 * inputDocs.apply(SolrIO.write().to("my-collection").withConnectionConfiguration(conn));
 *
 * }</pre>
 *
 * <p>When writing it is possible to customize the retry behavior if an error is encountered. By
 * default this is disabled and only one attempt will be made.
 */
@Experimental(Kind.SOURCE_SINK)
public class SolrIO {

  private static final Logger LOG = LoggerFactory.getLogger(SolrIO.class);

  public static Read read() {
    // 1000 for batch size is good enough in many cases,
    // ex: if document size is large, around 10KB, the response's size will be around 10MB
    // if document seize is small, around 1KB, the response's size will be around 1MB
    return new AutoValue_SolrIO_Read.Builder().setBatchSize(1000).setQuery("*:*").build();
  }

  public static ReadAll readAll() {
    return new ReadAll();
  }

  public static Write write() {
    // 1000 for batch size is good enough in many cases,
    // ex: if document size is large, around 10KB, the request's size will be around 10MB
    // if document size is small, around 1KB, the request's size will be around 1MB
    return new AutoValue_SolrIO_Write.Builder().setMaxBatchSize(1000).build();
  }

  private SolrIO() {}

  /** A POJO describing a connection configuration to Solr. */
  @AutoValue
  public abstract static class ConnectionConfiguration implements Serializable {

    abstract String getZkHost();

    abstract @Nullable String getUsername();

    abstract @Nullable String getPassword();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setZkHost(String zkHost);

      abstract Builder setUsername(String username);

      abstract Builder setPassword(String password);

      abstract ConnectionConfiguration build();
    }

    /**
     * Creates a new Solr connection configuration.
     *
     * @param zkHost host of zookeeper
     * @return the connection configuration object
     */
    public static ConnectionConfiguration create(String zkHost) {
      checkArgument(zkHost != null, "zkHost can not be null");
      return new AutoValue_SolrIO_ConnectionConfiguration.Builder().setZkHost(zkHost).build();
    }

    /** If Solr basic authentication is enabled, provide the username and password. */
    public ConnectionConfiguration withBasicCredentials(String username, String password) {
      checkArgument(username != null, "username can not be null");
      checkArgument(!username.isEmpty(), "username can not be empty");
      checkArgument(password != null, "password can not be null");
      checkArgument(!password.isEmpty(), "password can not be empty");
      return builder().setUsername(username).setPassword(password).build();
    }

    private void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("zkHost", getZkHost()));
      builder.addIfNotNull(DisplayData.item("username", getUsername()));
    }

    private HttpClient createHttpClient() {
      // This is bug in Solr, if we don't create a customize HttpClient,
      // UpdateRequest with commit flag will throw an authentication error.
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(HttpClientUtil.PROP_BASIC_AUTH_USER, getUsername());
      params.set(HttpClientUtil.PROP_BASIC_AUTH_PASS, getPassword());
      return HttpClientUtil.createClient(params);
    }

    AuthorizedSolrClient<CloudSolrClient> createClient() {
      CloudSolrClient solrClient = new CloudSolrClient(getZkHost(), createHttpClient());
      return new AuthorizedSolrClient<>(solrClient, this);
    }

    AuthorizedSolrClient<HttpSolrClient> createClient(String shardUrl) {
      HttpSolrClient solrClient = new HttpSolrClient(shardUrl, createHttpClient());
      return new AuthorizedSolrClient<>(solrClient, this);
    }
  }

  /**
   * A POJO encapsulating a configuration for retry behavior when issuing requests to Solr. A retry
   * will be attempted until the maxAttempts or maxDuration is exceeded, whichever comes first, for
   * any of the following exceptions:
   *
   * <ul>
   *   <li>{@link IOException}
   *   <li>{@link SolrServerException}
   *   <li>{@link SolrException} where the {@link SolrException.ErrorCode} is one of:
   *       <ul>
   *         <li>{@link SolrException.ErrorCode#CONFLICT}
   *         <li>{@link SolrException.ErrorCode#SERVER_ERROR}
   *         <li>{@link SolrException.ErrorCode#SERVICE_UNAVAILABLE}
   *         <li>{@link SolrException.ErrorCode#INVALID_STATE}
   *         <li>{@link SolrException.ErrorCode#UNKNOWN}
   *       </ul>
   * </ul>
   */
  @AutoValue
  public abstract static class RetryConfiguration implements Serializable {
    @VisibleForTesting
    static final RetryPredicate DEFAULT_RETRY_PREDICATE = new DefaultRetryPredicate();

    abstract int getMaxAttempts();

    abstract Duration getMaxDuration();

    abstract RetryPredicate getRetryPredicate();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract SolrIO.RetryConfiguration.Builder setMaxAttempts(int maxAttempts);

      abstract SolrIO.RetryConfiguration.Builder setMaxDuration(Duration maxDuration);

      abstract SolrIO.RetryConfiguration.Builder setRetryPredicate(RetryPredicate retryPredicate);

      abstract SolrIO.RetryConfiguration build();
    }

    public static RetryConfiguration create(int maxAttempts, Duration maxDuration) {
      checkArgument(maxAttempts > 0, "maxAttempts must be greater than 0");
      checkArgument(
          maxDuration != null && maxDuration.isLongerThan(Duration.ZERO),
          "maxDuration must be greater than 0");
      return new AutoValue_SolrIO_RetryConfiguration.Builder()
          .setMaxAttempts(maxAttempts)
          .setMaxDuration(maxDuration)
          .setRetryPredicate(DEFAULT_RETRY_PREDICATE)
          .build();
    }

    // Exposed only to allow tests to easily simulate server errors
    @VisibleForTesting
    RetryConfiguration withRetryPredicate(RetryPredicate predicate) {
      checkArgument(predicate != null, "predicate must be provided");
      return builder().setRetryPredicate(predicate).build();
    }

    /**
     * An interface used to control if we retry the Solr call when a {@link Throwable} occurs. If
     * {@link RetryPredicate#test(Object)} returns true, {@link Write} tries to resend the requests
     * to the Solr server if the {@link RetryConfiguration} permits it.
     */
    @FunctionalInterface
    interface RetryPredicate extends Predicate<Throwable>, Serializable {}

    /** This is the default predicate used to test if a failed Solr operation should be retried. */
    private static class DefaultRetryPredicate implements RetryPredicate {
      private static final ImmutableSet<Integer> ELIGIBLE_CODES =
          ImmutableSet.of(
              SolrException.ErrorCode.CONFLICT.code,
              SolrException.ErrorCode.SERVER_ERROR.code,
              SolrException.ErrorCode.SERVICE_UNAVAILABLE.code,
              SolrException.ErrorCode.INVALID_STATE.code,
              SolrException.ErrorCode.UNKNOWN.code);

      @Override
      public boolean test(Throwable t) {
        return (t instanceof IOException
            || t instanceof SolrServerException
            || (t instanceof SolrException && ELIGIBLE_CODES.contains(((SolrException) t).code())));
      }
    }
  }

  /** A {@link PTransform} reading data from Solr. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<SolrDocument>> {
    private static final long MAX_BATCH_SIZE = 10000L;

    abstract @Nullable ConnectionConfiguration getConnectionConfiguration();

    abstract @Nullable String getCollection();

    abstract String getQuery();

    abstract int getBatchSize();

    abstract @Nullable ReplicaInfo getReplicaInfo();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration connectionConfiguration);

      abstract Builder setCollection(String collection);

      abstract Builder setQuery(String query);

      abstract Builder setBatchSize(int batchSize);

      abstract Builder setReplicaInfo(ReplicaInfo replicaInfo);

      abstract Read build();
    }

    /** Provide the Solr connection configuration object. */
    public Read withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkArgument(connectionConfiguration != null, "connectionConfiguration can not be null");
      return builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    /**
     * Provide name of collection while reading from Solr.
     *
     * @param collection the collection toward which the requests will be issued
     */
    public Read from(String collection) {
      checkArgument(collection != null, "collection can not be null");
      return builder().setCollection(collection).build();
    }

    /**
     * Provide a query used while reading from Solr.
     *
     * @param query the query. See <a
     *     href="https://cwiki.apache.org/confluence/display/solr/The+Standard+Query+Parser">Solr
     *     Query </a>
     */
    public Read withQuery(String query) {
      checkArgument(query != null, "query can not be null");
      checkArgument(!query.isEmpty(), "query can not be empty");
      return builder().setQuery(query).build();
    }

    /**
     * Provide a size for the cursor read. See <a
     * href="https://cwiki.apache.org/confluence/display/solr/Pagination+of+Results">cursor API</a>
     * Default is 100. Maximum is 10 000. If documents are small, increasing batch size might
     * improve read performance. If documents are big, you might need to decrease batchSize
     *
     * @param batchSize number of documents read in each scroll read
     */
    @VisibleForTesting
    Read withBatchSize(int batchSize) {
      // TODO remove this configuration, we can figure out the best number
      // by tuning batchSize when pipelines run.
      checkArgument(
          batchSize > 0 && batchSize < MAX_BATCH_SIZE,
          "Valid values for batchSize are 1 (inclusive) to %s (exclusive), but was: %s ",
          MAX_BATCH_SIZE,
          batchSize);
      return builder().setBatchSize(batchSize).build();
    }

    /** Read from a specific Replica (partition). */
    public Read withReplicaInfo(ReplicaInfo replicaInfo) {
      checkArgument(replicaInfo != null, "replicaInfo can not be null");
      return builder().setReplicaInfo(replicaInfo).build();
    }

    @Override
    public PCollection<SolrDocument> expand(PBegin input) {
      checkArgument(
          getConnectionConfiguration() != null, "withConnectionConfiguration() is required");
      checkArgument(getCollection() != null, "from() is required");
      return input.apply("Create", Create.of(this)).apply("ReadAll", readAll());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      getConnectionConfiguration().populateDisplayData(builder);
      builder.add(DisplayData.item("collection", getCollection()));
      builder.addIfNotNull(DisplayData.item("query", getQuery()));
      builder.add(DisplayData.item("batchSize", getBatchSize()));
      final String replicaInfo = (getReplicaInfo() != null) ? getReplicaInfo().toString() : null;
      builder.addIfNotNull(DisplayData.item("replicaInfo", replicaInfo));
    }
  }

  /** A POJO describing a replica of Solr. */
  @AutoValue
  public abstract static class ReplicaInfo implements Serializable {
    public abstract String coreName();

    public abstract String coreUrl();

    public abstract String baseUrl();

    static ReplicaInfo create(Replica replica) {
      return new AutoValue_SolrIO_ReplicaInfo(
          replica.getStr(ZkStateReader.CORE_NAME_PROP),
          replica.getCoreUrl(),
          replica.getStr(ZkStateReader.BASE_URL_PROP));
    }
  }

  private static class SplitFn extends DoFn<Read, Read> {
    @ProcessElement
    public void process(@Element Read spec, OutputReceiver<Read> out) throws IOException {
      ConnectionConfiguration connectionConfig = spec.getConnectionConfiguration();
      try (AuthorizedSolrClient<CloudSolrClient> client = connectionConfig.createClient()) {
        String collection = spec.getCollection();
        final ClusterState clusterState = AuthorizedSolrClient.getClusterState(client);
        DocCollection docCollection = clusterState.getCollection(collection);
        for (Slice slice : docCollection.getSlices()) {
          ArrayList<Replica> replicas = new ArrayList<>(slice.getReplicas());
          Collections.shuffle(replicas);
          // Load balancing by randomly picking an active replica
          Replica randomActiveReplica = null;
          for (Replica replica : replicas) {
            // We need to check both state of the replica and live nodes
            // to make sure that the replica is alive
            if (replica.getState() == Replica.State.ACTIVE
                && clusterState.getLiveNodes().contains(replica.getNodeName())) {
              randomActiveReplica = replica;
              break;
            }
          }
          // TODO in case of this replica goes inactive while the pipeline runs.
          // We should pick another active replica of this shard.
          checkState(
              randomActiveReplica != null,
              "Can not found an active replica for slice %s",
              slice.getName());
          out.output(spec.withReplicaInfo(ReplicaInfo.create(checkNotNull(randomActiveReplica))));
        }
      }
    }
  }

  private static class ReadFn extends DoFn<Read, SolrDocument> {
    @ProcessElement
    public void process(@Element Read spec, OutputReceiver<SolrDocument> out) throws IOException {
      ReplicaInfo replicaInfo = spec.getReplicaInfo();
      checkArgument(replicaInfo != null, "replicaInfo is required");
      String cursorMark = CursorMarkParams.CURSOR_MARK_START;
      String query = spec.getQuery();
      if (query == null) {
        query = "*:*";
      }
      SolrQuery solrQuery = new SolrQuery(query);
      solrQuery.setRows(spec.getBatchSize());
      solrQuery.setDistrib(false);
      try (AuthorizedSolrClient<HttpSolrClient> client =
          spec.getConnectionConfiguration().createClient(replicaInfo.baseUrl())) {
        SchemaRequest.UniqueKey request = new SchemaRequest.UniqueKey();
        try {
          SchemaResponse.UniqueKeyResponse response = client.process(spec.getCollection(), request);
          solrQuery.addSort(response.getUniqueKey(), SolrQuery.ORDER.asc);
        } catch (SolrServerException e) {
          throw new IOException("Can not get unique key from solr", e);
        }

        while (true) {
          solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
          try {
            QueryResponse response;
            response = client.query(replicaInfo.coreName(), solrQuery);
            if (cursorMark.equals(response.getNextCursorMark())) {
              break;
            }
            cursorMark = response.getNextCursorMark();
            for (SolrDocument doc : response.getResults()) {
              out.output(doc);
            }
          } catch (SolrServerException e) {
            throw new IOException(e);
          }
        }
      }
    }
  }

  public static class ReadAll extends PTransform<PCollection<Read>, PCollection<SolrDocument>> {
    @Override
    public PCollection<SolrDocument> expand(PCollection<Read> input) {
      return input
          .apply("Split", ParDo.of(new SplitFn()))
          .apply("Reshuffle", Reshuffle.viaRandomKey())
          .apply("Read", ParDo.of(new ReadFn()));
    }
  }

  /** A {@link PTransform} writing data to Solr. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<SolrInputDocument>, PDone> {

    abstract @Nullable ConnectionConfiguration getConnectionConfiguration();

    abstract @Nullable String getCollection();

    abstract int getMaxBatchSize();

    abstract Builder builder();

    abstract @Nullable RetryConfiguration getRetryConfiguration();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration connectionConfiguration);

      abstract Builder setCollection(String collection);

      abstract Builder setMaxBatchSize(int maxBatchSize);

      abstract Builder setRetryConfiguration(RetryConfiguration retryConfiguration);

      abstract Write build();
    }

    /** Provide the Solr connection configuration object. */
    public Write withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkArgument(connectionConfiguration != null, "connectionConfiguration can not be null");
      return builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    /**
     * Provide name of collection while reading from Solr.
     *
     * @param collection the collection toward which the requests will be issued
     */
    public Write to(String collection) {
      checkArgument(collection != null, "collection can not be null");
      return builder().setCollection(collection).build();
    }

    /**
     * Provide a maximum size in number of documents for the batch. Depending on the execution
     * engine, size of bundles may vary, this sets the maximum size. Change this if you need to have
     * smaller batch. Default max batch size is 1000.
     *
     * @param batchSize maximum batch size in number of documents
     */
    public Write withMaxBatchSize(int batchSize) {
      checkArgument(batchSize > 0, "batchSize must be larger than 0, but was: %s", batchSize);
      return builder().setMaxBatchSize(batchSize).build();
    }

    /**
     * Provides configuration to retry a failed batch call to Solr. A batch is considered as failed
     * if the underlying {@link CloudSolrClient} surfaces {@link
     * org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException}, {@link
     * SolrServerException} or {@link IOException}. Users should consider that retrying might
     * compound the underlying problem which caused the initial failure. Users should also be aware
     * that once retrying is exhausted the error is surfaced to the runner which <em>may</em> then
     * opt to retry the current partition in entirety or abort if the max number of retries of the
     * runner is completed. Retrying uses an exponential backoff algorithm, with minimum backoff of
     * 5 seconds and then surfacing the error once the maximum number of retries or maximum
     * configuration duration is exceeded.
     *
     * <p>Example use:
     *
     * <pre>{@code
     * SolrIO.write()
     *   .withRetryConfiguration(SolrIO.RetryConfiguration.create(10, Duration.standardMinutes(3))
     *   ...
     * }</pre>
     *
     * @param retryConfiguration the rules which govern the retry behavior
     * @return the {@link Write} with retrying configured
     */
    public Write withRetryConfiguration(RetryConfiguration retryConfiguration) {
      checkArgument(retryConfiguration != null, "retryConfiguration is required");
      return builder().setRetryConfiguration(retryConfiguration).build();
    }

    @Override
    public PDone expand(PCollection<SolrInputDocument> input) {
      checkState(getConnectionConfiguration() != null, "withConnectionConfiguration() is required");
      checkState(getCollection() != null, "to() is required");

      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    @VisibleForTesting
    static class WriteFn extends DoFn<SolrInputDocument, Void> {
      @VisibleForTesting
      static final String RETRY_ATTEMPT_LOG = "Error writing to Solr. Retry attempt[%d]";

      private static final Duration RETRY_INITIAL_BACKOFF = Duration.standardSeconds(5);

      private transient FluentBackoff retryBackoff; // defaults to no retrying
      private final Write spec;
      private transient AuthorizedSolrClient solrClient;
      private Collection<SolrInputDocument> batch;

      WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() {
        solrClient = spec.getConnectionConfiguration().createClient();

        retryBackoff =
            FluentBackoff.DEFAULT
                .withMaxRetries(0) // default to no retrying
                .withInitialBackoff(RETRY_INITIAL_BACKOFF);

        if (spec.getRetryConfiguration() != null) {
          // FluentBackoff counts retries excluding the original while we count attempts
          // to remove ambiguity (hence the -1)
          retryBackoff =
              retryBackoff
                  .withMaxRetries(spec.getRetryConfiguration().getMaxAttempts() - 1)
                  .withMaxCumulativeBackoff(spec.getRetryConfiguration().getMaxDuration());
        }
      }

      @StartBundle
      public void startBundle(StartBundleContext context) {
        batch = new ArrayList<>();
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        SolrInputDocument document = context.element();
        batch.add(document);
        if (batch.size() >= spec.getMaxBatchSize()) {
          flushBatch();
        }
      }

      @FinishBundle
      public void finishBundle(FinishBundleContext context) throws Exception {
        flushBatch();
      }

      // Flushes the batch, implementing the retry mechanism as configured in the spec.
      private void flushBatch() throws IOException, InterruptedException {
        if (batch.isEmpty()) {
          return;
        }
        try {
          UpdateRequest updateRequest = new UpdateRequest();
          updateRequest.add(batch);

          Sleeper sleeper = Sleeper.DEFAULT;
          BackOff backoff = retryBackoff.backoff();
          int attempt = 0;
          while (true) {
            attempt++;
            try {
              solrClient.process(spec.getCollection(), updateRequest);
              break;
            } catch (Exception exception) {

              // fail immediately if no retry configuration doesn't handle this
              if (spec.getRetryConfiguration() == null
                  || !spec.getRetryConfiguration().getRetryPredicate().test(exception)) {
                throw new IOException(
                    "Error writing to Solr (no attempt made to retry)", exception);
              }

              // see if we can pause and try again
              if (!BackOffUtils.next(sleeper, backoff)) {
                throw new IOException(
                    String.format(
                        "Error writing to Solr after %d attempt(s). No more attempts allowed",
                        attempt),
                    exception);

              } else {
                // Note: this used in test cases to verify behavior
                LOG.warn(String.format(RETRY_ATTEMPT_LOG, attempt), exception);
              }
            }
          }
        } finally {
          batch.clear();
        }
      }

      @Teardown
      public void closeClient() throws IOException {
        if (solrClient != null) {
          solrClient.close();
        }
      }
    }
  }
}
