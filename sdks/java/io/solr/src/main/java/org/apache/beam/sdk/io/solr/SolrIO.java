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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
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
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
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
@Experimental(Experimental.Kind.SOURCE_SINK)
public class SolrIO {

  private static final Logger LOG = LoggerFactory.getLogger(SolrIO.class);

  public static Read read() {
    // 1000 for batch size is good enough in many cases,
    // ex: if document size is large, around 10KB, the response's size will be around 10MB
    // if document seize is small, around 1KB, the response's size will be around 1MB
    return new AutoValue_SolrIO_Read.Builder().setBatchSize(1000).setQuery("*:*").build();
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

    @Nullable
    abstract String getUsername();

    @Nullable
    abstract String getPassword();

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

    AuthorizedSolrClient<CloudSolrClient> createClient() throws MalformedURLException {
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
     * {@link RetryPredicate#test(Object)} returns true, {@link Write} tries to resend the
     * requests to the Solr server if the {@link RetryConfiguration} permits it.
     */
    @FunctionalInterface
    interface RetryPredicate extends Predicate<Throwable>, Serializable {}

    /** This is the default predicate used to test if a failed Solr operation should be retried. */
    private static class DefaultRetryPredicate implements RetryPredicate {
      private static final Set<Integer> ELIGIBLE_CODES =
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

    @Nullable
    abstract ConnectionConfiguration getConnectionConfiguration();

    @Nullable
    abstract String getCollection();

    abstract String getQuery();

    abstract int getBatchSize();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration connectionConfiguration);

      abstract Builder setQuery(String query);

      abstract Builder setBatchSize(int batchSize);

      abstract Builder setCollection(String collection);

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
          "Valid values for batchSize are 1 (inclusize) to %s (exclusive), but was: %s ",
          MAX_BATCH_SIZE,
          batchSize);
      return builder().setBatchSize(batchSize).build();
    }

    @Override
    public PCollection<SolrDocument> expand(PBegin input) {
      checkArgument(
          getConnectionConfiguration() != null, "withConnectionConfiguration() is required");
      checkArgument(getCollection() != null, "from() is required");

      return input.apply(org.apache.beam.sdk.io.Read.from(new BoundedSolrSource(this, null)));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("query", getQuery()));
      getConnectionConfiguration().populateDisplayData(builder);
    }
  }

  /** A POJO describing a replica of Solr. */
  @AutoValue
  abstract static class ReplicaInfo implements Serializable {
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

  /** A {@link BoundedSource} reading from Solr. */
  @VisibleForTesting
  static class BoundedSolrSource extends BoundedSource<SolrDocument> {

    private final SolrIO.Read spec;
    // replica is the info of the shard where the source will read the documents
    @Nullable private final ReplicaInfo replica;

    BoundedSolrSource(Read spec, @Nullable Replica replica) {
      this.spec = spec;
      this.replica = replica == null ? null : ReplicaInfo.create(replica);
    }

    @Override
    public List<? extends BoundedSource<SolrDocument>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      ConnectionConfiguration connectionConfig = spec.getConnectionConfiguration();
      List<BoundedSolrSource> sources = new ArrayList<>();
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
          sources.add(new BoundedSolrSource(spec, randomActiveReplica));
        }
      }
      return sources;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws IOException {
      if (replica != null) {
        return getEstimatedSizeOfShard(replica);
      } else {
        return getEstimatedSizeOfCollection();
      }
    }

    private long getEstimatedSizeOfShard(ReplicaInfo replica) throws IOException {
      try (AuthorizedSolrClient solrClient =
          spec.getConnectionConfiguration().createClient(replica.baseUrl())) {
        CoreAdminRequest req = new CoreAdminRequest();
        req.setAction(CoreAdminParams.CoreAdminAction.STATUS);
        req.setIndexInfoNeeded(true);
        CoreAdminResponse response;
        try {
          response = solrClient.process(req);
        } catch (SolrServerException e) {
          throw new IOException("Can not get core status from " + replica, e);
        }
        NamedList<Object> coreStatus = response.getCoreStatus(replica.coreName());
        NamedList<Object> indexStats = (NamedList<Object>) coreStatus.get("index");
        return (long) indexStats.get("sizeInBytes");
      }
    }

    private long getEstimatedSizeOfCollection() throws IOException {
      long sizeInBytes = 0;
      ConnectionConfiguration config = spec.getConnectionConfiguration();
      try (AuthorizedSolrClient<CloudSolrClient> solrClient = config.createClient()) {
        DocCollection docCollection =
            AuthorizedSolrClient.getClusterState(solrClient).getCollection(spec.getCollection());
        if (docCollection.getSlices().isEmpty()) {
          return 0;
        }

        ArrayList<Slice> slices = new ArrayList<>(docCollection.getSlices());
        Collections.shuffle(slices);
        ExecutorService executor =
            Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                    .setThreadFactory(MoreExecutors.platformThreadFactory())
                    .setDaemon(true)
                    .setNameFormat("solrio-size-of-collection-estimation")
                    .build());
        try {
          ArrayList<Future<Long>> futures = new ArrayList<>();
          for (int i = 0; i < 100 && i < slices.size(); i++) {
            Slice slice = slices.get(i);
            final Replica replica = slice.getLeader();
            Future<Long> future =
                executor.submit(() -> getEstimatedSizeOfShard(ReplicaInfo.create(replica)));
            futures.add(future);
          }
          for (Future<Long> future : futures) {
            try {
              sizeInBytes += future.get();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IOException(e);
            } catch (ExecutionException e) {
              throw new IOException("Can not estimate size of shard", e.getCause());
            }
          }
        } finally {
          executor.shutdownNow();
        }

        if (slices.size() <= 100) {
          return sizeInBytes;
        }
        return (sizeInBytes / 100) * slices.size();
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
      if (replica != null) {
        builder.addIfNotNull(DisplayData.item("shardUrl", replica.coreUrl()));
      }
    }

    @Override
    public BoundedReader<SolrDocument> createReader(PipelineOptions options) throws IOException {
      return new BoundedSolrReader(this);
    }

    @Override
    public Coder<SolrDocument> getOutputCoder() {
      return JavaBinCodecCoder.of(SolrDocument.class);
    }
  }

  private static class BoundedSolrReader extends BoundedSource.BoundedReader<SolrDocument> {

    private final BoundedSolrSource source;

    private AuthorizedSolrClient solrClient;
    private SolrDocument current;
    private String cursorMark;
    private Iterator<SolrDocument> batchIterator;
    private boolean done;
    private String uniqueKey;

    private BoundedSolrReader(BoundedSolrSource source) {
      this.source = source;
      this.cursorMark = CursorMarkParams.CURSOR_MARK_START;
    }

    @Override
    public boolean start() throws IOException {
      if (source.replica != null) {
        solrClient =
            source.spec.getConnectionConfiguration().createClient(source.replica.baseUrl());
      } else {
        solrClient = source.spec.getConnectionConfiguration().createClient();
      }
      SchemaRequest.UniqueKey uniqueKeyRequest = new SchemaRequest.UniqueKey();
      try {
        String collection = source.spec.getCollection();
        SchemaResponse.UniqueKeyResponse uniqueKeyResponse =
            (SchemaResponse.UniqueKeyResponse) solrClient.process(collection, uniqueKeyRequest);
        uniqueKey = uniqueKeyResponse.getUniqueKey();
      } catch (SolrServerException e) {
        throw new IOException("Can not get unique key from solr", e);
      }
      return advance();
    }

    private SolrQuery getQueryParams(BoundedSolrSource source) {
      String query = source.spec.getQuery();
      if (query == null) {
        query = "*:*";
      }
      SolrQuery solrQuery = new SolrQuery(query);
      solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
      solrQuery.setRows(source.spec.getBatchSize());
      solrQuery.addSort(uniqueKey, SolrQuery.ORDER.asc);
      if (source.replica != null) {
        solrQuery.setDistrib(false);
      }
      return solrQuery;
    }

    private void updateCursorMark(QueryResponse response) {
      if (cursorMark.equals(response.getNextCursorMark())) {
        done = true;
      }
      cursorMark = response.getNextCursorMark();
    }

    @Override
    public boolean advance() throws IOException {
      if (batchIterator != null && batchIterator.hasNext()) {
        current = batchIterator.next();
        return true;
      } else {
        SolrQuery solrQuery = getQueryParams(source);
        try {
          QueryResponse response;
          if (source.replica != null) {
            response = solrClient.query(source.replica.coreName(), solrQuery);
          } else {
            response = solrClient.query(source.spec.getCollection(), solrQuery);
          }
          updateCursorMark(response);
          return readNextBatchAndReturnFirstDocument(response);
        } catch (SolrServerException e) {
          throw new IOException(e);
        }
      }
    }

    private boolean readNextBatchAndReturnFirstDocument(QueryResponse response) {
      if (done) {
        current = null;
        batchIterator = null;
        return false;
      }

      batchIterator = response.getResults().iterator();
      current = batchIterator.next();
      return true;
    }

    @Override
    public SolrDocument getCurrent() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override
    public void close() throws IOException {
      solrClient.close();
    }

    @Override
    public BoundedSource<SolrDocument> getCurrentSource() {
      return source;
    }
  }


  /** A {@link PTransform} writing data to Solr. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<SolrInputDocument>, PDone> {
    @Nullable
    abstract ConnectionConfiguration getConnectionConfiguration();

    @Nullable
    abstract String getCollection();

    abstract int getMaxBatchSize();

    abstract Builder builder();

    @Nullable
    abstract RetryConfiguration getRetryConfiguration();

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
     * smaller batch.
     *
     * @param batchSize maximum batch size in number of documents
     */
    @VisibleForTesting
    Write withMaxBatchSize(int batchSize) {
      // TODO remove this configuration, we can figure out the best number
      // by tuning batchSize when pipelines run.
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
      public void setup() throws Exception {
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
      public void startBundle(StartBundleContext context) throws Exception {
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
