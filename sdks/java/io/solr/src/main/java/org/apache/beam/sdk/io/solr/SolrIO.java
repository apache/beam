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
import com.google.common.base.Strings;

import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
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
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;

/**
 * Transforms for reading and writing data from/to Solr.
 *
 * <h3>Reading from Solr</h3>
 *
 * <p>{@link SolrIO#read SolrIO.read()} returns a bounded
 * {@link PCollection PCollection&lt;SolrDocument&gt;} representing Solr documents.
 *
 * <p>To configure the {@link SolrIO#read}, you have to provide a connection configuration
 * containing the Zookeeper address of the Solr cluster. The following example
 * illustrates options for configuring the source:
 *
 * <pre>{@code
 *
 * pipeline.apply(SolrIO.read().withConnectionConfiguration(
 *    SolrIO.ConnectionConfiguration.create("127.0.0.1:9983", "my-collection")
 * )
 *
 * }</pre>
 *
 * <p>You can specify a query on the {@code read()} using {@code withQuery()}.
 *
 * <h3>Writing to Solr</h3>
 *
 * <p>To write documents to Solr, use
 * {@link SolrIO#write SolrIO.write()}, which writes Solr documents from a
 * {@link PCollection PCollection&lt;SolrInputDocument&gt;} (which can be bounded or unbounded).
 *
 * <p>To configure {@link SolrIO#write SolrIO.write()}, similar to the read, you
 * have to provide a connection configuration. For instance:
 *
 * <pre>{@code
 *
 *  pipeline
 *    .apply(...)
 *    .apply(SolrIO.write().withConnectionConfiguration(
 *       SolrIO.ConnectionConfiguration.create("127.0.0.1:9983", "my-collection")
 *    )
 *
 * }</pre>
 *
 * <p>Optionally, you can provide {@code withBatchSize()}
 * to specify the size of the write batch in number of documents.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class SolrIO {

  /**
   * Create new SolrIO.Read based on provided Solr connection configuration object.
   * @param connectionConfiguration the Solr {@link ConnectionConfiguration} object
   * @return the {@link Read} with connection configuration set
   */
  public static Read read(ConnectionConfiguration connectionConfiguration) {
    checkArgument(
        connectionConfiguration != null,
        "SolrIO.read(connectionConfiguration) "
            + "called with null connectionConfiguration");
    return new AutoValue_SolrIO_Read.Builder()
        .setConnectionConfiguration(connectionConfiguration)
        .setBatchSize(100L)
        .setQuery("*:*")
        .build();
  }

  /**
   * Create new SolrIO.Write based on provided Solr connection configuration object.
   * @param connectionConfiguration the Solr {@link ConnectionConfiguration} object
   * @return the {@link Write} with connection configuration set
   */
  public static Write write(ConnectionConfiguration connectionConfiguration) {
    checkArgument(
        connectionConfiguration != null,
        "SolrIO.write(connectionConfiguration) "
            + "called with null connectionConfiguration");
    return new AutoValue_SolrIO_Write.Builder()
        .setMaxBatchSize(1000L)
        .setConnectionConfiguration(connectionConfiguration)
        .build();
  }

  private SolrIO() {
  }

  /** A POJO describing a connection configuration to Solr. */
  @AutoValue
  public abstract static class ConnectionConfiguration implements Serializable {

    abstract String getZkHost();
    abstract String getCollection();
    @Nullable abstract String getUsername();
    @Nullable abstract String getPassword();
    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setZkHost(String zkHost);
      abstract Builder setCollection(String collection);
      abstract Builder setUsername(String username);
      abstract Builder setPassword(String password);
      abstract ConnectionConfiguration build();
    }

    /**
     * Creates a new Solr connection configuration.
     *
     * @param zkHost host of zookeeper
     * @param collection the collection toward which the requests will be issued
     * @return the connection configuration object
     * @throws IOException when it fails to connect to Solr
     */
    public static ConnectionConfiguration create(String zkHost, String collection)
        throws IOException {
      checkArgument(zkHost != null,
          "ConnectionConfiguration.create(zkHost, collection) "
              + "called with null address");
      checkArgument(collection != null,
          "ConnectionConfiguration.create(zkHost, collection) "
              + "called with null collectioin");
      return new AutoValue_SolrIO_ConnectionConfiguration.Builder()
          .setZkHost(zkHost)
          .setCollection(collection)
          .build();
    }

    /**
     * If Solr basic authentication is enabled, provide the username and password.
     *
     * @param username the username used to authenticate to Solr
     * @param password the password used to authenticate to Solr
     * @return the {@link ConnectionConfiguration} object with basic credentials set
     */
    public ConnectionConfiguration withBasicCredentials(String username, String password) {
      checkArgument(
          username != null,
          "ConnectionConfiguration.create().withBasicCredentials(username, password) "
              + "called with null username");
      checkArgument(
          !username.isEmpty(),
          "ConnectionConfiguration.create().withBasicCredentials(username, password) "
              + "called with empty username");
      checkArgument(
          password != null,
          "ConnectionConfiguration.create().withBasicCredentials(username, password) "
              + "called with null username");
      checkArgument(
          !password.isEmpty(),
          "ConnectionConfiguration.create().withBasicCredentials(username, password) "
              + "called with empty username");
      return builder().setUsername(username).setPassword(password).build();
    }

    private void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("zkHost", getZkHost()));
      builder.add(DisplayData.item("collection", getCollection()));
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

    AuthorizedCloudSolrClient createClient() throws MalformedURLException {
      CloudSolrClient solrClient = new CloudSolrClient(getZkHost(), createHttpClient());
      solrClient.setDefaultCollection(getCollection());
      return new AuthorizedCloudSolrClient(solrClient, this);
    }

    AuthorizedSolrClient createClient(String shardUrl) {
      HttpSolrClient solrClient = new HttpSolrClient(shardUrl, createHttpClient());
      return new AuthorizedSolrClient<>(solrClient, this);
    }
  }

  /** A {@link PTransform} reading data from Solr. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<SolrDocument>> {
    private static final long MAX_BATCH_SIZE = 10000L;

    abstract ConnectionConfiguration getConnectionConfiguration();
    abstract String getQuery();
    abstract long getBatchSize();
    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration connectionConfiguration);
      abstract Builder setQuery(String query);
      abstract Builder setBatchSize(long batchSize);
      abstract Read build();
    }

    /**
     * Provide a query used while reading from Solr.
     *
     * @param query the query. See <a
     *     href="https://cwiki.apache.org/confluence/display/solr/The+Standard+Query+Parser">
     *              Solr Query
     *     </a>
     * @return the {@link Read} object with query set
     */
    public Read withQuery(String query) {
      checkArgument(!Strings.isNullOrEmpty(query),
          "SolrIO.read().withQuery(query) called" + " with null or empty query");
      return builder().setQuery(query).build();
    }

    /**
     * Provide a size for the cursor read. See <a
     * href="https://cwiki.apache.org/confluence/display/solr/Pagination+of+Results">
     * cursor API</a> Default is 100. Maximum is 10 000. If documents are small, increasing batch
     * size might improve read performance. If documents are big, you might need to decrease
     * batchSize
     *
     * @param batchSize number of documents read in each scroll read
     * @return the {@link Read} with batch size set
     */
    public Read withBatchSize(long batchSize) {
      checkArgument(batchSize > 0, "SolrIO.read().withBatchSize(batchSize) "
          + "called with a negative or equal to 0 value: %s", batchSize);
      checkArgument(batchSize <= MAX_BATCH_SIZE,
          "SolrIO.read().withBatchSize(batchSize) "
              + "called with a too large value (over %s): %s",
          MAX_BATCH_SIZE, batchSize);
      return builder().setBatchSize(batchSize).build();
    }

    @Override
    public PCollection<SolrDocument> expand(PBegin input) {
      return input.apply(org.apache.beam.sdk.io.Read
          .from(new BoundedSolrSource(this, null)));
    }

    @Override
    public void validate(PipelineOptions options) {
      checkState(getConnectionConfiguration() != null,
          "SolrIO.read() requires a connection configuration"
              + " to be set via withConnectionConfiguration(configuration)");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("query", getQuery()));
      getConnectionConfiguration().populateDisplayData(builder);
    }
  }

  /**  A POJO describing a replica of Solr. */
  @AutoValue
  abstract static class ReplicaInfo implements Serializable {
    public abstract String coreName();
    public abstract String coreUrl();
    public abstract String baseUrl();

    static ReplicaInfo create(Replica replica) {
      return new AutoValue_SolrIO_ReplicaInfo(replica.getStr(ZkStateReader.CORE_NAME_PROP),
          replica.getCoreUrl(), replica.getStr(ZkStateReader.BASE_URL_PROP));
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
    public List<? extends BoundedSource<SolrDocument>> split(long desiredBundleSizeBytes,
        PipelineOptions options) throws Exception {
      List<BoundedSolrSource> sources = new ArrayList<>();
      int numShard;
      try (AuthorizedCloudSolrClient client = spec.getConnectionConfiguration().createClient()) {
        String collection = spec.getConnectionConfiguration().getCollection();
        DocCollection docCollection = client.getDocCollection(collection);
        numShard = docCollection.getSlices().size();
        for (Slice slice : docCollection.getSlices()) {
          sources.add(new BoundedSolrSource(spec, slice.getLeader()));
        }
      }
      checkArgument(sources.size() == numShard, "Not enough leaders were found");
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

    private long getEstimatedSizeOfShard(@Nonnull ReplicaInfo replica) throws IOException {
      try (AuthorizedSolrClient solrClient = spec.getConnectionConfiguration()
          .createClient(replica.baseUrl())) {
        CoreAdminRequest req = new CoreAdminRequest();
        req.setAction(CoreAdminParams.CoreAdminAction.STATUS);
        req.setIndexInfoNeeded(true);
        CoreAdminResponse response;
        try {
          response = (CoreAdminResponse) solrClient.process(req);
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
      try (AuthorizedCloudSolrClient solrClient = config.createClient()) {
        DocCollection docCollection = solrClient.getDocCollection(config.getCollection());
        for (Slice slice : docCollection.getSlices()) {
          Replica replica = slice.getLeader();
          sizeInBytes += getEstimatedSizeOfShard(ReplicaInfo.create(replica));
        }
      }
      return sizeInBytes;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
      if (replica != null) {
        builder.addIfNotNull(DisplayData.item("shardUrl", replica.coreUrl()));
      }
    }

    @Override
    public BoundedReader<SolrDocument> createReader(PipelineOptions options)
        throws IOException {
      return new BoundedSolrReader(this);
    }

    @Override
    public void validate() {
      spec.validate(null);
    }

    @Override
    public Coder<SolrDocument> getDefaultOutputCoder() {
      return SolrDocumentCoder.of();
    }
  }

  private static class BoundedSolrReader extends BoundedSource.BoundedReader<SolrDocument> {

    private final BoundedSolrSource source;

    private AuthorizedSolrClient solrClient;
    private SolrDocument current;
    private String cursorMark;
    private Iterator<SolrDocument> batchIterator;
    private boolean done;

    private BoundedSolrReader(BoundedSolrSource source) {
      this.source = source;
      this.cursorMark = CursorMarkParams.CURSOR_MARK_START;
    }

    @Override
    public boolean start() throws IOException {
      if (source.replica != null) {
        solrClient = source.spec.getConnectionConfiguration()
            .createClient(source.replica.coreUrl());
      } else {
        solrClient = source.spec.getConnectionConfiguration()
            .createClient();
      }

      SolrQuery solrParams = getQueryParams(source);
      try {
        QueryResponse response = solrClient.query(solrParams);
        updateCursorMark(response);
        return readNextBatchAndReturnFirstDocument(response);
      } catch (SolrServerException e) {
        throw new IOException(e);
      }
    }

    private SolrQuery getQueryParams(BoundedSolrSource source) {
      String query = source.spec.getQuery();
      if (query == null) {
        query = "*:*";
      }
      SolrQuery solrQuery = new SolrQuery(query);
      solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
      solrQuery.addSort("id", SolrQuery.ORDER.asc);
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
      if (batchIterator.hasNext()) {
        current = batchIterator.next();
        return true;
      } else {
        SolrQuery solrQuery = getQueryParams(source);
        try {
          QueryResponse response = solrClient.query(solrQuery);
          updateCursorMark(response);
          return readNextBatchAndReturnFirstDocument(response);
        } catch (SolrServerException e) {
          throw new IOException(e);
        }
      }
    }

    private boolean readNextBatchAndReturnFirstDocument(QueryResponse response) {
      if (response.getResults().isEmpty() || done) {
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

    abstract ConnectionConfiguration getConnectionConfiguration();
    abstract long getMaxBatchSize();
    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration connectionConfiguration);
      abstract Builder setMaxBatchSize(long maxBatchSize);
      abstract Write build();
    }

    /**
     * Provide a maximum size in number of documents for the batch. Depending on the
     * execution engine, size of bundles may vary, this sets the maximum size. Change this if you
     * need to have smaller batch.
     *
     * @param batchSize maximum batch size in number of documents
     * @return the {@link Write} with connection batch size set
     */
    public Write withMaxBatchSize(long batchSize) {
      checkArgument(batchSize > 0,
          "SolrIO.write()" + ".withMaxBatchSize(batchSize) called with incorrect <= 0 value");
      return builder().setMaxBatchSize(batchSize).build();
    }

    @Override
    public void validate(PipelineOptions options) {
      checkState(getConnectionConfiguration() != null,
          "SolrIO.write() requires a connection configuration"
              + " to be set via withConnectionConfiguration(configuration)");
    }

    @Override
    public PDone expand(PCollection<SolrInputDocument> input) {
      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    @VisibleForTesting
    static class WriteFn extends DoFn<SolrInputDocument, Void> {

      private final Write spec;

      private transient AuthorizedSolrClient solrClient;
      private Collection<SolrInputDocument> batch;

      WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void createClient() throws Exception {
        solrClient = spec.getConnectionConfiguration().createClient();
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

      private void flushBatch() throws IOException {
        if (batch.isEmpty()) {
          return;
        }
        try {
          UpdateRequest updateRequest = new UpdateRequest();
          updateRequest.add(batch);
          solrClient.process(updateRequest);
        } catch (SolrServerException e) {
          throw new IOException("Error writing to Solr", e);
        } finally {
          batch.clear();
        }
      }

      @Teardown
      public void closeClient() throws Exception {
        if (solrClient != null) {
          solrClient.close();
        }
      }
    }
  }
}
