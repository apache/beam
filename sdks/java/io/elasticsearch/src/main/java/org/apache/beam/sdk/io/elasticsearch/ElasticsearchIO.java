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
package org.apache.beam.sdk.io.elasticsearch;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

/**
 * Transforms for reading and writing data from/to Elasticsearch.
 * This IO is only compatible with Elasticsearch v2.x
 *
 * <h3>Reading from Elasticsearch</h3>
 *
 * <p>{@link ElasticsearchIO#read ElasticsearchIO.read()} returns a bounded
 * {@link PCollection PCollection&lt;String&gt;} representing JSON documents.
 *
 * <p>To configure the {@link ElasticsearchIO#read}, you have to provide a connection configuration
 * containing the HTTP address of the instances, an index name and a type. The following example
 * illustrates options for configuring the source:
 *
 * <pre>{@code
 *
 * pipeline.apply(ElasticsearchIO.read().withConnectionConfiguration(
 *    ElasticsearchIO.ConnectionConfiguration.create("http://host:9200", "my-index", "my-type")
 * )
 *
 * }</pre>
 *
 * <p>The connection configuration also accepts optional configuration: {@code withUsername()} and
 * {@code withPassword()}.
 *
 * <p>You can also specify a query on the {@code read()} using {@code withQuery()}.
 *
 * <h3>Writing to Elasticsearch</h3>
 *
 * <p>To write documents to Elasticsearch, use
 * {@link ElasticsearchIO#write ElasticsearchIO.write()}, which writes JSON documents from a
 * {@link PCollection PCollection&lt;String&gt;} (which can be bounded or unbounded).
 *
 * <p>To configure {@link ElasticsearchIO#write ElasticsearchIO.write()}, similar to the read, you
 * have to provide a connection configuration. For instance:
 *
 * <pre>{@code
 *
 *  pipeline
 *    .apply(...)
 *    .apply(ElasticsearchIO.write().withConnectionConfiguration(
 *       ElasticsearchIO.ConnectionConfiguration.create("http://host:9200", "my-index", "my-type")
 *    )
 *
 * }</pre>
 *
 * <p>Optionally, you can provide {@code withBatchSize()} and {@code withBatchSizeBytes()}
 * to specify the size of the write batch in number of documents or in bytes.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class ElasticsearchIO {

  private static void checkVersion(ConnectionConfiguration connectionConfiguration)
      throws IOException {
    RestClient restClient = connectionConfiguration.createClient();
    Response response = restClient.performRequest("GET", "", new BasicHeader("", ""));
    JsonNode jsonNode = parseResponse(response);
    String version = jsonNode.path("version").path("number").asText();
    boolean version2x = version.startsWith("2.");
    restClient.close();
    checkArgument(version2x, "The Elasticsearch version to connect to is different of 2.x. "
        + "This version of the ElasticsearchIO is only compatible with Elasticsearch v2.x");
  }

  public static Read read() {
    // default scrollKeepalive = 5m as a majorant for un-predictable time between 2 start/read calls
    // default batchSize to 100 as recommended by ES dev team as a safe value when dealing
    // with big documents and still a good compromise for performances
    return new AutoValue_ElasticsearchIO_Read.Builder()
        .setScrollKeepalive("5m")
        .setBatchSize(100L)
        .build();
  }

  public static Write write() {
    return new AutoValue_ElasticsearchIO_Write.Builder()
        // advised default starting batch size in ES docs
        .setMaxBatchSize(1000L)
        // advised default starting batch size in ES docs
        .setMaxBatchSizeBytes(5L * 1024L * 1024L)
        .build();
  }

  private ElasticsearchIO() {}

  private static final ObjectMapper mapper = new ObjectMapper();

  static JsonNode parseResponse(Response response) throws IOException {
    return mapper.readValue(response.getEntity().getContent(), JsonNode.class);
  }

  /** A POJO describing a connection configuration to Elasticsearch. */
  @AutoValue
  public abstract static class ConnectionConfiguration implements Serializable {

    abstract List<String> getAddresses();

    @Nullable
    abstract String getUsername();

    @Nullable
    abstract String getPassword();

    abstract String getIndex();

    abstract String getType();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setAddresses(List<String> addresses);

      abstract Builder setUsername(String username);

      abstract Builder setPassword(String password);

      abstract Builder setIndex(String index);

      abstract Builder setType(String type);

      abstract ConnectionConfiguration build();
    }

    /**
     * Creates a new Elasticsearch connection configuration.
     *
     * @param addresses list of addresses of Elasticsearch nodes
     * @param index the index toward which the requests will be issued
     * @param type the document type toward which the requests will be issued
     * @return the connection configuration object
     * @throws IOException when it fails to connect to Elasticsearch
     */
    public static ConnectionConfiguration create(String[] addresses, String index, String type)
        throws IOException {
      checkArgument(
          addresses != null,
          "ConnectionConfiguration.create(addresses, index, type) called with null address");
      checkArgument(
          addresses.length != 0,
          "ConnectionConfiguration.create(addresses, "
              + "index, type) called with empty addresses");
      checkArgument(
          index != null,
          "ConnectionConfiguration.create(addresses, index, type) called with null index");
      checkArgument(
          type != null,
          "ConnectionConfiguration.create(addresses, index, type) called with null type");
      ConnectionConfiguration connectionConfiguration =
          new AutoValue_ElasticsearchIO_ConnectionConfiguration.Builder()
              .setAddresses(Arrays.asList(addresses))
              .setIndex(index)
              .setType(type)
              .build();
      return connectionConfiguration;
    }

    /**
     * If Elasticsearch authentication is enabled, provide the username.
     *
     * @param username the username used to authenticate to Elasticsearch
     * @return the {@link ConnectionConfiguration} object with username set
     */
    public ConnectionConfiguration withUsername(String username) {
      checkArgument(
          username != null,
          "ConnectionConfiguration.create().withUsername(username) called with null username");
      checkArgument(
          !username.isEmpty(),
          "ConnectionConfiguration.create().withUsername(username) called with empty username");
      return builder().setUsername(username).build();
    }

    /**
     * If Elasticsearch authentication is enabled, provide the password.
     *
     * @param password the password used to authenticate to Elasticsearch
     * @return the {@link ConnectionConfiguration} object with password set
     */
    public ConnectionConfiguration withPassword(String password) {
      checkArgument(
          password != null,
          "ConnectionConfiguration.create().withPassword(password) called with null password");
      checkArgument(
          !password.isEmpty(),
          "ConnectionConfiguration.create().withPassword(password) called with empty password");
      return builder().setPassword(password).build();
    }

    private void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("address", getAddresses().toString()));
      builder.add(DisplayData.item("index", getIndex()));
      builder.add(DisplayData.item("type", getType()));
      builder.addIfNotNull(DisplayData.item("username", getUsername()));
    }

    RestClient createClient() throws MalformedURLException {
      HttpHost[] hosts = new HttpHost[getAddresses().size()];
      int i = 0;
      for (String address : getAddresses()) {
        URL url = new URL(address);
        hosts[i] = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
        i++;
      }
      RestClientBuilder restClientBuilder = RestClient.builder(hosts);
      if (getUsername() != null) {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
            AuthScope.ANY, new UsernamePasswordCredentials(getUsername(), getPassword()));
        restClientBuilder.setHttpClientConfigCallback(
            new RestClientBuilder.HttpClientConfigCallback() {
              public HttpAsyncClientBuilder customizeHttpClient(
                  HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
              }
            });
      }
      return restClientBuilder.build();
    }
  }

  /** A {@link PTransform} reading data from Elasticsearch. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {

    private static final long MAX_BATCH_SIZE = 10000L;

    @Nullable
    abstract ConnectionConfiguration getConnectionConfiguration();

    @Nullable
    abstract String getQuery();

    abstract String getScrollKeepalive();

    abstract long getBatchSize();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration connectionConfiguration);

      abstract Builder setQuery(String query);

      abstract Builder setScrollKeepalive(String scrollKeepalive);

      abstract Builder setBatchSize(long batchSize);

      abstract Read build();
    }

    /**
     * Provide the Elasticsearch connection configuration object.
     *
     * @param connectionConfiguration the Elasticsearch {@link ConnectionConfiguration} object
     * @return the {@link Read} with connection configuration set
     */
    public Read withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkArgument(
          connectionConfiguration != null,
          "ElasticsearchIO.read()"
              + ".withConnectionConfiguration(configuration) called with null configuration");
      return builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    /**
     * Provide a query used while reading from Elasticsearch.
     *
     * @param query the query. See <a
     *     href="https://www.elastic.co/guide/en/elasticsearch/reference/2.4/query-dsl.html">Query
     *     DSL</a>
     * @return the {@link Read} object with query set
     */
    public Read withQuery(String query) {
      checkArgument(
          !Strings.isNullOrEmpty(query),
          "ElasticsearchIO.read().withQuery(query) called" + " with null or empty query");
      return builder().setQuery(query).build();
    }

    /**
     * Provide a scroll keepalive. See <a
     * href="https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-request-scroll.html">scroll
     * API</a> Default is "5m". Change this only if you get "No search context found" errors.
     *
     * @param scrollKeepalive keepalive duration ex "5m" from 5 minutes
     * @return the {@link Read} with scroll keepalive set
     */
    public Read withScrollKeepalive(String scrollKeepalive) {
      checkArgument(
          scrollKeepalive != null && !scrollKeepalive.equals("0m"),
          "ElasticsearchIO.read().withScrollKeepalive(keepalive) called"
              + " with null or \"0m\" keepalive");
      return builder().setScrollKeepalive(scrollKeepalive).build();
    }

    /**
     * Provide a size for the scroll read. See <a
     * href="https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-request-scroll.html">
     * scroll API</a> Default is 100. Maximum is 10 000. If documents are small, increasing batch
     * size might improve read performance. If documents are big, you might need to decrease
     * batchSize
     *
     * @param batchSize number of documents read in each scroll read
     * @return the {@link Read} with batch size set
     */
    public Read withBatchSize(long batchSize) {
      checkArgument(
          batchSize > 0,
          "ElasticsearchIO.read().withBatchSize(batchSize) called with a negative "
              + "or equal to 0 value: %s",
          batchSize);
      checkArgument(
          batchSize <= MAX_BATCH_SIZE,
          "ElasticsearchIO.read().withBatchSize(batchSize) "
              + "called with a too large value (over %s): %s",
          MAX_BATCH_SIZE,
          batchSize);
      return builder().setBatchSize(batchSize).build();
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      return input.apply(
          org.apache.beam.sdk.io.Read.from(new BoundedElasticsearchSource(this, null)));
    }

    @Override
    public void validate(PipelineOptions options) {
      ConnectionConfiguration connectionConfiguration = getConnectionConfiguration();
      checkState(
          connectionConfiguration != null,
          "ElasticsearchIO.read() requires a connection configuration"
              + " to be set via withConnectionConfiguration(configuration)");
      try {
        checkVersion(connectionConfiguration);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("query", getQuery()));
      getConnectionConfiguration().populateDisplayData(builder);
    }
  }

  /** A {@link BoundedSource} reading from Elasticsearch. */
  @VisibleForTesting
  static class BoundedElasticsearchSource extends BoundedSource<String> {

    private final ElasticsearchIO.Read spec;
    // shardPreference is the shard number where the source will read the documents
    @Nullable private final String shardPreference;

    BoundedElasticsearchSource(Read spec, @Nullable String shardPreference) {
      this.spec = spec;
      this.shardPreference = shardPreference;
    }

    @Override
    public List<? extends BoundedSource<String>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      List<BoundedElasticsearchSource> sources = new ArrayList<>();

      // 1. We split per shard :
      // unfortunately, Elasticsearch 2. x doesn 't provide a way to do parallel reads on a single
      // shard.So we do not use desiredBundleSize because we cannot split shards.
      // With the slice API in ES 5.0 we will be able to use desiredBundleSize.
      // Basically we will just ask the slice API to return data
      // in nbBundles = estimatedSize / desiredBundleSize chuncks.
      // So each beam source will read around desiredBundleSize volume of data.

      // 2. Primary and replica shards have the same shard_id, we filter primary
      // to have one source for each shard_id. Even if we specify preference=shards:2,
      // ES load balances (round robin) the request between primary shard 2 and replica shard 2.
      // But, as each shard (replica or primary) is responsible for only one part of the data,
      // there will be no duplicate.

      JsonNode statsJson = getStats(true);
      JsonNode shardsJson =
          statsJson
              .path("indices")
              .path(spec.getConnectionConfiguration().getIndex())
              .path("shards");

      Iterator<Map.Entry<String, JsonNode>> shards = shardsJson.fields();
      while (shards.hasNext()) {
        Map.Entry<String, JsonNode> shardJson = shards.next();
        String shardId = shardJson.getKey();
        sources.add(new BoundedElasticsearchSource(spec, shardId));
      }
      checkArgument(!sources.isEmpty(), "No primary shard found");
      return sources;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws IOException {
      // we use indices stats API to estimate size and list the shards
      // (https://www.elastic.co/guide/en/elasticsearch/reference/2.4/indices-stats.html)
      // as Elasticsearch 2.x doesn't not support any way to do parallel read inside a shard
      // the estimated size bytes is not really used in the split into bundles.
      // However, we implement this method anyway as the runners can use it.
      // NB: Elasticsearch 5.x now provides the slice API.
      // (https://www.elastic.co/guide/en/elasticsearch/reference/5.0/search-request-scroll.html
      // #sliced-scroll)
      JsonNode statsJson = getStats(false);
      JsonNode indexStats =
          statsJson
              .path("indices")
              .path(spec.getConnectionConfiguration().getIndex())
              .path("primaries");
      JsonNode store = indexStats.path("store");
      return store.path("size_in_bytes").asLong();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("shard", shardPreference));
    }

    @Override
    public BoundedReader<String> createReader(PipelineOptions options) throws IOException {
      return new BoundedElasticsearchReader(this);
    }

    @Override
    public void validate() {
      spec.validate(null);
    }

    @Override
    public Coder<String> getDefaultOutputCoder() {
      return StringUtf8Coder.of();
    }

    private JsonNode getStats(boolean shardLevel) throws IOException {
      HashMap<String, String> params = new HashMap<>();
      if (shardLevel) {
        params.put("level", "shards");
      }
      String endpoint = String.format("/%s/_stats", spec.getConnectionConfiguration().getIndex());
      try (RestClient restClient = spec.getConnectionConfiguration().createClient()) {
        return parseResponse(
            restClient.performRequest("GET", endpoint, params, new BasicHeader("", "")));
      }
    }
  }

  private static class BoundedElasticsearchReader extends BoundedSource.BoundedReader<String> {

    private final BoundedElasticsearchSource source;

    private RestClient restClient;
    private String current;
    private String scrollId;
    private ListIterator<String> batchIterator;

    private BoundedElasticsearchReader(BoundedElasticsearchSource source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      restClient = source.spec.getConnectionConfiguration().createClient();

      String query = source.spec.getQuery();
      if (query == null) {
        query = "{ \"query\": { \"match_all\": {} } }";
      }

      Response response;
      String endPoint =
          String.format(
              "/%s/%s/_search",
              source.spec.getConnectionConfiguration().getIndex(),
              source.spec.getConnectionConfiguration().getType());
      Map<String, String> params = new HashMap<>();
      params.put("scroll", source.spec.getScrollKeepalive());
      params.put("size", String.valueOf(source.spec.getBatchSize()));
      if (source.shardPreference != null) {
        params.put("preference", "_shards:" + source.shardPreference);
      }
      HttpEntity queryEntity = new NStringEntity(query, ContentType.APPLICATION_JSON);
      response =
          restClient.performRequest("GET", endPoint, params, queryEntity, new BasicHeader("", ""));
      JsonNode searchResult = parseResponse(response);
      updateScrollId(searchResult);
      return readNextBatchAndReturnFirstDocument(searchResult);
    }

    private void updateScrollId(JsonNode searchResult) {
      scrollId = searchResult.path("_scroll_id").asText();
    }

    @Override
    public boolean advance() throws IOException {
      if (batchIterator.hasNext()) {
        current = batchIterator.next();
        return true;
      } else {
        String requestBody =
            String.format(
                "{\"scroll\" : \"%s\",\"scroll_id\" : \"%s\"}",
                source.spec.getScrollKeepalive(), scrollId);
        HttpEntity scrollEntity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
        Response response =
            restClient.performRequest(
                "GET",
                "/_search/scroll",
                Collections.<String, String>emptyMap(),
                scrollEntity,
                new BasicHeader("", ""));
        JsonNode searchResult = parseResponse(response);
        updateScrollId(searchResult);
        return readNextBatchAndReturnFirstDocument(searchResult);
      }
    }

    private boolean readNextBatchAndReturnFirstDocument(JsonNode searchResult) {
      //stop if no more data
      JsonNode hits = searchResult.path("hits").path("hits");
      if (hits.size() == 0) {
        current = null;
        batchIterator = null;
        return false;
      }
      // list behind iterator is empty
      List<String> batch = new ArrayList<>();
      for (JsonNode hit : hits) {
        String document = hit.path("_source").toString();
        batch.add(document);
      }
      batchIterator = batch.listIterator();
      current = batchIterator.next();
      return true;
    }

    @Override
    public String getCurrent() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override
    public void close() throws IOException {
      // remove the scroll
      String requestBody = String.format("{\"scroll_id\" : [\"%s\"]}", scrollId);
      HttpEntity entity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
      try {
        restClient.performRequest(
            "DELETE",
            "/_search/scroll",
            Collections.<String, String>emptyMap(),
            entity,
            new BasicHeader("", ""));
      } finally {
        if (restClient != null) {
          restClient.close();
        }
      }
    }

    @Override
    public BoundedSource<String> getCurrentSource() {
      return source;
    }
  }

  /** A {@link PTransform} writing data to Elasticsearch. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<String>, PDone> {

    @Nullable
    abstract ConnectionConfiguration getConnectionConfiguration();

    abstract long getMaxBatchSize();

    abstract long getMaxBatchSizeBytes();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration connectionConfiguration);

      abstract Builder setMaxBatchSize(long maxBatchSize);

      abstract Builder setMaxBatchSizeBytes(long maxBatchSizeBytes);

      abstract Write build();
    }

    /**
     * Provide the Elasticsearch connection configuration object.
     *
     * @param connectionConfiguration the Elasticsearch {@link ConnectionConfiguration} object
     * @return the {@link Write} with connection configuration set
     */
    public Write withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkArgument(
          connectionConfiguration != null,
          "ElasticsearchIO.write()"
              + ".withConnectionConfiguration(configuration) called with null configuration");
      return builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    /**
     * Provide a maximum size in number of documents for the batch see bulk API
     * (https://www.elastic.co/guide/en/elasticsearch/reference/2.4/docs-bulk.html). Default is 1000
     * docs (like Elasticsearch bulk size advice). See
     * https://www.elastic.co/guide/en/elasticsearch/guide/current/bulk.html Depending on the
     * execution engine, size of bundles may vary, this sets the maximum size. Change this if you
     * need to have smaller ElasticSearch bulks.
     *
     * @param batchSize maximum batch size in number of documents
     * @return the {@link Write} with connection batch size set
     */
    public Write withMaxBatchSize(long batchSize) {
      checkArgument(
          batchSize > 0,
          "ElasticsearchIO.write()"
              + ".withMaxBatchSize(batchSize) called with incorrect <= 0 value");
      return builder().setMaxBatchSize(batchSize).build();
    }

    /**
     * Provide a maximum size in bytes for the batch see bulk API
     * (https://www.elastic.co/guide/en/elasticsearch/reference/2.4/docs-bulk.html). Default is 5MB
     * (like Elasticsearch bulk size advice). See
     * https://www.elastic.co/guide/en/elasticsearch/guide/current/bulk.html Depending on the
     * execution engine, size of bundles may vary, this sets the maximum size. Change this if you
     * need to have smaller ElasticSearch bulks.
     *
     * @param batchSizeBytes maximum batch size in bytes
     * @return the {@link Write} with connection batch size in bytes set
     */
    public Write withMaxBatchSizeBytes(long batchSizeBytes) {
      checkArgument(
          batchSizeBytes > 0,
          "ElasticsearchIO.write()"
              + ".withMaxBatchSizeBytes(batchSizeBytes) called with incorrect <= 0 value");
      return builder().setMaxBatchSizeBytes(batchSizeBytes).build();
    }

    @Override
    public void validate(PipelineOptions options) {
      ConnectionConfiguration connectionConfiguration = getConnectionConfiguration();
      checkState(
          connectionConfiguration != null,
          "ElasticsearchIO.write() requires a connection configuration"
              + " to be set via withConnectionConfiguration(configuration)");
      try {
        checkVersion(connectionConfiguration);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }

    @Override
    public PDone expand(PCollection<String> input) {
      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    @VisibleForTesting
    static class WriteFn extends DoFn<String, Void> {

      private final Write spec;

      private transient RestClient restClient;
      private ArrayList<String> batch;
      private long currentBatchSizeBytes;

      WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void createClient() throws Exception {
        restClient = spec.getConnectionConfiguration().createClient();
      }

      @StartBundle
      public void startBundle(StartBundleContext context) throws Exception {
        batch = new ArrayList<>();
        currentBatchSizeBytes = 0;
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        String document = context.element();
        batch.add(String.format("{ \"index\" : {} }%n%s%n", document));
        currentBatchSizeBytes += document.getBytes().length;
        if (batch.size() >= spec.getMaxBatchSize()
            || currentBatchSizeBytes >= spec.getMaxBatchSizeBytes()) {
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
        StringBuilder bulkRequest = new StringBuilder();
        for (String json : batch) {
          bulkRequest.append(json);
        }
        batch.clear();
        currentBatchSizeBytes = 0;
        Response response;
        String endPoint =
            String.format(
                "/%s/%s/_bulk",
                spec.getConnectionConfiguration().getIndex(),
                spec.getConnectionConfiguration().getType());
        HttpEntity requestBody =
            new NStringEntity(bulkRequest.toString(), ContentType.APPLICATION_JSON);
        response =
            restClient.performRequest(
                "POST",
                endPoint,
                Collections.<String, String>emptyMap(),
                requestBody,
                new BasicHeader("", ""));
        JsonNode searchResult = parseResponse(response);
        boolean errors = searchResult.path("errors").asBoolean();
        if (errors) {
          StringBuilder errorMessages =
              new StringBuilder(
                  "Error writing to Elasticsearch, some elements could not be inserted:");
          JsonNode items = searchResult.path("items");
          //some items present in bulk might have errors, concatenate error messages
          for (JsonNode item : items) {
            JsonNode creationObject = item.path("create");
            JsonNode error = creationObject.get("error");
            if (error != null) {
              String type = error.path("type").asText();
              String reason = error.path("reason").asText();
              String docId = creationObject.path("_id").asText();
              errorMessages.append(String.format("%nDocument id %s: %s (%s)", docId, reason, type));
              JsonNode causedBy = error.get("caused_by");
              if (causedBy != null) {
                String cbReason = causedBy.path("reason").asText();
                String cbType = causedBy.path("type").asText();
                errorMessages.append(String.format("%nCaused by: %s (%s)", cbReason, cbType));
              }
            }
          }
          throw new IOException(errorMessages.toString());
        }
      }

      @Teardown
      public void closeClient() throws Exception {
        if (restClient != null) {
          restClient.close();
        }
      }
    }
  }
}
