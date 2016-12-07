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

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import javax.annotation.Nullable;

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
 * IO to read and write data on Elasticsearch.
 *
 * <h3>Reading from Elasticsearch</h3>
 *
 * <p>ElasticsearchIO source returns a bounded collection of String representing JSON document
 * as {@code PCollection<String>}.
 *
 * <p>To configure the Elasticsearch source, you have to provide a connection configuration
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
 * <p>ElasticsearchIO supports sink to write documents (as JSON String).
 *
 * <p>To configure Elasticsearch sink, similar to the read, you have to provide a connection
 * configuration. For instance:
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
 * <p>Optionally, you can provide {@code withBatchSize()} and {@code withBatchSizeMegaBytes()}
 * to specify the size of the write batch in number of documents or in megabytes.
 */
public class ElasticsearchIO {

  public static Read read() {
    return new AutoValue_ElasticsearchIO_Read.Builder().setScrollKeepalive("1m").build();
  }

  public static Write write() {
    return new AutoValue_ElasticsearchIO_Write.Builder()
        .setBatchSize(1000L)
        .setBatchSizeMegaBytes(5)
        .build();
  }

  private ElasticsearchIO() {}

  private static JsonObject parseResponse(Response response) throws IOException {
    InputStream content = response.getEntity().getContent();
    InputStreamReader inputStreamReader = new InputStreamReader(content, "UTF-8");
    JsonObject jsonObject = new Gson().fromJson(inputStreamReader, JsonObject.class);
    return jsonObject;
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
     * Create Elasticsearch connection configuration.
     *
     * @param addresses list of addresses of Elasticsearch nodes
     * @param index the index toward which the requests will be issued
     * @param type the document type toward which the requests will be issued
     * @return the connection configuration object
     */
    public static ConnectionConfiguration create(String[] addresses, String index, String type) {
      checkArgument(
          addresses != null,
          "ConnectionConfiguration.create(addresses, index, type) " + "called with null address");
      checkArgument(
          addresses.length != 0,
          "ConnectionConfiguration.create(addresses, "
              + "index, type) called with empty addresses");
      checkArgument(
          index != null,
          "ConnectionConfiguration.create(addresses, index, type) called" + " with null index");
      checkArgument(
          type != null,
          "ConnectionConfiguration.create(addresses, index, type) called " + "with null type");
      return new AutoValue_ElasticsearchIO_ConnectionConfiguration.Builder()
          .setAddresses(Arrays.asList(addresses))
          .setIndex(index)
          .setType(type)
          .build();
    }

    /**
     * If Elasticsearch authentication is enabled, provide the username.
     *
     * @param username the username used to authenticate to Elasticsearch
     * @return the {@link ConnectionConfiguration} object with username set
     */
    public ConnectionConfiguration withUsername(String username) {
      return builder().setUsername(username).build();
    }

    /**
     * If Elasticsearch authentication is enabled, provide the password.
     *
     * @param password the password used to authenticate to Elasticsearch
     * @return the {@link ConnectionConfiguration} object with password set
     */
    public ConnectionConfiguration withPassword(String password) {
      return builder().setPassword(password).build();
    }

    private void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("address", getAddresses().toString()));
      builder.add(DisplayData.item("index", getIndex()));
      builder.add(DisplayData.item("type", getType()));
      builder.addIfNotNull(DisplayData.item("username", getUsername()));
    }

    private RestClient createClient() throws MalformedURLException {
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

    @Nullable
    abstract ConnectionConfiguration getConnectionConfiguration();

    @Nullable
    abstract String getQuery();

    @Nullable
    abstract String getScrollKeepalive();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration connectionConfiguration);

      abstract Builder setQuery(String query);

      abstract Builder setScrollKeepalive(String scrollKeepalive);

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
          query != null, "ElasticsearchIO.read().withQuery(query) called with null " + "query");
      return builder().setQuery(query).build();
    }

    /**
     * Provide a scroll keepalive. See <a
     * href="https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-request-scroll.html">scroll
     * API</a> Default is "1m"
     *
     * @param scrollKeepalive keepalive duration ex "5m" from 5 minutes
     * @return the {@link ElasticsearchIO.Read} with scroll keepalive set
     */
    public Read withScrollKeepalive(String scrollKeepalive) {
      checkArgument(
          scrollKeepalive != null,
          "ElasticsearchIO.read().withScrollKeepalive" + "(keepalive) called with null keepalive");
      return builder().setScrollKeepalive(scrollKeepalive).build();
    }

    @Override
    public PCollection<String> apply(PBegin input) {
      return input.apply(
          org.apache.beam.sdk.io.Read.from(new BoundedElasticsearchSource(this, null)));
    }

    @Override
    public void validate(PBegin input) {
      checkState(
          getConnectionConfiguration() != null,
          "ElasticsearchIO.read() requires a connection configuration"
              + " to be set via withConnectionConfiguration(configuration)");
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

    BoundedElasticsearchSource(Read spec, String shardPreference) {
      this.spec = spec;
      this.shardPreference = shardPreference;
    }

    @Override
    public List<? extends BoundedSource<String>> splitIntoBundles(
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

      JsonObject statsJson = getStats(true);
      JsonObject shardsJson =
          statsJson
              .getAsJsonObject("indices")
              .getAsJsonObject(spec.getConnectionConfiguration().getIndex())
              .getAsJsonObject("shards");
      Set<Map.Entry<String, JsonElement>> shards = shardsJson.entrySet();
      for (Map.Entry<String, JsonElement> shardJson : shards) {
        String shardId = shardJson.getKey();
        JsonArray value = (JsonArray) shardJson.getValue();
        boolean isPrimaryShard =
            value
                .get(0)
                .getAsJsonObject()
                .getAsJsonObject("routing")
                .getAsJsonPrimitive("primary")
                .getAsBoolean();
        if (isPrimaryShard) {
          sources.add(new BoundedElasticsearchSource(spec, shardId));
        }
      }
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
      JsonObject statsJson = getStats(false);
      JsonObject indexStats =
          statsJson
              .getAsJsonObject("indices")
              .getAsJsonObject(spec.getConnectionConfiguration().getIndex())
              .getAsJsonObject("primaries");
      JsonObject store = indexStats.getAsJsonObject("store");
      return store.getAsJsonPrimitive("size_in_bytes").getAsLong();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("shard", shardPreference));
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      return false;
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

    private JsonObject getStats(boolean shardLevel) throws IOException {
      RestClient restClient = spec.getConnectionConfiguration().createClient();
      HashMap<String, String> params = new HashMap<>();
      if (shardLevel) {
        params.put("level", "shards");
      }
      String endpoint = String.format("/%s/_stats", spec.getConnectionConfiguration().getIndex());
      Response response;
      try {
        response = restClient.performRequest("GET", endpoint, params, new BasicHeader("", ""));
      } finally {
        restClient.close();
      }
      return parseResponse(response);
    }
  }

  private static class BoundedElasticsearchReader extends BoundedSource.BoundedReader<String> {

    private final BoundedElasticsearchSource source;

    private RestClient restClient;
    private String current;
    private String scrollId;

    public BoundedElasticsearchReader(BoundedElasticsearchSource source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      restClient = source.spec.getConnectionConfiguration().createClient();

      String query = source.spec.getQuery();
      if (query == null) {
        query = "{\n" + "  \"query\": {\n" + "    \"match_all\": {}\n" + "  }\n" + "}";
      }

      Response response;
      String endPoint =
          String.format(
              "/%s/%s/_search",
              source.spec.getConnectionConfiguration().getIndex(),
              source.spec.getConnectionConfiguration().getType());
      Map<String, String> params = new HashMap<>();
      params.put("scroll", source.spec.getScrollKeepalive());
      params.put("size", "1");
      if (source.shardPreference != null) {
        params.put("preference", "_shards:" + source.shardPreference);
      }
      HttpEntity queryEntity = new NStringEntity(query, ContentType.APPLICATION_JSON);
      response =
          restClient.performRequest("GET", endPoint, params, queryEntity, new BasicHeader("", ""));
      JsonObject searchResult = parseResponse(response);
      updateScrollId(searchResult);
      return updateCurrent(searchResult);
    }

    private void updateScrollId(JsonObject searchResult) {
      scrollId = searchResult.getAsJsonPrimitive("_scroll_id").getAsString();
    }

    @Override
    public boolean advance() throws IOException {
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
      JsonObject searchResult = parseResponse(response);
      updateScrollId(searchResult);
      return updateCurrent(searchResult);
    }

    private boolean updateCurrent(JsonObject searchResult) {
      //stop if no more data
      if (searchResult.getAsJsonObject("hits").getAsJsonArray("hits").size() == 0) {
        current = null;
        return false;
      }
      current =
          searchResult
              .getAsJsonObject("hits")
              .getAsJsonArray("hits")
              .get(0)
              .getAsJsonObject()
              .getAsJsonObject("_source")
              .toString();
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

    abstract long getBatchSize();

    abstract int getBatchSizeMegaBytes();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration connectionConfiguration);

      abstract Builder setBatchSize(long batchSize);

      abstract Builder setBatchSizeMegaBytes(int batchSizeMegaBytes);

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
     * Provide a size in number of documents for the batch see bulk API
     * (https://www.elastic.co/guide/en/elasticsearch/reference/2.4/docs-bulk.html). Default is 1000
     * docs
     * @param batchSize batch size in number of documents
     * @return the {@link Write} with connection batch size set
     */
    public Write withBatchSize(long batchSize) {
      return builder().setBatchSize(batchSize).build();
    }

    /**
     * Provide a size in megabytes for the batch see bulk API
     * (https://www.elastic.co/guide/en/elasticsearch/reference/2.4/docs-bulk.html). Default is 5MB
     *
     * @param batchSizeMegaBytes batch size in megabytes
     * @return the {@link Write} with connection batch size in megabytes set
     */
    public Write withBatchSizeMegaBytes(int batchSizeMegaBytes) {
      return builder().setBatchSizeMegaBytes(batchSizeMegaBytes).build();
    }

    @Override
    public void validate(PCollection<String> input) {
      checkState(
          getConnectionConfiguration() != null,
          "ElasticsearchIO.write() requires a connection configuration"
              + " to be set via withConnectionConfiguration(configuration)");
    }

    @Override
    public PDone apply(PCollection<String> input) {
      input.apply(ParDo.of(new WriterFn(this)));
      return PDone.in(input.getPipeline());
    }

    private static class WriterFn extends DoFn<String, Void> {

      private final Write spec;

      private transient RestClient restClient;
      private ArrayList<String> batch;
      private long currentBatchSizeBytes;

      public WriterFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void createClient() throws Exception {
        restClient = spec.getConnectionConfiguration().createClient();
      }

      @StartBundle
      public void startBundle(Context context) throws Exception {
        batch = new ArrayList<>();
        currentBatchSizeBytes = 0;
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        String document = context.element();
        batch.add(String.format("{ \"index\" : {} }%n%s%n", document));
        currentBatchSizeBytes += document.getBytes().length;
        if (batch.size() >= spec.getBatchSize()
            || currentBatchSizeBytes >= (spec.getBatchSizeMegaBytes() * 1024L * 1024L)) {
          finishBundle(context);
        }
      }

      @FinishBundle
      public void finishBundle(Context context) throws Exception {
        if (batch.isEmpty()) {
          return;
        }
        StringBuilder bulkRequest = new StringBuilder();
        for (String json : batch) {
          bulkRequest.append(json);
        }
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
        JsonObject searchResult = parseResponse(response);
        boolean errors = searchResult.getAsJsonPrimitive("errors").getAsBoolean();
        if (errors) {
          StringBuilder errorMessages =
              new StringBuilder(
                  "Error writing to Elasticsearch, " + "some elements could not be inserted:");
          JsonArray items = searchResult.getAsJsonArray("items");
          //some items present in bulk might have errors, concatenate error messages
          for (JsonElement item : items) {
            JsonObject creationObject = item.getAsJsonObject().getAsJsonObject("create");
            JsonObject error = creationObject.getAsJsonObject("error");
            if (error != null) {
              String docId = creationObject.getAsJsonPrimitive("_id").getAsString();
              errorMessages.append(String.format("%n document id %s : ", docId));
              String errorMessage =
                  error.getAsJsonObject("caused_by").getAsJsonPrimitive("reason").getAsString();
              errorMessages.append(errorMessage);
            }
          }
          throw new IOException(errorMessages.toString());
        }
        batch.clear();
        currentBatchSizeBytes = 0;
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
