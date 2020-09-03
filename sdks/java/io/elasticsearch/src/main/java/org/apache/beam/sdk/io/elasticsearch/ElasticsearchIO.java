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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.auto.value.AutoValue;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.ssl.SSLContexts;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms for reading and writing data from/to Elasticsearch.
 *
 * <h3>Reading from Elasticsearch</h3>
 *
 * <p>{@link ElasticsearchIO#read ElasticsearchIO.read()} returns a bounded {@link PCollection
 * PCollection&lt;String&gt;} representing JSON documents.
 *
 * <p>To configure the {@link ElasticsearchIO#read}, you have to provide a connection configuration
 * containing the HTTP address of the instances, an index name and a type. The following example
 * illustrates options for configuring the source:
 *
 * <pre>{@code
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
 * <p>To write documents to Elasticsearch, use {@link ElasticsearchIO#write
 * ElasticsearchIO.write()}, which writes JSON documents from a {@link PCollection
 * PCollection&lt;String&gt;} (which can be bounded or unbounded).
 *
 * <p>To configure {@link ElasticsearchIO#write ElasticsearchIO.write()}, similar to the read, you
 * have to provide a connection configuration. For instance:
 *
 * <pre>{@code
 * pipeline
 *   .apply(...)
 *   .apply(ElasticsearchIO.write().withConnectionConfiguration(
 *      ElasticsearchIO.ConnectionConfiguration.create("http://host:9200", "my-index", "my-type")
 *   )
 *
 * }</pre>
 *
 * <p>Optionally, you can provide {@code withBatchSize()} and {@code withBatchSizeBytes()} to
 * specify the size of the write batch in number of documents or in bytes.
 *
 * <p>Optionally, you can provide an {@link ElasticsearchIO.Write.FieldValueExtractFn} using {@code
 * withIdFn()} that will be run to extract the id value out of the provided document rather than
 * using the document id auto-generated by Elasticsearch.
 *
 * <p>Optionally, you can provide {@link ElasticsearchIO.Write.FieldValueExtractFn} using {@code
 * withIndexFn()} or {@code withTypeFn()} to enable per-document routing to the target Elasticsearch
 * index (all versions) and type (version &gt; 6). Support for type routing was removed in
 * Elasticsearch 6 (see
 * https://www.elastic.co/blog/index-type-parent-child-join-now-future-in-elasticsearch)
 *
 * <p>When {withUsePartialUpdate()} is enabled, the input document must contain an id field and
 * {@code withIdFn()} must be used to allow its extraction by the ElasticsearchIO.
 *
 * <p>Optionally, {@code withSocketTimeout()} can be used to override the default retry timeout and
 * socket timeout of 30000ms. {@code withConnectTimeout()} can be used to override the default
 * connect timeout of 1000ms.
 */
@Experimental(Kind.SOURCE_SINK)
public class ElasticsearchIO {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchIO.class);

  public static Read read() {
    // default scrollKeepalive = 5m as a majorant for un-predictable time between 2 start/read calls
    // default batchSize to 100 as recommended by ES dev team as a safe value when dealing
    // with big documents and still a good compromise for performances
    return new AutoValue_ElasticsearchIO_Read.Builder()
        .setWithMetadata(false)
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
        .setUsePartialUpdate(false) // default is document upsert
        .build();
  }

  private ElasticsearchIO() {}

  private static final ObjectMapper mapper = new ObjectMapper();

  @VisibleForTesting
  static JsonNode parseResponse(HttpEntity responseEntity) throws IOException {
    return mapper.readValue(responseEntity.getContent(), JsonNode.class);
  }

  static void checkForErrors(HttpEntity responseEntity, int backendVersion, boolean partialUpdate)
      throws IOException {
    JsonNode searchResult = parseResponse(responseEntity);
    boolean errors = searchResult.path("errors").asBoolean();
    if (errors) {
      StringBuilder errorMessages =
          new StringBuilder("Error writing to Elasticsearch, some elements could not be inserted:");
      JsonNode items = searchResult.path("items");
      // some items present in bulk might have errors, concatenate error messages
      for (JsonNode item : items) {

        String errorRootName = "";
        // when use partial update, the response items includes all the update.
        if (partialUpdate) {
          errorRootName = "update";
        } else {
          if (backendVersion == 2) {
            errorRootName = "create";
          } else if (backendVersion >= 5) {
            errorRootName = "index";
          }
        }
        JsonNode errorRoot = item.path(errorRootName);
        JsonNode error = errorRoot.get("error");
        if (error != null) {
          String type = error.path("type").asText();
          String reason = error.path("reason").asText();
          String docId = errorRoot.path("_id").asText();
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

  /** A POJO describing a connection configuration to Elasticsearch. */
  @AutoValue
  public abstract static class ConnectionConfiguration implements Serializable {

    public abstract List<String> getAddresses();

    public abstract @Nullable String getUsername();

    public abstract @Nullable String getPassword();

    public abstract @Nullable String getKeystorePath();

    public abstract @Nullable String getKeystorePassword();

    public abstract String getIndex();

    public abstract String getType();

    public abstract @Nullable Integer getSocketTimeout();

    public abstract @Nullable Integer getConnectTimeout();

    public abstract boolean isTrustSelfSignedCerts();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setAddresses(List<String> addresses);

      abstract Builder setUsername(String username);

      abstract Builder setPassword(String password);

      abstract Builder setKeystorePath(String keystorePath);

      abstract Builder setKeystorePassword(String password);

      abstract Builder setIndex(String index);

      abstract Builder setType(String type);

      abstract Builder setSocketTimeout(Integer maxRetryTimeout);

      abstract Builder setConnectTimeout(Integer connectTimeout);

      abstract Builder setTrustSelfSignedCerts(boolean trustSelfSignedCerts);

      abstract ConnectionConfiguration build();
    }

    /**
     * Creates a new Elasticsearch connection configuration.
     *
     * @param addresses list of addresses of Elasticsearch nodes
     * @param index the index toward which the requests will be issued
     * @param type the document type toward which the requests will be issued
     * @return the connection configuration object
     */
    public static ConnectionConfiguration create(String[] addresses, String index, String type) {
      checkArgument(addresses != null, "addresses can not be null");
      checkArgument(addresses.length > 0, "addresses can not be empty");
      checkArgument(index != null, "index can not be null");
      checkArgument(type != null, "type can not be null");
      return new AutoValue_ElasticsearchIO_ConnectionConfiguration.Builder()
          .setAddresses(Arrays.asList(addresses))
          .setIndex(index)
          .setType(type)
          .setTrustSelfSignedCerts(false)
          .build();
    }

    /**
     * If Elasticsearch authentication is enabled, provide the username.
     *
     * @param username the username used to authenticate to Elasticsearch
     * @return a {@link ConnectionConfiguration} describes a connection configuration to
     *     Elasticsearch.
     */
    public ConnectionConfiguration withUsername(String username) {
      checkArgument(username != null, "username can not be null");
      checkArgument(!username.isEmpty(), "username can not be empty");
      return builder().setUsername(username).build();
    }

    /**
     * If Elasticsearch authentication is enabled, provide the password.
     *
     * @param password the password used to authenticate to Elasticsearch
     * @return a {@link ConnectionConfiguration} describes a connection configuration to
     *     Elasticsearch.
     */
    public ConnectionConfiguration withPassword(String password) {
      checkArgument(password != null, "password can not be null");
      checkArgument(!password.isEmpty(), "password can not be empty");
      return builder().setPassword(password).build();
    }

    /**
     * If Elasticsearch uses SSL/TLS with mutual authentication (via shield), provide the keystore
     * containing the client key.
     *
     * @param keystorePath the location of the keystore containing the client key.
     * @return a {@link ConnectionConfiguration} describes a connection configuration to
     *     Elasticsearch.
     */
    public ConnectionConfiguration withKeystorePath(String keystorePath) {
      checkArgument(keystorePath != null, "keystorePath can not be null");
      checkArgument(!keystorePath.isEmpty(), "keystorePath can not be empty");
      return builder().setKeystorePath(keystorePath).build();
    }

    /**
     * If Elasticsearch uses SSL/TLS with mutual authentication (via shield), provide the password
     * to open the client keystore.
     *
     * @param keystorePassword the password of the client keystore.
     * @return a {@link ConnectionConfiguration} describes a connection configuration to
     *     Elasticsearch.
     */
    public ConnectionConfiguration withKeystorePassword(String keystorePassword) {
      checkArgument(keystorePassword != null, "keystorePassword can not be null");
      return builder().setKeystorePassword(keystorePassword).build();
    }

    /**
     * If Elasticsearch uses SSL/TLS then configure whether to trust self signed certs or not. The
     * default is false.
     *
     * @param trustSelfSignedCerts Whether to trust self signed certs
     * @return a {@link ConnectionConfiguration} describes a connection configuration to
     *     Elasticsearch.
     */
    public ConnectionConfiguration withTrustSelfSignedCerts(boolean trustSelfSignedCerts) {
      return builder().setTrustSelfSignedCerts(trustSelfSignedCerts).build();
    }

    /**
     * If set, overwrites the default max retry timeout (30000ms) in the Elastic {@link RestClient}
     * and the default socket timeout (30000ms) in the {@link RequestConfig} of the Elastic {@link
     * RestClient}.
     *
     * @param socketTimeout the socket and retry timeout in millis.
     * @return a {@link ConnectionConfiguration} describes a connection configuration to
     *     Elasticsearch.
     */
    public ConnectionConfiguration withSocketTimeout(Integer socketTimeout) {
      checkArgument(socketTimeout != null, "socketTimeout can not be null");
      return builder().setSocketTimeout(socketTimeout).build();
    }

    /**
     * If set, overwrites the default connect timeout (1000ms) in the {@link RequestConfig} of the
     * Elastic {@link RestClient}.
     *
     * @param connectTimeout the socket and retry timeout in millis.
     * @return a {@link ConnectionConfiguration} describes a connection configuration to
     *     Elasticsearch.
     */
    public ConnectionConfiguration withConnectTimeout(Integer connectTimeout) {
      checkArgument(connectTimeout != null, "connectTimeout can not be null");
      return builder().setConnectTimeout(connectTimeout).build();
    }

    private void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("address", getAddresses().toString()));
      builder.add(DisplayData.item("index", getIndex()));
      builder.add(DisplayData.item("type", getType()));
      builder.addIfNotNull(DisplayData.item("username", getUsername()));
      builder.addIfNotNull(DisplayData.item("keystore.path", getKeystorePath()));
      builder.addIfNotNull(DisplayData.item("socketTimeout", getSocketTimeout()));
      builder.addIfNotNull(DisplayData.item("connectTimeout", getConnectTimeout()));
      builder.addIfNotNull(DisplayData.item("trustSelfSignedCerts", isTrustSelfSignedCerts()));
    }

    @VisibleForTesting
    RestClient createClient() throws IOException {
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
            httpAsyncClientBuilder ->
                httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
      }
      if (getKeystorePath() != null && !getKeystorePath().isEmpty()) {
        try {
          KeyStore keyStore = KeyStore.getInstance("jks");
          try (InputStream is = new FileInputStream(new File(getKeystorePath()))) {
            String keystorePassword = getKeystorePassword();
            keyStore.load(is, (keystorePassword == null) ? null : keystorePassword.toCharArray());
          }
          final TrustStrategy trustStrategy =
              isTrustSelfSignedCerts() ? new TrustSelfSignedStrategy() : null;
          final SSLContext sslContext =
              SSLContexts.custom().loadTrustMaterial(keyStore, trustStrategy).build();
          final SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(sslContext);
          restClientBuilder.setHttpClientConfigCallback(
              httpClientBuilder ->
                  httpClientBuilder.setSSLContext(sslContext).setSSLStrategy(sessionStrategy));
        } catch (Exception e) {
          throw new IOException("Can't load the client certificate from the keystore", e);
        }
      }
      restClientBuilder.setRequestConfigCallback(
          new RestClientBuilder.RequestConfigCallback() {
            @Override
            public RequestConfig.Builder customizeRequestConfig(
                RequestConfig.Builder requestConfigBuilder) {
              if (getConnectTimeout() != null) {
                requestConfigBuilder.setConnectTimeout(getConnectTimeout());
              }
              if (getSocketTimeout() != null) {
                requestConfigBuilder.setSocketTimeout(getSocketTimeout());
              }
              return requestConfigBuilder;
            }
          });
      return restClientBuilder.build();
    }
  }

  /** A {@link PTransform} reading data from Elasticsearch. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {

    private static final long MAX_BATCH_SIZE = 10000L;

    abstract @Nullable ConnectionConfiguration getConnectionConfiguration();

    abstract @Nullable ValueProvider<String> getQuery();

    abstract boolean isWithMetadata();

    abstract String getScrollKeepalive();

    abstract long getBatchSize();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration connectionConfiguration);

      abstract Builder setQuery(ValueProvider<String> query);

      abstract Builder setWithMetadata(boolean withMetadata);

      abstract Builder setScrollKeepalive(String scrollKeepalive);

      abstract Builder setBatchSize(long batchSize);

      abstract Read build();
    }

    /**
     * Provide the Elasticsearch connection configuration object.
     *
     * @param connectionConfiguration a {@link ConnectionConfiguration} describes a connection
     *     configuration to Elasticsearch.
     * @return a {@link PTransform} reading data from Elasticsearch.
     */
    public Read withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkArgument(connectionConfiguration != null, "connectionConfiguration can not be null");
      return builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    /**
     * Provide a query used while reading from Elasticsearch.
     *
     * @param query the query. See <a
     *     href="https://www.elastic.co/guide/en/elasticsearch/reference/2.4/query-dsl.html">Query
     *     DSL</a>
     * @return a {@link PTransform} reading data from Elasticsearch.
     */
    public Read withQuery(String query) {
      checkArgument(query != null, "query can not be null");
      checkArgument(!query.isEmpty(), "query can not be empty");
      return withQuery(ValueProvider.StaticValueProvider.of(query));
    }

    /**
     * Provide a {@link ValueProvider} that provides the query used while reading from
     * Elasticsearch. This is useful for cases when the query must be dynamic.
     *
     * @param query the query. See <a
     *     href="https://www.elastic.co/guide/en/elasticsearch/reference/2.4/query-dsl.html">Query
     *     DSL</a>
     * @return a {@link PTransform} reading data from Elasticsearch.
     */
    public Read withQuery(ValueProvider<String> query) {
      checkArgument(query != null, "query can not be null");
      return builder().setQuery(query).build();
    }

    /**
     * Include metadata in result json documents. Document source will be under json node _source.
     *
     * @return a {@link PTransform} reading data from Elasticsearch.
     */
    public Read withMetadata() {
      return builder().setWithMetadata(true).build();
    }

    /**
     * Provide a scroll keepalive. See <a
     * href="https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-request-scroll.html">scroll
     * API</a> Default is "5m". Change this only if you get "No search context found" errors.
     *
     * @param scrollKeepalive keepalive duration of the scroll
     * @return a {@link PTransform} reading data from Elasticsearch.
     */
    public Read withScrollKeepalive(String scrollKeepalive) {
      checkArgument(scrollKeepalive != null, "scrollKeepalive can not be null");
      checkArgument(!"0m".equals(scrollKeepalive), "scrollKeepalive can not be 0m");
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
     * @return a {@link PTransform} reading data from Elasticsearch.
     */
    public Read withBatchSize(long batchSize) {
      checkArgument(
          batchSize > 0 && batchSize <= MAX_BATCH_SIZE,
          "batchSize must be > 0 and <= %s, but was: %s",
          MAX_BATCH_SIZE,
          batchSize);
      return builder().setBatchSize(batchSize).build();
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      ConnectionConfiguration connectionConfiguration = getConnectionConfiguration();
      checkState(connectionConfiguration != null, "withConnectionConfiguration() is required");
      return input.apply(
          org.apache.beam.sdk.io.Read.from(new BoundedElasticsearchSource(this, null, null, null)));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("query", getQuery()));
      builder.addIfNotNull(DisplayData.item("withMetadata", isWithMetadata()));
      builder.addIfNotNull(DisplayData.item("batchSize", getBatchSize()));
      builder.addIfNotNull(DisplayData.item("scrollKeepalive", getScrollKeepalive()));
      getConnectionConfiguration().populateDisplayData(builder);
    }
  }

  /** A {@link BoundedSource} reading from Elasticsearch. */
  @VisibleForTesting
  public static class BoundedElasticsearchSource extends BoundedSource<String> {

    private int backendVersion;

    private final Read spec;
    // shardPreference is the shard id where the source will read the documents
    private final @Nullable String shardPreference;
    private final @Nullable Integer numSlices;
    private final @Nullable Integer sliceId;
    private @Nullable Long estimatedByteSize;

    // constructor used in split() when we know the backend version
    private BoundedElasticsearchSource(
        Read spec,
        @Nullable String shardPreference,
        @Nullable Integer numSlices,
        @Nullable Integer sliceId,
        @Nullable Long estimatedByteSize,
        int backendVersion) {
      this.backendVersion = backendVersion;
      this.spec = spec;
      this.shardPreference = shardPreference;
      this.numSlices = numSlices;
      this.estimatedByteSize = estimatedByteSize;
      this.sliceId = sliceId;
    }

    @VisibleForTesting
    BoundedElasticsearchSource(
        Read spec,
        @Nullable String shardPreference,
        @Nullable Integer numSlices,
        @Nullable Integer sliceId) {
      this.spec = spec;
      this.shardPreference = shardPreference;
      this.numSlices = numSlices;
      this.sliceId = sliceId;
    }

    @Override
    public List<? extends BoundedSource<String>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      ConnectionConfiguration connectionConfiguration = spec.getConnectionConfiguration();
      this.backendVersion = getBackendVersion(connectionConfiguration);
      List<BoundedElasticsearchSource> sources = new ArrayList<>();
      if (backendVersion == 2) {
        // 1. We split per shard :
        // unfortunately, Elasticsearch 2.x doesn't provide a way to do parallel reads on a single
        // shard.So we do not use desiredBundleSize because we cannot split shards.
        // With the slice API in ES 5.x+ we will be able to use desiredBundleSize.
        // Basically we will just ask the slice API to return data
        // in nbBundles = estimatedSize / desiredBundleSize chuncks.
        // So each beam source will read around desiredBundleSize volume of data.

        JsonNode statsJson = BoundedElasticsearchSource.getStats(connectionConfiguration, true);
        JsonNode shardsJson =
            statsJson.path("indices").path(connectionConfiguration.getIndex()).path("shards");

        Iterator<Map.Entry<String, JsonNode>> shards = shardsJson.fields();
        while (shards.hasNext()) {
          Map.Entry<String, JsonNode> shardJson = shards.next();
          String shardId = shardJson.getKey();
          sources.add(
              new BoundedElasticsearchSource(spec, shardId, null, null, null, backendVersion));
        }
        checkArgument(!sources.isEmpty(), "No shard found");
      } else if (backendVersion >= 5) {
        long indexSize = getEstimatedSizeBytes(options);
        float nbBundlesFloat = (float) indexSize / desiredBundleSizeBytes;
        int nbBundles = (int) Math.ceil(nbBundlesFloat);
        // ES slice api imposes that the number of slices is <= 1024 even if it can be overloaded
        if (nbBundles > 1024) {
          nbBundles = 1024;
        }
        // split the index into nbBundles chunks of desiredBundleSizeBytes by creating
        // nbBundles sources each reading a slice of the index
        // (see https://goo.gl/MhtSWz)
        // the slice API allows to split the ES shards
        // to have bundles closer to desiredBundleSizeBytes
        for (int i = 0; i < nbBundles; i++) {
          long estimatedByteSizeForBundle = getEstimatedSizeBytes(options) / nbBundles;
          sources.add(
              new BoundedElasticsearchSource(
                  spec, null, nbBundles, i, estimatedByteSizeForBundle, backendVersion));
        }
      }
      return sources;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws IOException {
      if (estimatedByteSize != null) {
        return estimatedByteSize;
      }
      final ConnectionConfiguration connectionConfiguration = spec.getConnectionConfiguration();
      JsonNode statsJson = getStats(connectionConfiguration, false);
      JsonNode indexStats =
          statsJson.path("indices").path(connectionConfiguration.getIndex()).path("primaries");
      long indexSize = indexStats.path("store").path("size_in_bytes").asLong();
      LOG.debug("estimate source byte size: total index size " + indexSize);

      String query = spec.getQuery() != null ? spec.getQuery().get() : null;
      if (query == null || query.isEmpty()) { // return index size if no query
        estimatedByteSize = indexSize;
        return estimatedByteSize;
      }

      long totalCount = indexStats.path("docs").path("count").asLong();
      LOG.debug("estimate source byte size: total document count " + totalCount);
      if (totalCount == 0) { // The min size is 1, because DirectRunner does not like 0
        estimatedByteSize = 1L;
        return estimatedByteSize;
      }

      String endPoint =
          String.format(
              "/%s/%s/_count",
              connectionConfiguration.getIndex(), connectionConfiguration.getType());
      try (RestClient restClient = connectionConfiguration.createClient()) {
        long count = queryCount(restClient, endPoint, query);
        LOG.debug("estimate source byte size: query document count " + count);
        if (count == 0) {
          estimatedByteSize = 1L;
        } else {
          // We estimate the average byte size for each document is (index/totalCount)
          // and then multiply the document count in the index
          estimatedByteSize = (indexSize / totalCount) * count;
        }
      }
      return estimatedByteSize;
    }

    private long queryCount(
        @Nonnull RestClient restClient, @Nonnull String endPoint, @Nonnull String query)
        throws IOException {
      Request request = new Request("GET", endPoint);
      request.setEntity(new NStringEntity(query, ContentType.APPLICATION_JSON));
      JsonNode searchResult = parseResponse(restClient.performRequest(request).getEntity());
      return searchResult.path("count").asLong();
    }

    @VisibleForTesting
    static long estimateIndexSize(ConnectionConfiguration connectionConfiguration)
        throws IOException {
      // we use indices stats API to estimate size and list the shards
      // (https://www.elastic.co/guide/en/elasticsearch/reference/2.4/indices-stats.html)
      // as Elasticsearch 2.x doesn't not support any way to do parallel read inside a shard
      // the estimated size bytes is not really used in the split into bundles.
      // However, we implement this method anyway as the runners can use it.
      // NB: Elasticsearch 5.x+ now provides the slice API.
      // (https://www.elastic.co/guide/en/elasticsearch/reference/5.0/search-request-scroll.html
      // #sliced-scroll)
      JsonNode statsJson = getStats(connectionConfiguration, false);
      JsonNode indexStats =
          statsJson.path("indices").path(connectionConfiguration.getIndex()).path("primaries");
      JsonNode store = indexStats.path("store");
      return store.path("size_in_bytes").asLong();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("shard", shardPreference));
      builder.addIfNotNull(DisplayData.item("numSlices", numSlices));
      builder.addIfNotNull(DisplayData.item("sliceId", sliceId));
    }

    @Override
    public BoundedReader<String> createReader(PipelineOptions options) {
      return new BoundedElasticsearchReader(this);
    }

    @Override
    public void validate() {
      spec.validate(null);
    }

    @Override
    public Coder<String> getOutputCoder() {
      return StringUtf8Coder.of();
    }

    private static JsonNode getStats(
        ConnectionConfiguration connectionConfiguration, boolean shardLevel) throws IOException {
      HashMap<String, String> params = new HashMap<>();
      if (shardLevel) {
        params.put("level", "shards");
      }
      String endpoint = String.format("/%s/_stats", connectionConfiguration.getIndex());
      try (RestClient restClient = connectionConfiguration.createClient()) {
        Request request = new Request("GET", endpoint);
        request.addParameters(params);
        return parseResponse(restClient.performRequest(request).getEntity());
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

      String query = source.spec.getQuery() != null ? source.spec.getQuery().get() : null;
      if (query == null) {
        query = "{\"query\": { \"match_all\": {} }}";
      }
      if ((source.backendVersion >= 5) && source.numSlices != null && source.numSlices > 1) {
        // if there is more than one slice, add the slice to the user query
        String sliceQuery =
            String.format("\"slice\": {\"id\": %s,\"max\": %s}", source.sliceId, source.numSlices);
        query = query.replaceFirst("\\{", "{" + sliceQuery + ",");
      }
      String endPoint =
          String.format(
              "/%s/%s/_search",
              source.spec.getConnectionConfiguration().getIndex(),
              source.spec.getConnectionConfiguration().getType());
      Map<String, String> params = new HashMap<>();
      params.put("scroll", source.spec.getScrollKeepalive());
      if (source.backendVersion == 2) {
        params.put("size", String.valueOf(source.spec.getBatchSize()));
        if (source.shardPreference != null) {
          params.put("preference", "_shards:" + source.shardPreference);
        }
      }
      HttpEntity queryEntity = new NStringEntity(query, ContentType.APPLICATION_JSON);
      Request request = new Request("GET", endPoint);
      request.addParameters(params);
      request.setEntity(queryEntity);
      Response response = restClient.performRequest(request);
      JsonNode searchResult = parseResponse(response.getEntity());
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
        Request request = new Request("GET", "/_search/scroll");
        request.addParameters(Collections.emptyMap());
        request.setEntity(scrollEntity);
        Response response = restClient.performRequest(request);
        JsonNode searchResult = parseResponse(response.getEntity());
        updateScrollId(searchResult);
        return readNextBatchAndReturnFirstDocument(searchResult);
      }
    }

    private boolean readNextBatchAndReturnFirstDocument(JsonNode searchResult) {
      // stop if no more data
      JsonNode hits = searchResult.path("hits").path("hits");
      if (hits.size() == 0) {
        current = null;
        batchIterator = null;
        return false;
      }
      // list behind iterator is empty
      List<String> batch = new ArrayList<>();
      boolean withMetadata = source.spec.isWithMetadata();
      for (JsonNode hit : hits) {
        if (withMetadata) {
          batch.add(hit.toString());
        } else {
          String document = hit.path("_source").toString();
          batch.add(document);
        }
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
        Request request = new Request("DELETE", "/_search/scroll");
        request.addParameters(Collections.emptyMap());
        request.setEntity(entity);
        restClient.performRequest(request);
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
  /**
   * A POJO encapsulating a configuration for retry behavior when issuing requests to ES. A retry
   * will be attempted until the maxAttempts or maxDuration is exceeded, whichever comes first, for
   * 429 TOO_MANY_REQUESTS error.
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
      abstract ElasticsearchIO.RetryConfiguration.Builder setMaxAttempts(int maxAttempts);

      abstract ElasticsearchIO.RetryConfiguration.Builder setMaxDuration(Duration maxDuration);

      abstract ElasticsearchIO.RetryConfiguration.Builder setRetryPredicate(
          RetryPredicate retryPredicate);

      abstract ElasticsearchIO.RetryConfiguration build();
    }

    /**
     * Creates RetryConfiguration for {@link ElasticsearchIO} with provided maxAttempts,
     * maxDurations and exponential backoff based retries.
     *
     * @param maxAttempts max number of attempts.
     * @param maxDuration maximum duration for retries.
     * @return {@link RetryConfiguration} object with provided settings.
     */
    public static RetryConfiguration create(int maxAttempts, Duration maxDuration) {
      checkArgument(maxAttempts > 0, "maxAttempts must be greater than 0");
      checkArgument(
          maxDuration != null && maxDuration.isLongerThan(Duration.ZERO),
          "maxDuration must be greater than 0");
      return new AutoValue_ElasticsearchIO_RetryConfiguration.Builder()
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
     * An interface used to control if we retry the Elasticsearch call when a {@link Response} is
     * obtained. If {@link RetryPredicate#test(Object)} returns true, {@link Write} tries to resend
     * the requests to the Elasticsearch server if the {@link RetryConfiguration} permits it.
     */
    @FunctionalInterface
    interface RetryPredicate extends Predicate<HttpEntity>, Serializable {}

    /**
     * This is the default predicate used to test if a failed ES operation should be retried. A
     * retry will be attempted until the maxAttempts or maxDuration is exceeded, whichever comes
     * first, for TOO_MANY_REQUESTS(429) error.
     */
    @VisibleForTesting
    static class DefaultRetryPredicate implements RetryPredicate {

      private int errorCode;

      DefaultRetryPredicate(int code) {
        this.errorCode = code;
      }

      DefaultRetryPredicate() {
        this(429);
      }

      /** Returns true if the response has the error code for any mutation. */
      private static boolean errorCodePresent(HttpEntity responseEntity, int errorCode) {
        try {
          JsonNode json = parseResponse(responseEntity);
          if (json.path("errors").asBoolean()) {
            for (JsonNode item : json.path("items")) {
              if (item.findValue("status").asInt() == errorCode) {
                return true;
              }
            }
          }
        } catch (IOException e) {
          LOG.warn("Could not extract error codes from responseEntity {}", responseEntity);
        }
        return false;
      }

      @Override
      public boolean test(HttpEntity responseEntity) {
        return errorCodePresent(responseEntity, errorCode);
      }
    }
  }

  /** A {@link PTransform} writing data to Elasticsearch. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<String>, PDone> {

    /**
     * Interface allowing a specific field value to be returned from a parsed JSON document. This is
     * used for using explicit document ids, and for dynamic routing (index/Type) on a document
     * basis. A null response will result in default behaviour and an exception will be propagated
     * as a failure.
     */
    public interface FieldValueExtractFn extends SerializableFunction<JsonNode, String> {}

    public interface BooleanFieldValueExtractFn extends SerializableFunction<JsonNode, Boolean> {}

    abstract @Nullable ConnectionConfiguration getConnectionConfiguration();

    abstract long getMaxBatchSize();

    abstract long getMaxBatchSizeBytes();

    abstract @Nullable FieldValueExtractFn getIdFn();

    abstract @Nullable FieldValueExtractFn getIndexFn();

    abstract @Nullable FieldValueExtractFn getTypeFn();

    abstract @Nullable RetryConfiguration getRetryConfiguration();

    abstract boolean getUsePartialUpdate();

    abstract @Nullable BooleanFieldValueExtractFn getIsDeleteFn();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration connectionConfiguration);

      abstract Builder setMaxBatchSize(long maxBatchSize);

      abstract Builder setMaxBatchSizeBytes(long maxBatchSizeBytes);

      abstract Builder setIdFn(FieldValueExtractFn idFunction);

      abstract Builder setIndexFn(FieldValueExtractFn indexFn);

      abstract Builder setTypeFn(FieldValueExtractFn typeFn);

      abstract Builder setUsePartialUpdate(boolean usePartialUpdate);

      abstract Builder setRetryConfiguration(RetryConfiguration retryConfiguration);

      abstract Builder setIsDeleteFn(BooleanFieldValueExtractFn isDeleteFn);

      abstract Write build();
    }

    /**
     * Provide the Elasticsearch connection configuration object.
     *
     * @param connectionConfiguration the Elasticsearch {@link ConnectionConfiguration} object
     * @return the {@link Write} with connection configuration set
     */
    public Write withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkArgument(connectionConfiguration != null, "connectionConfiguration can not be null");
      return builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    /**
     * Provide a maximum size in number of documents for the batch see bulk API
     * (https://www.elastic.co/guide/en/elasticsearch/reference/2.4/docs-bulk.html). Default is 1000
     * docs (like Elasticsearch bulk size advice). See
     * https://www.elastic.co/guide/en/elasticsearch/guide/current/bulk.html Depending on the
     * execution engine, size of bundles may vary, this sets the maximum size. Change this if you
     * need to have smaller Elasticsearch bulks.
     *
     * @param batchSize maximum batch size in number of documents
     * @return the {@link Write} with connection batch size set
     */
    public Write withMaxBatchSize(long batchSize) {
      checkArgument(batchSize > 0, "batchSize must be > 0, but was %s", batchSize);
      return builder().setMaxBatchSize(batchSize).build();
    }

    /**
     * Provide a maximum size in bytes for the batch see bulk API
     * (https://www.elastic.co/guide/en/elasticsearch/reference/2.4/docs-bulk.html). Default is 5MB
     * (like Elasticsearch bulk size advice). See
     * https://www.elastic.co/guide/en/elasticsearch/guide/current/bulk.html Depending on the
     * execution engine, size of bundles may vary, this sets the maximum size. Change this if you
     * need to have smaller Elasticsearch bulks.
     *
     * @param batchSizeBytes maximum batch size in bytes
     * @return the {@link Write} with connection batch size in bytes set
     */
    public Write withMaxBatchSizeBytes(long batchSizeBytes) {
      checkArgument(batchSizeBytes > 0, "batchSizeBytes must be > 0, but was %s", batchSizeBytes);
      return builder().setMaxBatchSizeBytes(batchSizeBytes).build();
    }

    /**
     * Provide a function to extract the id from the document. This id will be used as the document
     * id in Elasticsearch. Should the function throw an Exception then the batch will fail and the
     * exception propagated.
     *
     * @param idFn to extract the document ID
     * @return the {@link Write} with the function set
     */
    public Write withIdFn(FieldValueExtractFn idFn) {
      checkArgument(idFn != null, "idFn must not be null");
      return builder().setIdFn(idFn).build();
    }

    /**
     * Provide a function to extract the target index from the document allowing for dynamic
     * document routing. Should the function throw an Exception then the batch will fail and the
     * exception propagated.
     *
     * @param indexFn to extract the destination index from
     * @return the {@link Write} with the function set
     */
    public Write withIndexFn(FieldValueExtractFn indexFn) {
      checkArgument(indexFn != null, "indexFn must not be null");
      return builder().setIndexFn(indexFn).build();
    }

    /**
     * Provide a function to extract the target type from the document allowing for dynamic document
     * routing. Should the function throw an Exception then the batch will fail and the exception
     * propagated. Users are encouraged to consider carefully if multipe types are a sensible model
     * <a
     * href="https://www.elastic.co/blog/index-type-parent-child-join-now-future-in-elasticsearch">as
     * discussed in this blog</a>.
     *
     * @param typeFn to extract the destination index from
     * @return the {@link Write} with the function set
     */
    public Write withTypeFn(FieldValueExtractFn typeFn) {
      checkArgument(typeFn != null, "typeFn must not be null");
      return builder().setTypeFn(typeFn).build();
    }

    /**
     * Provide an instruction to control whether partial updates or inserts (default) are issued to
     * Elasticsearch.
     *
     * @param usePartialUpdate set to true to issue partial updates
     * @return the {@link Write} with the partial update control set
     */
    public Write withUsePartialUpdate(boolean usePartialUpdate) {
      return builder().setUsePartialUpdate(usePartialUpdate).build();
    }

    /**
     * Provides configuration to retry a failed batch call to Elasticsearch. A batch is considered
     * as failed if the underlying {@link RestClient} surfaces 429 HTTP status code as error for one
     * or more of the items in the {@link Response}. Users should consider that retrying might
     * compound the underlying problem which caused the initial failure. Users should also be aware
     * that once retrying is exhausted the error is surfaced to the runner which <em>may</em> then
     * opt to retry the current bundle in entirety or abort if the max number of retries of the
     * runner is completed. Retrying uses an exponential backoff algorithm, with minimum backoff of
     * 5 seconds and then surfacing the error once the maximum number of retries or maximum
     * configuration duration is exceeded.
     *
     * <p>Example use:
     *
     * <pre>{@code
     * ElasticsearchIO.write()
     *   .withRetryConfiguration(ElasticsearchIO.RetryConfiguration.create(10, Duration.standardMinutes(3))
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

    /**
     * Provide a function to extract the target operation either upsert or delete from the document
     * fields allowing dynamic bulk operation decision. While using withIsDeleteFn, it should be
     * taken care that the document's id extraction is defined using the withIdFn function or else
     * IllegalArgumentException is thrown. Should the function throw an Exception then the batch
     * will fail and the exception propagated.
     *
     * @param isDeleteFn set to true for deleting the specific document
     * @return the {@link Write} with the function set
     */
    public Write withIsDeleteFn(BooleanFieldValueExtractFn isDeleteFn) {
      checkArgument(isDeleteFn != null, "deleteFn is required");
      return builder().setIsDeleteFn(isDeleteFn).build();
    }

    @Override
    public PDone expand(PCollection<String> input) {
      ConnectionConfiguration connectionConfiguration = getConnectionConfiguration();
      FieldValueExtractFn idFn = getIdFn();
      BooleanFieldValueExtractFn isDeleteFn = getIsDeleteFn();
      checkState(connectionConfiguration != null, "withConnectionConfiguration() is required");
      checkArgument(
          isDeleteFn == null || idFn != null,
          "Id needs to be specified by withIdFn for delete operation");
      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    /** {@link DoFn} to for the {@link Write} transform. */
    @VisibleForTesting
    static class WriteFn extends DoFn<String, Void> {
      private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
      private static final int DEFAULT_RETRY_ON_CONFLICT = 5; // race conditions on updates

      private static final Duration RETRY_INITIAL_BACKOFF = Duration.standardSeconds(5);

      @VisibleForTesting
      static final String RETRY_ATTEMPT_LOG = "Error writing to Elasticsearch. Retry attempt[%d]";

      @VisibleForTesting
      static final String RETRY_FAILED_LOG =
          "Error writing to ES after %d attempt(s). No more attempts allowed";

      private transient FluentBackoff retryBackoff;

      private int backendVersion;
      private final Write spec;
      private transient RestClient restClient;
      private ArrayList<String> batch;
      private long currentBatchSizeBytes;

      // Encapsulates the elements which form the metadata for an Elasticsearch bulk operation
      private static class DocumentMetadata implements Serializable {
        final String index;
        final String type;
        final String id;
        final Integer retryOnConflict;

        DocumentMetadata(String index, String type, String id, Integer retryOnConflict) {
          this.index = index;
          this.type = type;
          this.id = id;
          this.retryOnConflict = retryOnConflict;
        }
      }

      @VisibleForTesting
      WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() throws IOException {
        ConnectionConfiguration connectionConfiguration = spec.getConnectionConfiguration();
        backendVersion = getBackendVersion(connectionConfiguration);
        restClient = connectionConfiguration.createClient();

        retryBackoff =
            FluentBackoff.DEFAULT.withMaxRetries(0).withInitialBackoff(RETRY_INITIAL_BACKOFF);

        if (spec.getRetryConfiguration() != null) {
          retryBackoff =
              FluentBackoff.DEFAULT
                  .withInitialBackoff(RETRY_INITIAL_BACKOFF)
                  .withMaxRetries(spec.getRetryConfiguration().getMaxAttempts() - 1)
                  .withMaxCumulativeBackoff(spec.getRetryConfiguration().getMaxDuration());
        }
        // configure a custom serializer for metadata to be able to change serialization based
        // on ES version
        SimpleModule module = new SimpleModule();
        module.addSerializer(DocumentMetadata.class, new DocumentMetadataSerializer());
        OBJECT_MAPPER.registerModule(module);
      }

      @StartBundle
      public void startBundle(StartBundleContext context) {
        batch = new ArrayList<>();
        currentBatchSizeBytes = 0;
      }

      private class DocumentMetadataSerializer extends StdSerializer<DocumentMetadata> {

        private DocumentMetadataSerializer() {
          super(DocumentMetadata.class);
        }

        @Override
        public void serialize(
            DocumentMetadata value, JsonGenerator gen, SerializerProvider provider)
            throws IOException {
          gen.writeStartObject();
          if (value.index != null) {
            gen.writeStringField("_index", value.index);
          }
          if (value.type != null) {
            gen.writeStringField("_type", value.type);
          }
          if (value.id != null) {
            gen.writeStringField("_id", value.id);
          }
          if (value.retryOnConflict != null && (backendVersion <= 6)) {
            gen.writeNumberField("_retry_on_conflict", value.retryOnConflict);
          }
          if (value.retryOnConflict != null && backendVersion >= 7) {
            gen.writeNumberField("retry_on_conflict", value.retryOnConflict);
          }
          gen.writeEndObject();
        }
      }
      /**
       * Extracts the components that comprise the document address from the document using the
       * {@link FieldValueExtractFn} configured. This allows any or all of the index, type and
       * document id to be controlled on a per document basis. Sanitization of the index is
       * performed, automatically lower-casing the value as required by Elasticsearch.
       *
       * @param parsedDocument the json from which the index, type and id may be extracted
       * @return the document address as JSON or the default
       * @throws IOException if the document cannot be parsed as JSON
       */
      private String getDocumentMetadata(JsonNode parsedDocument) throws IOException {
        DocumentMetadata metadata =
            new DocumentMetadata(
                spec.getIndexFn() != null
                    ? lowerCaseOrNull(spec.getIndexFn().apply(parsedDocument))
                    : null,
                spec.getTypeFn() != null ? spec.getTypeFn().apply(parsedDocument) : null,
                spec.getIdFn() != null ? spec.getIdFn().apply(parsedDocument) : null,
                spec.getUsePartialUpdate() ? DEFAULT_RETRY_ON_CONFLICT : null);
        return OBJECT_MAPPER.writeValueAsString(metadata);
      }

      private static String lowerCaseOrNull(String input) {
        return input == null ? null : input.toLowerCase();
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        String document = context.element(); // use configuration and auto-generated document IDs
        String documentMetadata = "{}";
        boolean isDelete = false;
        if (spec.getIndexFn() != null || spec.getTypeFn() != null || spec.getIdFn() != null) {
          // parse once and reused for efficiency
          JsonNode parsedDocument = OBJECT_MAPPER.readTree(document);
          documentMetadata = getDocumentMetadata(parsedDocument);
          if (spec.getIsDeleteFn() != null) {
            isDelete = spec.getIsDeleteFn().apply(parsedDocument);
          }
        }

        if (isDelete) {
          // delete request used for deleting a document.
          batch.add(String.format("{ \"delete\" : %s }%n", documentMetadata));
        } else {
          // index is an insert/upsert and update is a partial update (or insert if not existing)
          if (spec.getUsePartialUpdate()) {
            batch.add(
                String.format(
                    "{ \"update\" : %s }%n{ \"doc\" : %s, \"doc_as_upsert\" : true }%n",
                    documentMetadata, document));
          } else {
            batch.add(String.format("{ \"index\" : %s }%n%s%n", documentMetadata, document));
          }
        }

        currentBatchSizeBytes += document.getBytes(StandardCharsets.UTF_8).length;
        if (batch.size() >= spec.getMaxBatchSize()
            || currentBatchSizeBytes >= spec.getMaxBatchSizeBytes()) {
          flushBatch();
        }
      }

      @FinishBundle
      public void finishBundle(FinishBundleContext context)
          throws IOException, InterruptedException {
        flushBatch();
      }

      private void flushBatch() throws IOException, InterruptedException {
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
        HttpEntity responseEntity;
        // Elasticsearch will default to the index/type provided here if none are set in the
        // document meta (i.e. using ElasticsearchIO$Write#withIndexFn and
        // ElasticsearchIO$Write#withTypeFn options)
        String endPoint =
            String.format(
                "/%s/%s/_bulk",
                spec.getConnectionConfiguration().getIndex(),
                spec.getConnectionConfiguration().getType());
        HttpEntity requestBody =
            new NStringEntity(bulkRequest.toString(), ContentType.APPLICATION_JSON);
        Request request = new Request("POST", endPoint);
        request.addParameters(Collections.emptyMap());
        request.setEntity(requestBody);
        response = restClient.performRequest(request);
        responseEntity = new BufferedHttpEntity(response.getEntity());
        if (spec.getRetryConfiguration() != null
            && spec.getRetryConfiguration().getRetryPredicate().test(responseEntity)) {
          responseEntity = handleRetry("POST", endPoint, Collections.emptyMap(), requestBody);
        }
        checkForErrors(responseEntity, backendVersion, spec.getUsePartialUpdate());
      }

      /** retry request based on retry configuration policy. */
      private HttpEntity handleRetry(
          String method, String endpoint, Map<String, String> params, HttpEntity requestBody)
          throws IOException, InterruptedException {
        Response response;
        HttpEntity responseEntity;
        Sleeper sleeper = Sleeper.DEFAULT;
        BackOff backoff = retryBackoff.backoff();
        int attempt = 0;
        // while retry policy exists
        while (BackOffUtils.next(sleeper, backoff)) {
          LOG.warn(String.format(RETRY_ATTEMPT_LOG, ++attempt));
          Request request = new Request(method, endpoint);
          request.addParameters(params);
          request.setEntity(requestBody);
          response = restClient.performRequest(request);
          responseEntity = new BufferedHttpEntity(response.getEntity());
          // if response has no 429 errors
          if (!spec.getRetryConfiguration().getRetryPredicate().test(responseEntity)) {
            return responseEntity;
          }
        }
        throw new IOException(String.format(RETRY_FAILED_LOG, attempt));
      }

      @Teardown
      public void closeClient() throws IOException {
        if (restClient != null) {
          restClient.close();
        }
      }
    }
  }

  static int getBackendVersion(ConnectionConfiguration connectionConfiguration) {
    try (RestClient restClient = connectionConfiguration.createClient()) {
      Request request = new Request("GET", "");
      Response response = restClient.performRequest(request);
      JsonNode jsonNode = parseResponse(response.getEntity());
      int backendVersion =
          Integer.parseInt(jsonNode.path("version").path("number").asText().substring(0, 1));
      checkArgument(
          (backendVersion == 2
              || backendVersion == 5
              || backendVersion == 6
              || backendVersion == 7),
          "The Elasticsearch version to connect to is %s.x. "
              + "This version of the ElasticsearchIO is only compatible with "
              + "Elasticsearch v7.x, v6.x, v5.x and v2.x",
          backendVersion);
      return backendVersion;

    } catch (IOException e) {
      throw new IllegalArgumentException("Cannot get Elasticsearch version", e);
    }
  }
}
