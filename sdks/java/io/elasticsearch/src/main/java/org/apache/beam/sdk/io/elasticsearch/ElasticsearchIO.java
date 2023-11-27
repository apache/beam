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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

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
import java.io.OutputStream;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Streams;
import org.apache.http.ConnectionClosedException;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.ssl.SSLContexts;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.joda.time.Duration;
import org.joda.time.Instant;
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
 * <p>The connection configuration also accepts optional configuration: {@code withUsername()},
 * {@code withPassword()}, {@code withApiKey()}, {@code withBearerToken()} and {@code
 * withDefaultHeaders()}.
 *
 * <p>You can also specify a query on the {@code read()} using {@code withQuery()}.
 *
 * <p>There are many more configuration options which can be found by looking at the with* methods
 * of {@link ElasticsearchIO.Read}
 *
 * <h3>Writing to Elasticsearch</h3>
 *
 * <p>To write documents to Elasticsearch, use {@link ElasticsearchIO#write
 * ElasticsearchIO.write()}, which writes JSON documents from a {@link PCollection
 * PCollection&lt;String&gt;} (which can be bounded or unbounded).
 *
 * <p>{@link ElasticsearchIO.Write} involves 2 discrete steps:
 *
 * <ul>
 *   <li>Converting the input PCollection of valid ES documents into Bulk API directives i.e. Should
 *       the input document result in: update, insert, delete, with version, with routing, etc (See
 *       {@link ElasticsearchIO.DocToBulk})
 *   <li>Batching Bulk API directives together and interfacing with an Elasticsearch cluster. (See
 *       {@link ElasticsearchIO.BulkIO})
 * </ul>
 *
 * <p>In most cases, using {@link ElasticsearchIO#write} will be desirable. In some cases, one may
 * want to use {@link ElasticsearchIO.DocToBulk} and {@link ElasticsearchIO.BulkIO} directly. Such
 * cases might include:
 *
 * <ul>
 *   <li>Unit testing. Ensure that output Bulk API entities for a given set of inputs will produce
 *       an expected result, without the need for an available Elasticsearch cluster. See {@link
 *       ElasticsearchIO.Write#docToBulk}
 *   <li>Flexible options for data backup. Serialized Bulk API entities can be forked and sent to
 *       both Elasticsearch and a data lake.
 *   <li>Mirroring data to multiple clusters. Presently, mirroring data to multiple clusters would
 *       require duplicate computation.
 *   <li>Better batching with input streams in one job. A job may produce multiple "shapes" of Bulk
 *       API directives based on multiple input types, and then "fan-in" all serialized Bulk
 *       directives into a single BulkIO transform to improve batching semantics.
 *   <li>Decoupled jobs. Job(s) could be made to produce Bulk directives and then publish them to a
 *       message bus. A distinct job could consume from that message bus and solely be responsible
 *       for IO with the target cluster(s).
 * </ul>
 *
 * <p>Note that configurations options for {@link ElasticsearchIO.Write} are a union of
 * configuration options for {@link ElasticsearchIO.DocToBulk} and {@link ElasticsearchIO.BulkIO}.
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
 * <p>There are many more configuration options which can be found by looking at the with* methods
 * of {@link ElasticsearchIO.Write}
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class ElasticsearchIO {

  private static final List<Integer> VALID_CLUSTER_VERSIONS = Arrays.asList(5, 6, 7, 8);
  private static final Set<Integer> DEPRECATED_CLUSTER_VERSIONS =
      new HashSet<>(Arrays.asList(5, 6));
  private static final List<String> VERSION_TYPES =
      Arrays.asList("internal", "external", "external_gt", "external_gte");
  private static final String VERSION_CONFLICT_ERROR = "version_conflict_engine_exception";

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchIO.class);

  public static Read read() {
    // default scrollKeepalive = 5m as a majorant for un-predictable time between 2 start/read
    // calls
    // default batchSize to 100 as recommended by ES dev team as a safe value when dealing
    // with big documents and still a good compromise for performances
    return new AutoValue_ElasticsearchIO_Read.Builder()
        .setWithMetadata(false)
        .setScrollKeepalive("5m")
        .setBatchSize(100L)
        .build();
  }

  public static DocToBulk docToBulk() {
    return new AutoValue_ElasticsearchIO_DocToBulk.Builder().build();
  }

  public static BulkIO bulkIO() {
    return new AutoValue_ElasticsearchIO_BulkIO.Builder()
        // advised default starting batch size in ES docs
        .setMaxBatchSize(1000L)
        // advised default starting batch size in ES docs
        .setMaxBatchSizeBytes(5L * 1024L * 1024L)
        .setUseStatefulBatches(false)
        .setMaxParallelRequests(1)
        .setThrowWriteErrors(true)
        .build();
  }

  public static Write write() {
    return new Write();
  }

  private ElasticsearchIO() {}

  private static final ObjectMapper mapper = new ObjectMapper();

  @VisibleForTesting
  static JsonNode parseResponse(HttpEntity responseEntity) throws IOException {
    return mapper.readValue(responseEntity.getContent(), JsonNode.class);
  }

  static List<Document> createWriteReport(
      HttpEntity responseEntity, @Nullable Set<String> allowedErrorTypes, boolean throwWriteErrors)
      throws IOException {

    List<Document> responses = new ArrayList<>();
    int numErrors = 0;
    JsonNode searchResult = parseResponse(responseEntity);
    StringBuilder errorMessages =
        new StringBuilder("Error writing to Elasticsearch, some elements could not be inserted:");
    JsonNode items = searchResult.path("items");

    if (items.isMissingNode() || items.size() == 0) {
      // This would only be expected in cases like connectivity issues or similar
      errorMessages.append(searchResult);
      LOG.warn("'items' missing from Elasticsearch response: {}", errorMessages);
    }

    // some items present in bulk might have errors, concatenate error messages and record
    // which items had errors
    for (JsonNode item : items) {
      Document result = Document.create().withResponseItemJson(item.toString());

      JsonNode error = item.findValue("error");
      if (error != null) {
        // N.B. An empty-string within the allowedErrorTypes Set implies all errors are allowed.
        String type = error.path("type").asText();
        String reason = error.path("reason").asText();
        String docId = item.findValue("_id").asText();
        JsonNode causedBy = error.path("caused_by"); // May not be present
        String cbReason = causedBy.path("reason").asText();
        String cbType = causedBy.path("type").asText();

        if (allowedErrorTypes == null
            || (!allowedErrorTypes.contains(type) && !allowedErrorTypes.contains(cbType))) {
          // 'error' and 'causedBy` fields are not null, and the error is not being ignored.
          result = result.withHasError(true);
          numErrors++;

          errorMessages.append(String.format("%nDocument id %s: %s (%s)", docId, reason, type));

          if (!causedBy.isMissingNode()) {
            errorMessages.append(String.format("%nCaused by: %s (%s)", cbReason, cbType));
          }
        }
      }
      responses.add(result);
    }

    if (numErrors > 0) {
      LOG.error(errorMessages.toString());
      if (throwWriteErrors) {
        throw new IOException(errorMessages.toString());
      }
    }

    return responses;
  }

  /** A POJO describing a connection configuration to Elasticsearch. */
  @AutoValue
  public abstract static class ConnectionConfiguration implements Serializable {

    public abstract List<String> getAddresses();

    public abstract @Nullable String getUsername();

    public abstract @Nullable String getPassword();

    public abstract @Nullable String getApiKey();

    public abstract @Nullable String getBearerToken();

    public abstract @Nullable List<Header> getDefaultHeaders();

    public abstract @Nullable String getKeystorePath();

    public abstract @Nullable String getKeystorePassword();

    public abstract String getIndex();

    public abstract @Nullable String getType();

    public abstract @Nullable Integer getSocketTimeout();

    public abstract @Nullable Integer getConnectTimeout();

    public abstract boolean isTrustSelfSignedCerts();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setAddresses(List<String> addresses);

      abstract Builder setUsername(String username);

      abstract Builder setPassword(String password);

      abstract Builder setApiKey(String apiKey);

      abstract Builder setBearerToken(String bearerToken);

      abstract Builder setDefaultHeaders(List<Header> defaultHeaders);

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
     * Creates a new Elasticsearch connection configuration with no default type.
     *
     * @param addresses list of addresses of Elasticsearch nodes
     * @param index the index toward which the requests will be issued
     * @return the connection configuration object
     */
    public static ConnectionConfiguration create(String[] addresses, String index) {
      checkArgument(addresses != null, "addresses can not be null");
      checkArgument(addresses.length > 0, "addresses can not be empty");
      checkArgument(index != null, "index can not be null");
      return new AutoValue_ElasticsearchIO_ConnectionConfiguration.Builder()
          .setAddresses(Arrays.asList(addresses))
          .setIndex(index)
          .setType("")
          .setTrustSelfSignedCerts(false)
          .build();
    }

    /**
     * Creates a new Elasticsearch connection configuration with no default index nor type.
     *
     * @param addresses list of addresses of Elasticsearch nodes
     * @return the connection configuration object
     */
    public static ConnectionConfiguration create(String[] addresses) {
      checkArgument(addresses != null, "addresses can not be null");
      checkArgument(addresses.length > 0, "addresses can not be empty");
      return new AutoValue_ElasticsearchIO_ConnectionConfiguration.Builder()
          .setAddresses(Arrays.asList(addresses))
          .setIndex("")
          .setType("")
          .setTrustSelfSignedCerts(false)
          .build();
    }

    /**
     * Generates the API endpoint prefix based on the set values.
     *
     * <p>Based on ConnectionConfiguration constructors, we know that one of the following is true:
     *
     * <ul>
     *   <li>index and type are non-empty strings
     *   <li>index is non-empty string, type is empty string
     *   <li>index and type are empty string
     * </ul>
     *
     * <p>Example valid endpoints therefore include:
     *
     * <ul>
     *   <li>/_bulk
     *   <li>/index_name/_bulk
     *   <li>/index_name/type_name/_bulk
     * </ul>
     */
    public String getApiPrefix() {
      StringBuilder sb = new StringBuilder();
      if (!Strings.isNullOrEmpty(getIndex())) {
        sb.append("/").append(getIndex());
      }
      if (!Strings.isNullOrEmpty(getType())) {
        sb.append("/").append(getType());
      }
      return sb.toString();
    }

    public String getPrefixedEndpoint(String endpoint) {
      return getApiPrefix() + "/" + endpoint;
    }

    public String getBulkEndPoint() {
      return getPrefixedEndpoint("_bulk");
    }

    public String getSearchEndPoint() {
      return getPrefixedEndpoint("_search");
    }

    public String getCountEndPoint() {
      return getPrefixedEndpoint("_count");
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
     * If Elasticsearch authentication is enabled, provide an API key. Be aware that you can only
     * use one of {@Code withApiToken()}, {@code withBearerToken()} and {@code withDefaultHeaders}
     * at the same time, as they (potentially) use the same header.
     *
     * @param apiKey the API key used to authenticate to Elasticsearch
     * @return a {@link ConnectionConfiguration} describes a connection configuration to
     *     Elasticsearch.
     */
    public ConnectionConfiguration withApiKey(String apiKey) {
      checkArgument(!Strings.isNullOrEmpty(apiKey), "apiKey can not be null or empty");
      checkArgument(getBearerToken() == null, "apiKey can not be combined with bearerToken");
      checkArgument(getDefaultHeaders() == null, "apiKey can not be combined with defaultHeaders");
      return builder().setApiKey(apiKey).build();
    }

    /**
     * If Elasticsearch authentication is enabled, provide a bearer token. Be aware that you can
     * only use one of {@Code withApiToken()}, {@code withBearerToken()} and {@code
     * withDefaultHeaders} at the same time, as they (potentially) use the same header.
     *
     * @param bearerToken the bearer token used to authenticate to Elasticsearch
     * @return a {@link ConnectionConfiguration} describes a connection configuration to
     *     Elasticsearch.
     */
    public ConnectionConfiguration withBearerToken(String bearerToken) {
      checkArgument(!Strings.isNullOrEmpty(bearerToken), "bearerToken can not be null or empty");
      checkArgument(getApiKey() == null, "bearerToken can not be combined with apiKey");
      checkArgument(
          getDefaultHeaders() == null, "bearerToken can not be combined with defaultHeaders");
      return builder().setBearerToken(bearerToken).build();
    }

    /**
     * For authentication or custom requirements, provide a set if default headers for the client.
     * Be aware that you can only use one of {@code withApiToken()}, {@code withBearerToken()} and
     * {@code withDefaultHeaders} at the same time, as they (potentially) use the same header.
     *
     * <p>An example of where this could be useful is if the client needs to use short-lived
     * credentials that need to be renewed on a certain interval. To implement that, a user could
     * implement a custom header that tracks the renewal period, for example:
     *
     * <pre>
     * {@code class OAuthTokenHeader extends BasicHeader {
     *     OAuthToken accessToken;
     *
     *     ...
     *
     *     @Override
     *     public String getValue() {
     *         if (accessToken.isExpired()) {
     *             accessToken.renew();
     *         }
     *         return String.format("Bearer %s", accessToken.getToken());
     *     }
     * }}
     * </pre>
     *
     * @param defaultHeaders the headers to add to outgoing requests
     * @return a {@link ConnectionConfiguration} describes a connection configuration to
     *     Elasticsearch.
     */
    public ConnectionConfiguration withDefaultHeaders(Header[] defaultHeaders) {
      checkArgument(defaultHeaders != null, "defaultHeaders can not be null");
      checkArgument(defaultHeaders.length > 0, "defaultHeaders can not be empty");
      checkArgument(getApiKey() == null, "defaultHeaders can not be combined with apiKey");
      checkArgument(
          getBearerToken() == null, "defaultHeaders can not be combined with bearerToken");
      return builder().setDefaultHeaders(Arrays.asList(defaultHeaders)).build();
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
      builder.addIfNotNull(DisplayData.item("type", getType()));
      builder.addIfNotNull(DisplayData.item("username", getUsername()));
      builder.addIfNotNull(DisplayData.item("keystore.path", getKeystorePath()));
      builder.addIfNotNull(DisplayData.item("socketTimeout", getSocketTimeout()));
      builder.addIfNotNull(DisplayData.item("connectTimeout", getConnectTimeout()));
      builder.addIfNotNull(DisplayData.item("trustSelfSignedCerts", isTrustSelfSignedCerts()));
    }

    private SSLContext getSSLContext() throws IOException {
      if (getKeystorePath() != null && !getKeystorePath().isEmpty()) {
        try {
          KeyStore keyStore = KeyStore.getInstance("jks");
          try (InputStream is = new FileInputStream(new File(getKeystorePath()))) {
            String keystorePassword = getKeystorePassword();
            keyStore.load(is, (keystorePassword == null) ? null : keystorePassword.toCharArray());
          }
          final TrustStrategy trustStrategy =
              isTrustSelfSignedCerts() ? new TrustSelfSignedStrategy() : null;
          return SSLContexts.custom().loadTrustMaterial(keyStore, trustStrategy).build();
        } catch (Exception e) {
          throw new IOException("Can't load the client certificate from the keystore", e);
        }
      }
      return null;
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

      final SSLContext sslContext = getSSLContext();

      if (getApiKey() != null) {
        restClientBuilder.setDefaultHeaders(
            new Header[] {new BasicHeader("Authorization", "ApiKey " + getApiKey())});
      }
      if (getBearerToken() != null) {
        restClientBuilder.setDefaultHeaders(
            new Header[] {new BasicHeader("Authorization", "Bearer " + getBearerToken())});
      }
      if (getDefaultHeaders() != null) {
        Header[] headerList = new Header[getDefaultHeaders().size()];
        restClientBuilder.setDefaultHeaders(getDefaultHeaders().toArray(headerList));
      }

      restClientBuilder.setHttpClientConfigCallback(
          httpClientBuilder -> {
            if (getUsername() != null) {
              final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
              credentialsProvider.setCredentials(
                  AuthScope.ANY, new UsernamePasswordCredentials(getUsername(), getPassword()));
              httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
            if (sslContext != null) {
              final SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(sslContext);
              httpClientBuilder.setSSLContext(sslContext).setSSLStrategy(sessionStrategy);
            }
            return httpClientBuilder;
          });

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
     *     href="https://www.elastic.co/guide/en/elasticsearch/reference/7.17/query-dsl.html">Query
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
     *     href="https://www.elastic.co/guide/en/elasticsearch/reference/7.17/query-dsl.html">Query
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
     * href="https://www.elastic.co/guide/en/elasticsearch/reference/7.17/search-request-scroll.html">scroll
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
     * href="https://www.elastic.co/guide/en/elasticsearch/reference/7.17/search-request-scroll.html">
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
          org.apache.beam.sdk.io.Read.from(new BoundedElasticsearchSource(this, null, null)));
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
    private final @Nullable Integer numSlices;
    private final @Nullable Integer sliceId;
    private @Nullable Long estimatedByteSize;

    // constructor used in split() when we know the backend version
    private BoundedElasticsearchSource(
        Read spec,
        @Nullable Integer numSlices,
        @Nullable Integer sliceId,
        @Nullable Long estimatedByteSize,
        int backendVersion) {
      this.backendVersion = backendVersion;
      this.spec = spec;
      this.numSlices = numSlices;
      this.estimatedByteSize = estimatedByteSize;
      this.sliceId = sliceId;
    }

    @VisibleForTesting
    BoundedElasticsearchSource(Read spec, @Nullable Integer numSlices, @Nullable Integer sliceId) {
      this.spec = spec;
      this.numSlices = numSlices;
      this.sliceId = sliceId;
    }

    @Override
    public List<? extends BoundedSource<String>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      ConnectionConfiguration connectionConfiguration = spec.getConnectionConfiguration();
      this.backendVersion = getBackendVersion(connectionConfiguration);
      List<BoundedElasticsearchSource> sources = new ArrayList<>();
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
                spec, nbBundles, i, estimatedByteSizeForBundle, backendVersion));
      }
      return sources;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws IOException {
      if (estimatedByteSize != null) {
        return estimatedByteSize;
      }
      final ConnectionConfiguration connectionConfiguration = spec.getConnectionConfiguration();
      JsonNode statsJson = getStats(connectionConfiguration);
      JsonNode indexStats = statsJson.path("_all").path("primaries");
      long indexSize = indexStats.path("store").path("size_in_bytes").asLong();
      LOG.debug("estimate source byte size: total index size {}", indexSize);

      String query = spec.getQuery() != null ? spec.getQuery().get() : null;
      if (query == null || query.isEmpty()) { // return index size if no query
        estimatedByteSize = indexSize;
        return estimatedByteSize;
      }

      long totalCount = indexStats.path("docs").path("count").asLong();
      LOG.debug("estimate source byte size: total document count {}", totalCount);
      if (totalCount == 0) { // The min size is 1, because DirectRunner does not like 0
        estimatedByteSize = 1L;
        return estimatedByteSize;
      }

      String endPoint = connectionConfiguration.getCountEndPoint();
      try (RestClient restClient = connectionConfiguration.createClient()) {
        long count = queryCount(restClient, endPoint, query);
        LOG.debug("estimate source byte size: query document count {}", count);
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
      // (https://www.elastic.co/guide/en/elasticsearch/reference/7.17/indices-stats.html)
      // as Elasticsearch 2.x doesn't not support any way to do parallel read inside a shard
      // the estimated size bytes is not really used in the split into bundles.
      // However, we implement this method anyway as the runners can use it.
      // NB: Elasticsearch 5.x+ now provides the slice API.
      // (https://www.elastic.co/guide/en/elasticsearch/reference/5.0/search-request-scroll.html
      // #sliced-scroll)
      JsonNode statsJson = getStats(connectionConfiguration);
      JsonNode indexStats = statsJson.path("_all").path("primaries");
      JsonNode store = indexStats.path("store");
      return store.path("size_in_bytes").asLong();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
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

    private static JsonNode getStats(ConnectionConfiguration connectionConfiguration)
        throws IOException {
      HashMap<String, String> params = new HashMap<>();
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
      String endPoint = source.spec.getConnectionConfiguration().getSearchEndPoint();
      Map<String, String> params = new HashMap<>();
      params.put("scroll", source.spec.getScrollKeepalive());
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

  /** A {@link PTransform} converting docs to their Bulk API counterparts. */
  @AutoValue
  public abstract static class DocToBulk
      extends PTransform<PCollection<String>, PCollection<Document>> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final int DEFAULT_RETRY_ON_CONFLICT = 5; // race conditions on updates

    static {
      SimpleModule module = new SimpleModule();
      module.addSerializer(DocumentMetadata.class, new DocumentMetadataSerializer());
      OBJECT_MAPPER.registerModule(module);
    }

    abstract @Nullable ConnectionConfiguration getConnectionConfiguration();

    abstract Write.@Nullable FieldValueExtractFn getIdFn();

    abstract Write.@Nullable FieldValueExtractFn getIndexFn();

    abstract Write.@Nullable FieldValueExtractFn getRoutingFn();

    abstract Write.@Nullable FieldValueExtractFn getTypeFn();

    abstract Write.@Nullable FieldValueExtractFn getDocVersionFn();

    abstract @Nullable String getDocVersionType();

    abstract @Nullable String getUpsertScript();

    abstract @Nullable Boolean getUsePartialUpdate();

    abstract @Nullable Boolean getAppendOnly();

    abstract Write.@Nullable BooleanFieldValueExtractFn getIsDeleteFn();

    abstract @Nullable Integer getBackendVersion();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration connectionConfiguration);

      abstract Builder setIdFn(Write.FieldValueExtractFn idFunction);

      abstract Builder setIndexFn(Write.FieldValueExtractFn indexFn);

      abstract Builder setRoutingFn(Write.FieldValueExtractFn routingFunction);

      abstract Builder setTypeFn(Write.FieldValueExtractFn typeFn);

      abstract Builder setDocVersionFn(Write.FieldValueExtractFn docVersionFn);

      abstract Builder setDocVersionType(String docVersionType);

      abstract Builder setIsDeleteFn(Write.BooleanFieldValueExtractFn isDeleteFn);

      abstract Builder setUsePartialUpdate(Boolean usePartialUpdate);

      abstract Builder setAppendOnly(Boolean appendOnly);

      abstract Builder setUpsertScript(String source);

      abstract Builder setBackendVersion(Integer assumedBackendVersion);

      abstract DocToBulk build();
    }

    /**
     * Provide the Elasticsearch connection configuration object. Only required if
     * withBackendVersion was not used i.e. getBackendVersion() returns null.
     *
     * @param connectionConfiguration the Elasticsearch {@link ConnectionConfiguration} object
     * @return the {@link DocToBulk} with connection configuration set
     */
    public DocToBulk withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkArgument(connectionConfiguration != null, "connectionConfiguration can not be null");
      return builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    /**
     * Provide a function to extract the id from the document. This id will be used as the document
     * id in Elasticsearch. Should the function throw an Exception then the batch will fail and the
     * exception propagated.
     *
     * @param idFn to extract the document ID
     * @return the {@link DocToBulk} with the function set
     */
    public DocToBulk withIdFn(Write.FieldValueExtractFn idFn) {
      checkArgument(idFn != null, "idFn must not be null");
      return builder().setIdFn(idFn).build();
    }

    /**
     * Provide a function to extract the target index from the document allowing for dynamic
     * document routing. Should the function throw an Exception then the batch will fail and the
     * exception propagated.
     *
     * @param indexFn to extract the destination index from
     * @return the {@link DocToBulk} with the function set
     */
    public DocToBulk withIndexFn(Write.FieldValueExtractFn indexFn) {
      checkArgument(indexFn != null, "indexFn must not be null");
      return builder().setIndexFn(indexFn).build();
    }

    /**
     * Provide a function to extract the target routing from the document allowing for dynamic
     * document routing. Should the function throw an Exception then the batch will fail and the
     * exception propagated.
     *
     * @param routingFn to extract the destination index from
     * @return the {@link DocToBulk} with the function set
     */
    public DocToBulk withRoutingFn(Write.FieldValueExtractFn routingFn) {
      checkArgument(routingFn != null, "routingFn must not be null");
      return builder().setRoutingFn(routingFn).build();
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
     * @return the {@link DocToBulk} with the function set
     */
    public DocToBulk withTypeFn(Write.FieldValueExtractFn typeFn) {
      checkArgument(typeFn != null, "typeFn must not be null");
      return builder().setTypeFn(typeFn).build();
    }

    /**
     * Provide an instruction to control whether partial updates or inserts (default) are issued to
     * Elasticsearch.
     *
     * @param usePartialUpdate set to true to issue partial updates
     * @return the {@link DocToBulk} with the partial update control set
     */
    public DocToBulk withUsePartialUpdate(boolean usePartialUpdate) {
      return builder().setUsePartialUpdate(usePartialUpdate).build();
    }

    /**
     * Provide an instruction to control whether the target index should be considered append-only.
     * For append-only indexes and/or data streams, only {@code create} operations will be issued,
     * instead of {@code index}, which is the default.
     *
     * <p>{@code create} fails if a document with the same ID already exists in the target, {@code
     * index} adds or replaces a document as necessary. If no ID is provided, both operations are
     * equivalent, unless you are writing to a <a
     * href="https://www.elastic.co/guide/en/elasticsearch/reference/current/data-streams.html">data
     * stream</a>. Data streams only support the {@code create} operation. For more information see
     * the <a
     * href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html#docs-bulk-api-desc>
     * Elasticsearch documentation</a>
     *
     * <p>Updates and deletions are not allowed, so related options will be ignored.
     *
     * <p>When the documents contain
     *
     * @param appendOnly set to true to allow only document appending
     * @return the {@link DocToBulk} with the-append only control set
     */
    public DocToBulk withAppendOnly(boolean appendOnly) {
      return builder().setAppendOnly(appendOnly).build();
    }

    /**
     * Whether to use scripted updates and what script to use.
     *
     * @param source set to the value of the script source, painless lang
     * @return the {@link DocToBulk} with the scripted updates set
     */
    public DocToBulk withUpsertScript(String source) {
      if (getBackendVersion() == null || getBackendVersion() == 2) {
        LOG.warn("Painless scripts are not supported on Elasticsearch clusters before version 5.0");
      }
      return builder().setUsePartialUpdate(false).setUpsertScript(source).build();
    }

    /**
     * Provide a function to extract the doc version from the document. This version number will be
     * used as the document version in Elasticsearch. Should the function throw an Exception then
     * the batch will fail and the exception propagated. Incompatible with update operations and
     * should only be used with withUsePartialUpdate(false)
     *
     * @param docVersionFn to extract the document version
     * @return the {@link DocToBulk} with the function set
     */
    public DocToBulk withDocVersionFn(Write.FieldValueExtractFn docVersionFn) {
      checkArgument(docVersionFn != null, "docVersionFn must not be null");
      return builder().setDocVersionFn(docVersionFn).build();
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
    public DocToBulk withIsDeleteFn(Write.BooleanFieldValueExtractFn isDeleteFn) {
      checkArgument(isDeleteFn != null, "deleteFn is required");
      return builder().setIsDeleteFn(isDeleteFn).build();
    }

    /**
     * Provide a function to extract the doc version from the document. This version number will be
     * used as the document version in Elasticsearch. Should the function throw an Exception then
     * the batch will fail and the exception propagated. Incompatible with update operations and
     * should only be used with withUsePartialUpdate(false)
     *
     * @param docVersionType the version type to use, one of {@value VERSION_TYPES}
     * @return the {@link DocToBulk} with the doc version type set
     */
    public DocToBulk withDocVersionType(String docVersionType) {
      checkArgument(
          VERSION_TYPES.contains(docVersionType),
          "docVersionType must be one of " + "%s",
          String.join(", ", VERSION_TYPES));
      return builder().setDocVersionType(docVersionType).build();
    }

    /**
     * Use to set explicitly which version of Elasticsearch the destination cluster is running.
     * Providing this hint means there is no need for setting {@link
     * DocToBulk#withConnectionConfiguration}. This can also be very useful for testing purposes.
     *
     * <p>Note: if the value of @param backendVersion differs from the version the destination
     * cluster is running, behavior is undefined and likely to yield errors.
     *
     * @param backendVersion the major version number of the version of Elasticsearch being run in
     *     the cluster where documents will be indexed.
     * @return the {@link DocToBulk} with the Elasticsearch major version number set
     */
    public DocToBulk withBackendVersion(int backendVersion) {
      checkArgument(
          VALID_CLUSTER_VERSIONS.contains(backendVersion),
          "Backend version may only be one of " + "%s",
          String.join(", ", VERSION_TYPES));
      maybeLogVersionDeprecationWarning(backendVersion);
      return builder().setBackendVersion(backendVersion).build();
    }

    @Override
    public PCollection<Document> expand(PCollection<String> docs) {
      ConnectionConfiguration connectionConfiguration = getConnectionConfiguration();
      Integer backendVersion = getBackendVersion();
      Write.FieldValueExtractFn idFn = getIdFn();
      Write.BooleanFieldValueExtractFn isDeleteFn = getIsDeleteFn();
      checkState(
          (backendVersion != null || connectionConfiguration != null),
          "withBackendVersion() or withConnectionConfiguration() is required");
      checkArgument(
          isDeleteFn == null || idFn != null,
          "Id needs to be specified by withIdFn for delete operation");

      return docs.apply(ParDo.of(new DocToBulkFn(this)));
    }

    // Encapsulates the elements which form the metadata for an Elasticsearch bulk operation
    private static class DocumentMetadata implements Serializable {
      final String index;
      final String type;
      final String id;
      final Integer retryOnConflict;
      final String routing;
      final Integer backendVersion;
      final String version;
      final String versionType;

      DocumentMetadata(
          String index,
          String type,
          String id,
          Integer retryOnConflict,
          String routing,
          Integer backendVersion,
          String version,
          String versionType) {
        this.index = index;
        this.id = id;
        this.type = type;
        this.retryOnConflict = retryOnConflict;
        this.routing = routing;
        this.backendVersion = backendVersion;
        this.version = version;
        this.versionType = versionType;
      }
    }

    private static class DocumentMetadataSerializer extends StdSerializer<DocumentMetadata> {
      private DocumentMetadataSerializer() {
        super(DocumentMetadata.class);
      }

      @Override
      public void serialize(DocumentMetadata value, JsonGenerator gen, SerializerProvider provider)
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
        if (value.routing != null) {
          gen.writeStringField("routing", value.routing);
        }
        if (value.retryOnConflict != null && value.backendVersion <= 6) {
          gen.writeNumberField("_retry_on_conflict", value.retryOnConflict);
        }
        if (value.retryOnConflict != null && value.backendVersion >= 7) {
          gen.writeNumberField("retry_on_conflict", value.retryOnConflict);
        }
        if (value.version != null) {
          gen.writeStringField("version", value.version);
        }
        if (value.versionType != null) {
          gen.writeStringField("version_type", value.versionType);
        }
        gen.writeEndObject();
      }
    }

    @VisibleForTesting
    static String createBulkApiEntity(DocToBulk spec, String document, int backendVersion)
        throws IOException {
      String documentMetadata = "{}";
      boolean isDelete = false;
      if (spec.getIndexFn() != null
          || spec.getTypeFn() != null
          || spec.getIdFn() != null
          || spec.getRoutingFn() != null) {
        // parse once and reused for efficiency
        JsonNode parsedDocument = OBJECT_MAPPER.readTree(document);
        documentMetadata = getDocumentMetadata(spec, parsedDocument, backendVersion);
        if (spec.getIsDeleteFn() != null) {
          isDelete = spec.getIsDeleteFn().apply(parsedDocument);
        }
      }
      final boolean isAppendOnly = Boolean.TRUE.equals(spec.getAppendOnly());

      if (isDelete) {
        checkState(!isAppendOnly, "No deletions allowed for append-only indices");
        // delete request used for deleting a document
        return String.format("{ \"delete\" : %s }%n", documentMetadata);
      }

      if (isAppendOnly) {
        return String.format("{ \"create\" : %s }%n%s%n", documentMetadata, document);
      }

      // index is an insert/upsert and update is a partial update (or insert if not
      // existing)
      if (Boolean.TRUE.equals(spec.getUsePartialUpdate())) {
        return String.format(
            "{ \"update\" : %s }%n{ \"doc\" : %s, " + "\"doc_as_upsert\" : true }%n",
            documentMetadata, document);
      } else if (spec.getUpsertScript() != null) {
        return String.format(
            "{ \"update\" : %s }%n{ \"script\" : {\"source\": \"%s\", "
                + "\"params\": %s}, \"upsert\" : %s, \"scripted_upsert\": true}%n",
            documentMetadata, spec.getUpsertScript(), document, document);
      } else {
        return String.format("{ \"index\" : %s }%n%s%n", documentMetadata, document);
      }
    }

    private static String lowerCaseOrNull(String input) {
      return input == null ? null : input.toLowerCase();
    }

    /**
     * Extracts the components that comprise the document address from the document using the {@link
     * Write.FieldValueExtractFn} configured. This allows any or all of the index, type and document
     * id to be controlled on a per document basis. If none are provided then an empty default of
     * {@code {}} is returned. Sanitization of the index is performed, automatically lower-casing
     * the value as required by Elasticsearch.
     *
     * @param parsedDocument the json from which the index, type and id may be extracted
     * @return the document address as JSON or the default
     * @throws IOException if the document cannot be parsed as JSON
     */
    private static String getDocumentMetadata(
        DocToBulk spec, JsonNode parsedDocument, int backendVersion) throws IOException {
      DocumentMetadata metadata =
          new DocumentMetadata(
              spec.getIndexFn() != null
                  ? lowerCaseOrNull(spec.getIndexFn().apply(parsedDocument))
                  : null,
              spec.getTypeFn() != null ? spec.getTypeFn().apply(parsedDocument) : null,
              spec.getIdFn() != null ? spec.getIdFn().apply(parsedDocument) : null,
              (Boolean.TRUE.equals(spec.getUsePartialUpdate())
                      || (spec.getUpsertScript() != null && !spec.getUpsertScript().isEmpty()))
                  ? DEFAULT_RETRY_ON_CONFLICT
                  : null,
              spec.getRoutingFn() != null ? spec.getRoutingFn().apply(parsedDocument) : null,
              backendVersion,
              spec.getDocVersionFn() != null ? spec.getDocVersionFn().apply(parsedDocument) : null,
              spec.getDocVersionType());
      return OBJECT_MAPPER.writeValueAsString(metadata);
    }

    /** {@link DoFn} to for the {@link DocToBulk} transform. */
    @VisibleForTesting
    static class DocToBulkFn extends DoFn<String, Document> {
      private final DocToBulk spec;
      private int backendVersion;

      public DocToBulkFn(DocToBulk spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() throws IOException {
        if (spec.getBackendVersion() != null) {
          backendVersion = spec.getBackendVersion();
        } else {
          backendVersion = ElasticsearchIO.getBackendVersion(spec.getConnectionConfiguration());
        }
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws IOException {
        String inputDoc = c.element();
        String bulkDirective = createBulkApiEntity(spec, inputDoc, backendVersion);
        c.output(
            Document.create()
                .withInputDoc(inputDoc)
                .withBulkDirective(bulkDirective)
                // N.B. Saving the element timestamp for later use allows for exactly emulating
                // c.output(...) because c.output is equivalent to
                // c.outputWithTimestamp(..., c.timestamp())
                .withTimestamp(c.timestamp()));
      }
    }
  }

  public static class DocumentCoder extends AtomicCoder<Document> implements Serializable {
    private static final DocumentCoder INSTANCE = new DocumentCoder();

    private DocumentCoder() {}

    public static DocumentCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(Document value, OutputStream outStream) throws IOException {
      NullableCoder.of(StringUtf8Coder.of()).encode(value.getInputDoc(), outStream);
      NullableCoder.of(StringUtf8Coder.of()).encode(value.getBulkDirective(), outStream);
      BooleanCoder.of().encode(value.getHasError(), outStream);
      NullableCoder.of(StringUtf8Coder.of()).encode(value.getResponseItemJson(), outStream);
      NullableCoder.of(InstantCoder.of()).encode(value.getTimestamp(), outStream);
    }

    @Override
    public Document decode(InputStream inStream) throws IOException {
      String inputDoc = NullableCoder.of(StringUtf8Coder.of()).decode(inStream);
      String bulkDirective = NullableCoder.of(StringUtf8Coder.of()).decode(inStream);
      boolean hasError = BooleanCoder.of().decode(inStream);
      String responseItemJson = NullableCoder.of(StringUtf8Coder.of()).decode(inStream);
      Instant timestamp = NullableCoder.of(InstantCoder.of()).decode(inStream);

      return Document.create()
          .withInputDoc(inputDoc)
          .withBulkDirective(bulkDirective)
          .withHasError(hasError)
          .withResponseItemJson(responseItemJson)
          .withTimestamp(timestamp);
    }
  }

  // Immutable POJO for maintaining various states of documents and their bulk representation, plus
  // response from ES for the given document and the timestamp of the data
  @DefaultCoder(DocumentCoder.class)
  @AutoValue
  public abstract static class Document implements Serializable {
    public abstract @Nullable String getInputDoc();

    public abstract @Nullable String getBulkDirective();

    public abstract Boolean getHasError();

    public abstract @Nullable String getResponseItemJson();

    public abstract @Nullable Instant getTimestamp();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setInputDoc(String inputDoc);

      abstract Builder setBulkDirective(String bulkDirective);

      abstract Builder setHasError(boolean hasError);

      abstract Builder setResponseItemJson(String responseItemJson);

      abstract Builder setTimestamp(Instant timestamp);

      abstract Document build();
    }

    public static Document create() {
      return new AutoValue_ElasticsearchIO_Document.Builder().setHasError(false).build();
    }

    /**
     * Sets the input document i.e. desired document that will end up in Elasticsearch for this
     * WriteSummary object. The inputDoc will be a document that was part of the input PCollection
     * to either {@link Write} or {@link DocToBulk}
     *
     * @param inputDoc Serialized json input document destined to end up in Elasticsearch.
     * @return WriteSummary with inputDocument set.
     */
    public Document withInputDoc(String inputDoc) {
      return toBuilder().setInputDoc(inputDoc).build();
    }

    /**
     * Sets the bulk directive representation of an input document. This will be new-line separated
     * JSON where each line is valid JSON. Typically the first line includes meta-data and
     * instructions to Elasticsearch such as whether to overwrite a document, delete it, etc. and
     * the second line (if present) will be the document itself. For more info please see
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
     *
     * @param bulkDirective Serialized new-line delimited json bulk API information.
     * @return WriteSummary with bulkDirective set.
     */
    public Document withBulkDirective(String bulkDirective) {
      return toBuilder().setBulkDirective(bulkDirective).build();
    }

    /**
     * Sets the element from Elasticsearch Bulk API response "items" pertaining to this
     * WriteSummary.
     *
     * @param responseItemJson The Elasticsearch Bulk API response.
     * @return WriteSummary with Elasticsearch Bulk API response set.
     */
    public Document withResponseItemJson(String responseItemJson) {
      return toBuilder().setResponseItemJson(responseItemJson).build();
    }

    /**
     * Used to set whether or not there was an error for a given document as indicated by the
     * response from Elasticsearch. Note that if using {@link Write#withAllowableResponseErrors}
     * errors which are allowed will have a false value for hasError for their respective
     * WriteSummary.
     *
     * @param hasError Whether or not Elasticsearch returned an error when persisting a bulk
     *     directive.
     * @return WriteSummary with hasError set.
     */
    public Document withHasError(boolean hasError) {
      return toBuilder().setHasError(hasError).build();
    }

    /**
     * Sets the timestamp of the element in the PCollection, to be used in order to output
     * WriteSummary to the same window from which the inputDoc originated.
     *
     * @param timestamp The timestamp with which the WriteSummary will be output.
     * @return WriteSummary with timestamp set.
     */
    public Document withTimestamp(Instant timestamp) {
      return toBuilder().setTimestamp(timestamp).build();
    }
  }
  /**
   * A {@link PTransform} writing data to Elasticsearch.
   *
   * <p>This {@link PTransform} acts as a convenience wrapper for doing both document to bulk API
   * serialization as well as batching those Bulk API entities and writing them to an Elasticsearch
   * cluster. This class is effectively a thin proxy for DocToBulk->BulkIO all-in-one for
   * convenience and backward compatibility.
   */
  public static class Write extends PTransform<PCollection<String>, PCollectionTuple> {
    public interface FieldValueExtractFn extends SerializableFunction<JsonNode, String> {}

    public interface BooleanFieldValueExtractFn extends SerializableFunction<JsonNode, Boolean> {}

    // N.B. Be sure to create tuple tags as new anonymous subclasses to avoid type erasure issues
    public static final TupleTag<Document> SUCCESSFUL_WRITES = new TupleTag<Document>() {};
    public static final TupleTag<Document> FAILED_WRITES = new TupleTag<Document>() {};

    private DocToBulk docToBulk = new AutoValue_ElasticsearchIO_DocToBulk.Builder().build();

    private BulkIO bulkIO =
        new AutoValue_ElasticsearchIO_BulkIO.Builder()
            // advised default starting batch size in ES docs
            .setMaxBatchSize(1000L)
            // advised default starting batch size in ES docs
            .setMaxBatchSizeBytes(5L * 1024L * 1024L)
            .setUseStatefulBatches(false)
            .setMaxParallelRequests(1)
            .setThrowWriteErrors(true)
            .build();

    public DocToBulk getDocToBulk() {
      return docToBulk;
    }

    public BulkIO getBulkIO() {
      return bulkIO;
    }

    // For building Doc2Bulk
    /** Refer to {@link DocToBulk#withIdFn}. */
    public Write withIdFn(FieldValueExtractFn idFn) {
      docToBulk = docToBulk.withIdFn(idFn);
      return this;
    }

    /** Refer to {@link DocToBulk#withIndexFn}. */
    public Write withIndexFn(FieldValueExtractFn indexFn) {
      docToBulk = docToBulk.withIndexFn(indexFn);
      return this;
    }

    /** Refer to {@link DocToBulk#withRoutingFn}. */
    public Write withRoutingFn(FieldValueExtractFn routingFn) {
      docToBulk = docToBulk.withRoutingFn(routingFn);
      return this;
    }

    /** Refer to {@link DocToBulk#withTypeFn}. */
    public Write withTypeFn(FieldValueExtractFn typeFn) {
      docToBulk = docToBulk.withTypeFn(typeFn);
      return this;
    }

    /** Refer to {@link DocToBulk#withDocVersionFn}. */
    public Write withDocVersionFn(FieldValueExtractFn docVersionFn) {
      docToBulk = docToBulk.withDocVersionFn(docVersionFn);
      return this;
    }

    /** Refer to {@link DocToBulk#withDocVersionType}. */
    public Write withDocVersionType(String docVersionType) {
      docToBulk = docToBulk.withDocVersionType(docVersionType);
      return this;
    }

    /** Refer to {@link DocToBulk#withUsePartialUpdate}. */
    public Write withUsePartialUpdate(boolean usePartialUpdate) {
      docToBulk = docToBulk.withUsePartialUpdate(usePartialUpdate);
      return this;
    }

    /** Refer to {@link DocToBulk#withAppendOnly}. */
    public Write withAppendOnly(boolean appendOnly) {
      docToBulk = docToBulk.withAppendOnly(appendOnly);
      return this;
    }

    /** Refer to {@link DocToBulk#withUpsertScript}. */
    public Write withUpsertScript(String source) {
      docToBulk = docToBulk.withUpsertScript(source);
      return this;
    }

    /** Refer to {@link DocToBulk#withBackendVersion}. */
    public Write withBackendVersion(int backendVersion) {
      docToBulk = docToBulk.withBackendVersion(backendVersion);
      return this;
    }

    /** Refer to {@link DocToBulk#withIsDeleteFn}. */
    public Write withIsDeleteFn(Write.BooleanFieldValueExtractFn isDeleteFn) {
      docToBulk = docToBulk.withIsDeleteFn(isDeleteFn);
      return this;
    }
    // End building Doc2Bulk

    /** Refer to {@link BulkIO#withConnectionConfiguration}. */
    public Write withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkArgument(connectionConfiguration != null, "connectionConfiguration can not be null");
      docToBulk = docToBulk.withConnectionConfiguration(connectionConfiguration);
      bulkIO = bulkIO.withConnectionConfiguration(connectionConfiguration);
      return this;
    }

    /** Refer to {@link BulkIO#withMaxBatchSize}. */
    public Write withMaxBatchSize(long batchSize) {
      bulkIO = bulkIO.withMaxBatchSize(batchSize);
      return this;
    }

    /** Refer to {@link BulkIO#withMaxBatchSizeBytes}. */
    public Write withMaxBatchSizeBytes(long batchSizeBytes) {
      bulkIO = bulkIO.withMaxBatchSizeBytes(batchSizeBytes);
      return this;
    }

    /** Refer to {@link BulkIO#withRetryConfiguration}. */
    public Write withRetryConfiguration(RetryConfiguration retryConfiguration) {
      bulkIO = bulkIO.withRetryConfiguration(retryConfiguration);
      return this;
    }

    /** Refer to {@link BulkIO#withIgnoreVersionConflicts}. */
    public Write withIgnoreVersionConflicts(boolean ignoreVersionConflicts) {
      bulkIO = bulkIO.withIgnoreVersionConflicts(ignoreVersionConflicts);
      return this;
    }

    /** Refer to {@link BulkIO#withUseStatefulBatches}. */
    public Write withUseStatefulBatches(boolean useStatefulBatches) {
      bulkIO = bulkIO.withUseStatefulBatches(useStatefulBatches);
      return this;
    }

    /** Refer to {@link BulkIO#withMaxBufferingDuration}. */
    public Write withMaxBufferingDuration(Duration maxBufferingDuration) {
      bulkIO = bulkIO.withMaxBufferingDuration(maxBufferingDuration);
      return this;
    }

    /**
     * Refer to {@link BulkIO#withMaxParallelRequestsPerWindow}.
     *
     * @deprecated use {@link Write#withMaxParallelRequests} instead
     */
    @Deprecated
    public Write withMaxParallelRequestsPerWindow(int maxParallelRequestsPerWindow) {
      bulkIO = bulkIO.withMaxParallelRequestsPerWindow(maxParallelRequestsPerWindow);
      return this;
    }

    /** Refer to {@link BulkIO#withMaxParallelRequests}. */
    public Write withMaxParallelRequests(int maxParallelRequests) {
      bulkIO = bulkIO.withMaxParallelRequests(maxParallelRequests);
      return this;
    }

    /** Refer to {@link BulkIO#withAllowableResponseErrors}. */
    public Write withAllowableResponseErrors(@Nullable Set<String> allowableResponseErrors) {
      if (allowableResponseErrors == null) {
        allowableResponseErrors = new HashSet<>();
      }

      bulkIO = bulkIO.withAllowableResponseErrors(allowableResponseErrors);
      return this;
    }

    /** Refer to {@link BulkIO#withThrowWriteErrors}. */
    public Write withThrowWriteErrors(boolean throwWriteErrors) {
      bulkIO = bulkIO.withThrowWriteErrors(throwWriteErrors);
      return this;
    }

    @Override
    public PCollectionTuple expand(PCollection<String> input) {
      return input.apply(docToBulk).apply(bulkIO);
    }
  }

  /**
   * A {@link PTransform} writing Bulk API entities created by {@link ElasticsearchIO.DocToBulk} to
   * an Elasticsearch cluster. Typically, using {@link ElasticsearchIO.Write} is preferred, whereas
   * using {@link ElasticsearchIO.DocToBulk} and BulkIO separately is for advanced use cases such as
   * mirroring data to multiple clusters or data lakes without recomputation.
   */
  @AutoValue
  public abstract static class BulkIO extends PTransform<PCollection<Document>, PCollectionTuple> {
    @VisibleForTesting
    static final String RETRY_ATTEMPT_LOG = "Error writing to Elasticsearch. Retry attempt[{}]";

    @VisibleForTesting
    static final String RETRY_FAILED_LOG =
        "Error writing to ES after %d attempt(s). No more attempts allowed";

    abstract @Nullable ConnectionConfiguration getConnectionConfiguration();

    abstract long getMaxBatchSize();

    abstract long getMaxBatchSizeBytes();

    abstract @Nullable Duration getMaxBufferingDuration();

    abstract boolean getUseStatefulBatches();

    abstract @Nullable Integer getMaxParallelRequestsPerWindow();

    abstract int getMaxParallelRequests();

    abstract @Nullable RetryConfiguration getRetryConfiguration();

    abstract @Nullable Set<String> getAllowedResponseErrors();

    abstract boolean getThrowWriteErrors();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration connectionConfiguration);

      abstract Builder setMaxBatchSize(long maxBatchSize);

      abstract Builder setMaxBatchSizeBytes(long maxBatchSizeBytes);

      abstract Builder setRetryConfiguration(RetryConfiguration retryConfiguration);

      abstract Builder setAllowedResponseErrors(Set<String> allowedResponseErrors);

      abstract Builder setMaxBufferingDuration(Duration maxBufferingDuration);

      abstract Builder setUseStatefulBatches(boolean useStatefulBatches);

      /** @deprecated Use {@link #setMaxParallelRequests} instead. */
      @Deprecated
      abstract Builder setMaxParallelRequestsPerWindow(int maxParallelRequestsPerWindow);

      abstract Builder setMaxParallelRequests(int maxParallelRequests);

      abstract Builder setThrowWriteErrors(boolean throwWriteErrors);

      abstract BulkIO build();
    }

    /**
     * Provide the Elasticsearch connection configuration object.
     *
     * @param connectionConfiguration the Elasticsearch {@link ConnectionConfiguration} object
     * @return the {@link BulkIO} with connection configuration set
     */
    public BulkIO withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkArgument(connectionConfiguration != null, "connectionConfiguration can not be null");

      return builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    /**
     * Provide a maximum size in number of documents for the batch see bulk API
     * (https://www.elastic.co/guide/en/elasticsearch/reference/7.17/docs-bulk.html). Default is
     * 1000 docs (like Elasticsearch bulk size advice). See
     * https://www.elastic.co/guide/en/elasticsearch/guide/current/bulk.html Depending on the
     * execution engine, size of bundles may vary, this sets the maximum size. Change this if you
     * need to have smaller ElasticSearch bulks.
     *
     * @param batchSize maximum batch size in number of documents
     * @return the {@link BulkIO} with connection batch size set
     */
    public BulkIO withMaxBatchSize(long batchSize) {
      checkArgument(batchSize > 0, "batchSize must be > 0, but was %s", batchSize);
      return builder().setMaxBatchSize(batchSize).build();
    }

    /**
     * Provide a maximum size in bytes for the batch see bulk API
     * (https://www.elastic.co/guide/en/elasticsearch/reference/7.17/docs-bulk.html). Default is 5MB
     * (like Elasticsearch bulk size advice). See
     * https://www.elastic.co/guide/en/elasticsearch/guide/current/bulk.html Depending on the
     * execution engine, size of bundles may vary, this sets the maximum size. Change this if you
     * need to have smaller ElasticSearch bulks.
     *
     * @param batchSizeBytes maximum batch size in bytes
     * @return the {@link BulkIO} with connection batch size in bytes set
     */
    public BulkIO withMaxBatchSizeBytes(long batchSizeBytes) {
      checkArgument(batchSizeBytes > 0, "batchSizeBytes must be > 0, but was %s", batchSizeBytes);
      return builder().setMaxBatchSizeBytes(batchSizeBytes).build();
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
     * @return the {@link BulkIO} with retrying configured
     */
    public BulkIO withRetryConfiguration(RetryConfiguration retryConfiguration) {
      checkArgument(retryConfiguration != null, "retryConfiguration is required");
      return builder().setRetryConfiguration(retryConfiguration).build();
    }

    /**
     * Whether or not to suppress version conflict errors in a Bulk API response. This can be useful
     * if your use case involves using external version types.
     *
     * @param ignoreVersionConflicts true to suppress version conflicts, false to surface version
     *     conflict errors.
     * @return the {@link BulkIO} with version conflict handling configured
     */
    public BulkIO withIgnoreVersionConflicts(boolean ignoreVersionConflicts) {
      Set<String> allowedResponseErrors = getAllowedResponseErrors();
      if (allowedResponseErrors == null) {
        allowedResponseErrors = new HashSet<>();
      }
      if (ignoreVersionConflicts) {
        allowedResponseErrors.add(VERSION_CONFLICT_ERROR);
      }

      return builder().setAllowedResponseErrors(allowedResponseErrors).build();
    }

    /**
     * Provide a set of textual error types which can be contained in Bulk API response
     * items[].error.type field. Any element in @param allowableResponseErrorTypes will suppress
     * errors of the same type in Bulk responses.
     *
     * <p>See also
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html#bulk-failures-ex
     *
     * @param allowableResponseErrorTypes
     * @return the {@link BulkIO} with allowable response errors set
     */
    public BulkIO withAllowableResponseErrors(@Nullable Set<String> allowableResponseErrorTypes) {
      if (allowableResponseErrorTypes == null) {
        allowableResponseErrorTypes = new HashSet<>();
      }

      return builder().setAllowedResponseErrors(allowableResponseErrorTypes).build();
    }

    /**
     * If using {@link BulkIO#withUseStatefulBatches}, this can be used to set a maximum elapsed
     * time before buffered elements are emitted to Elasticsearch as a Bulk API request. If this
     * config is not set, Bulk requests will not be issued until {@link BulkIO#getMaxBatchSize}
     * number of documents have been buffered. This may result in higher latency in particular if
     * your max batch size is set to a large value and your pipeline input is low volume.
     *
     * @param maxBufferingDuration the maximum duration to wait before sending any buffered
     *     documents to Elasticsearch, regardless of maxBatchSize.
     * @return the {@link BulkIO} with maximum buffering duration set
     */
    public BulkIO withMaxBufferingDuration(Duration maxBufferingDuration) {
      LOG.warn(
          "Use of withMaxBufferingDuration requires withUseStatefulBatches(true). "
              + "Setting that automatically.");
      return builder()
          .setUseStatefulBatches(true)
          .setMaxBufferingDuration(maxBufferingDuration)
          .build();
    }

    /**
     * Whether or not to use Stateful Processing to ensure bulk requests have the desired number of
     * entities i.e. as close to the maxBatchSize as possible. By default without this feature
     * enabled, Bulk requests will not contain more than maxBatchSize entities, but the lower bound
     * of batch size is determined by Beam Runner bundle sizes, which may be as few as 1.
     *
     * @param useStatefulBatches true enables the use of Stateful Processing to ensure that batches
     *     are as close to the maxBatchSize as possible.
     * @return the {@link BulkIO} with Stateful Processing enabled or disabled
     */
    public BulkIO withUseStatefulBatches(boolean useStatefulBatches) {
      return builder().setUseStatefulBatches(useStatefulBatches).build();
    }

    /**
     * When using {@link BulkIO#withUseStatefulBatches} Stateful Processing, states and therefore
     * batches are maintained per-key-per-window. BE AWARE that low values for @param
     * maxParallelRequests, in particular if the input data has a finite number of windows, can
     * reduce parallelism greatly. Because data will be temporarily globally windowed as part of
     * writing data to Elasticsearch, if @param maxParallelRequests is set to 1, there will only
     * ever be 1 request in flight. Having only a single request in flight can be beneficial for
     * ensuring an Elasticsearch cluster is not overwhelmed by parallel requests, but may not work
     * for all use cases. If this number is less than the number of maximum workers in your
     * pipeline, the IO work will result in a sub-optimal distribution of the write step with most
     * runners.
     *
     * @param maxParallelRequests the maximum number of parallel bulk requests for a window of data
     * @return the {@link BulkIO} with maximum parallel bulk requests per window set
     * @deprecated use {@link BulkIO#withMaxParallelRequests} instead.
     */
    @Deprecated
    public BulkIO withMaxParallelRequestsPerWindow(int maxParallelRequests) {
      checkArgument(
          maxParallelRequests > 0, "maxParallelRequestsPerWindow value must be a positive integer");
      return builder().setMaxParallelRequests(maxParallelRequests).build();
    }

    /**
     * When using {@link BulkIO#withUseStatefulBatches} Stateful Processing, states and therefore
     * batches are maintained per-key-per-window. BE AWARE that low values for @param
     * maxParallelRequests, in particular if the input data has a finite number of windows, can
     * reduce parallelism greatly. Because data will be temporarily globally windowed as part of
     * writing data to Elasticsearch, if @param maxParallelRequests is set to 1, there will only
     * ever be 1 request in flight. Having only a single request in flight can be beneficial for
     * ensuring an Elasticsearch cluster is not overwhelmed by parallel requests, but may not work
     * for all use cases. If this number is less than the number of maximum workers in your
     * pipeline, the IO work will result in a sub-optimal distribution of the write step with most
     * runners.
     *
     * @param maxParallelRequests the maximum number of parallel bulk requests
     * @return the {@link BulkIO} with maximum parallel bulk requests
     */
    public BulkIO withMaxParallelRequests(int maxParallelRequests) {
      checkArgument(
          maxParallelRequests > 0, "maxParallelRequests value must be a positive integer");
      return builder().setMaxParallelRequests(maxParallelRequests).build();
    }

    /**
     * Whether to throw runtime exceptions when write (IO) errors occur. Especially useful in
     * streaming pipelines where non-transient IO failures will cause infinite retries. If true, a
     * runtime error will be thrown for any error found by {@link
     * ElasticsearchIO#createWriteReport}. If false, a {@link PCollectionTuple} will be returned
     * with tags {@link Write#SUCCESSFUL_WRITES} and {@link Write#FAILED_WRITES}, each being a
     * {@link PCollection} of {@link Document} representing documents which were written to
     * Elasticsearch without errors and those which failed to write due to errors, respectively.
     *
     * @param throwWriteErrors whether to surface write errors as runtime exceptions or return them
     *     in a {@link PCollection}
     * @return the {@link BulkIO} with write error treatment configured
     */
    public BulkIO withThrowWriteErrors(boolean throwWriteErrors) {
      return builder().setThrowWriteErrors(throwWriteErrors).build();
    }

    /**
     * Creates batches of documents using Stateful Processing based on user configurable settings of
     * withMaxBufferingDuration and withMaxParallelRequestsPerWindow.
     *
     * <p>Mostly exists for testability of withMaxParallelRequestsPerWindow.
     */
    @VisibleForTesting
    static class StatefulBatching
        extends PTransform<PCollection<Document>, PCollection<KV<Integer, Iterable<Document>>>> {
      final BulkIO spec;

      private StatefulBatching(BulkIO bulkSpec) {
        spec = bulkSpec;
      }

      public static StatefulBatching fromSpec(BulkIO spec) {
        return new StatefulBatching(spec);
      }

      @Override
      public PCollection<KV<Integer, Iterable<Document>>> expand(PCollection<Document> input) {
        GroupIntoBatches<Integer, Document> groupIntoBatches =
            GroupIntoBatches.ofSize(spec.getMaxBatchSize());

        if (spec.getMaxBufferingDuration() != null) {
          groupIntoBatches =
              groupIntoBatches.withMaxBufferingDuration(spec.getMaxBufferingDuration());
        }

        return input
            .apply(ParDo.of(new Reshuffle.AssignShardFn<>(spec.getMaxParallelRequests())))
            .apply(groupIntoBatches);
      }
    }

    @Override
    public PCollectionTuple expand(PCollection<Document> input) {
      ConnectionConfiguration connectionConfiguration = getConnectionConfiguration();
      checkState(connectionConfiguration != null, "withConnectionConfiguration() is required");

      @SuppressWarnings("unchecked")
      WindowFn<Document, ?> originalWindowFn =
          (WindowFn<Document, ?>) input.getWindowingStrategy().getWindowFn();

      PCollection<Document> docResults;
      PCollection<Document> globalDocs =
          input.apply("Window inputs globally", Window.into(new GlobalWindows()));

      if (getUseStatefulBatches()) {
        docResults =
            globalDocs
                .apply(StatefulBatching.fromSpec(this))
                .apply(ParDo.of(new BulkIOStatefulFn(this)));
      } else {
        docResults = globalDocs.apply(ParDo.of(new BulkIOBundleFn(this)));
      }

      return docResults
          // Restore windowing of input
          .apply("Restore original windows", Window.into(originalWindowFn))
          .apply(
              ParDo.of(new ResultFilteringFn())
                  .withOutputTags(Write.SUCCESSFUL_WRITES, TupleTagList.of(Write.FAILED_WRITES)));
    }

    private static class ResultFilteringFn extends DoFn<Document, Document> {
      @ProcessElement
      public void processElement(@Element Document doc, MultiOutputReceiver out) {
        if (doc.getHasError()) {
          out.get(Write.FAILED_WRITES).output(doc);
        } else {
          out.get(Write.SUCCESSFUL_WRITES).output(doc);
        }
      }
    }

    static class BulkIOBundleFn extends BulkIOBaseFn<Document> {
      @VisibleForTesting
      BulkIOBundleFn(BulkIO bulkSpec) {
        super(bulkSpec);
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        // the element KV pair is a pair of raw_doc + resulting Bulk API formatted newline-json
        // based on DocToBulk settings
        addAndMaybeFlush(context.element(), context);
      }
    }

    /*
    Intended for use in conjunction with {@link GroupIntoBatches}
     */
    static class BulkIOStatefulFn extends BulkIOBaseFn<KV<Integer, Iterable<Document>>> {
      @VisibleForTesting
      BulkIOStatefulFn(BulkIO bulkSpec) {
        super(bulkSpec);
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        for (Document timedDoc : context.element().getValue()) {
          addAndMaybeFlush(timedDoc, context);
        }
      }
    }

    /** {@link DoFn} to for the {@link BulkIO} transform. */
    @VisibleForTesting
    private abstract static class BulkIOBaseFn<T> extends DoFn<T, Document> {
      private static final Duration RETRY_INITIAL_BACKOFF = Duration.standardSeconds(5);
      private transient FluentBackoff retryBackoff;

      private BulkIO spec;
      private transient RestClient restClient;
      private transient List<Document> batch;
      long currentBatchSizeBytes;

      protected BulkIOBaseFn(BulkIO bulkSpec) {
        this.spec = bulkSpec;
      }

      @Override
      public Duration getAllowedTimestampSkew() {
        return Duration.millis(Long.MAX_VALUE);
      }

      @Setup
      public void setup() throws IOException {
        ConnectionConfiguration connectionConfiguration = spec.getConnectionConfiguration();

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
      }

      @StartBundle
      public void startBundle(StartBundleContext context) {
        batch = new ArrayList<>();
        currentBatchSizeBytes = 0;
      }

      @FinishBundle
      public void finishBundle(FinishBundleContext context)
          throws IOException, InterruptedException {
        flushAndOutputResults(new FinishBundleContextAdapter<>(context));
      }

      /**
       * Adapter interface which provides a common parent for {@link ProcessContext} and {@link
       * FinishBundleContext} so that we are able to use a single common invocation to output from.
       */
      interface ContextAdapter {
        void output(Document timedDoc);
      }

      private static final class ProcessContextAdapter<T> implements ContextAdapter {
        private final DoFn<T, Document>.ProcessContext context;

        private ProcessContextAdapter(DoFn<T, Document>.ProcessContext context) {
          this.context = context;
        }

        @Override
        public void output(Document timedDoc) {
          context.outputWithTimestamp(timedDoc, timedDoc.getTimestamp());
        }
      }

      private static final class FinishBundleContextAdapter<T> implements ContextAdapter {
        private final DoFn<T, Document>.FinishBundleContext context;

        private FinishBundleContextAdapter(DoFn<T, Document>.FinishBundleContext context) {
          this.context = context;
        }

        @Override
        public void output(Document timedDoc) {
          context.output(timedDoc, timedDoc.getTimestamp(), GlobalWindow.INSTANCE);
        }
      }

      private void flushAndOutputResults(ContextAdapter context)
          throws IOException, InterruptedException {
        for (Document timedDoc : flushBatch()) {
          context.output(timedDoc);
        }
      }

      protected void addAndMaybeFlush(Document doc, ProcessContext context)
          throws IOException, InterruptedException {

        batch.add(doc);
        currentBatchSizeBytes += doc.getBulkDirective().getBytes(StandardCharsets.UTF_8).length;

        if (batch.size() >= spec.getMaxBatchSize()
            || currentBatchSizeBytes >= spec.getMaxBatchSizeBytes()) {
          flushAndOutputResults(new ProcessContextAdapter<>(context));
        }
      }

      private boolean isRetryableClientException(Throwable t) {
        // RestClient#performRequest only throws wrapped IOException so we must inspect the
        // exception cause to determine if the exception is likely transient i.e. retryable or
        // not.
        return t.getCause() instanceof ConnectTimeoutException
            || t.getCause() instanceof SocketTimeoutException
            || t.getCause() instanceof ConnectionClosedException
            || t.getCause() instanceof ConnectException;
      }

      private List<Document> flushBatch() throws IOException, InterruptedException {

        if (batch.isEmpty()) {
          return new ArrayList<>();
        }

        LOG.debug(
            "ElasticsearchIO batch size: {}, batch size bytes: {}",
            batch.size(),
            currentBatchSizeBytes);

        StringBuilder bulkRequest = new StringBuilder();
        // Create a stable list of input entries, because order is important to keep constant
        List<Document> inputEntries = new ArrayList<>(batch);

        batch.clear();
        currentBatchSizeBytes = 0L;

        for (Document doc : inputEntries) {
          // N.B. we need to ensure that we can iterate in the same order later to match up
          // responses to these bulk directives. ES Bulk response `items` is in the same order
          // as the bulk directives in the request, so order is imperative.
          bulkRequest.append(doc.getBulkDirective());
        }

        Response response = null;
        HttpEntity responseEntity = null;

        // Elasticsearch will default to the index/type provided the {@link
        // ConnectionConfiguration} if none are set in the document meta (i.e.
        // using ElasticsearchIO$DocToBulk#withIndexFn and
        // ElasticsearchIO$DocToBulk#withTypeFn options)
        String endPoint = spec.getConnectionConfiguration().getBulkEndPoint();

        HttpEntity requestBody =
            new NStringEntity(bulkRequest.toString(), ContentType.APPLICATION_JSON);
        try {
          Request request = new Request("POST", endPoint);
          request.addParameters(Collections.emptyMap());
          request.setEntity(requestBody);
          response = restClient.performRequest(request);
          responseEntity = new BufferedHttpEntity(response.getEntity());
        } catch (java.io.IOException ex) {
          if (spec.getRetryConfiguration() == null || !isRetryableClientException(ex)) {
            throw ex;
          }
          LOG.error("Caught ES timeout, retrying", ex);
        }

        if (spec.getRetryConfiguration() != null
            && (response == null
                || responseEntity == null
                || spec.getRetryConfiguration().getRetryPredicate().test(responseEntity))) {
          if (responseEntity != null
              && spec.getRetryConfiguration().getRetryPredicate().test(responseEntity)) {
            LOG.warn("ES Cluster is responding with HTP 429 - TOO_MANY_REQUESTS.");
          }
          responseEntity = handleRetry("POST", endPoint, Collections.emptyMap(), requestBody);
        }

        List<Document> responses =
            createWriteReport(
                responseEntity, spec.getAllowedResponseErrors(), spec.getThrowWriteErrors());

        return Streams.zip(
                inputEntries.stream(),
                responses.stream(),
                (inputTimedDoc, responseDoc) ->
                    inputTimedDoc
                        .withHasError(responseDoc.getHasError())
                        .withResponseItemJson(responseDoc.getResponseItemJson()))
            .collect(Collectors.toList());
      }

      /** retry request based on retry configuration policy. */
      private HttpEntity handleRetry(
          String method, String endpoint, Map<String, String> params, HttpEntity requestBody)
          throws IOException, InterruptedException {
        Response response;
        HttpEntity responseEntity = null;
        Sleeper sleeper = Sleeper.DEFAULT;
        BackOff backoff = retryBackoff.backoff();
        int attempt = 0;
        // while retry policy exists
        while (BackOffUtils.next(sleeper, backoff)) {
          LOG.warn(RETRY_ATTEMPT_LOG, ++attempt);
          try {
            Request request = new Request(method, endpoint);
            request.addParameters(params);
            request.setEntity(requestBody);
            response = restClient.performRequest(request);
            responseEntity = new BufferedHttpEntity(response.getEntity());
          } catch (java.io.IOException ex) {
            if (isRetryableClientException(ex)) {
              LOG.error("Caught ES timeout, retrying", ex);
              continue;
            }
          }
          // if response has no 429 errors
          if (!Objects.requireNonNull(spec.getRetryConfiguration())
              .getRetryPredicate()
              .test(responseEntity)) {
            return responseEntity;
          } else {
            LOG.warn("ES Cluster is responding with HTP 429 - TOO_MANY_REQUESTS.");
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

  private static void maybeLogVersionDeprecationWarning(int clusterVersion) {
    if (DEPRECATED_CLUSTER_VERSIONS.contains(clusterVersion)) {
      LOG.warn(
          "Support for Elasticsearch cluster version {} will be dropped in a future release of "
              + "the Apache Beam SDK",
          clusterVersion);
    }
  }

  static int getBackendVersion(RestClient restClient) {
    try {
      Request request = new Request("GET", "");
      Response response = restClient.performRequest(request);
      JsonNode jsonNode = parseResponse(response.getEntity());
      int backendVersion =
          Integer.parseInt(jsonNode.path("version").path("number").asText().substring(0, 1));
      checkArgument(
          VALID_CLUSTER_VERSIONS.contains(backendVersion),
          "The Elasticsearch version to connect to is %s.x. "
              + "This version of the ElasticsearchIO is only compatible with "
              + "Elasticsearch "
              + VALID_CLUSTER_VERSIONS,
          backendVersion);
      maybeLogVersionDeprecationWarning(backendVersion);
      return backendVersion;

    } catch (IOException e) {
      throw new IllegalArgumentException("Cannot get Elasticsearch version", e);
    }
  }

  static int getBackendVersion(ConnectionConfiguration connectionConfiguration) {
    try (RestClient restClient = connectionConfiguration.createClient()) {
      return getBackendVersion(restClient);
    } catch (IOException e) {
      throw new IllegalArgumentException("Cannot get Elasticsearch version", e);
    }
  }
}
