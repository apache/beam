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

import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.ConnectionConfiguration;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.getBackendVersion;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.parseResponse;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestCommon.ES_TYPE;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestCommon.getEsIndex;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Document;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

/** Test utilities to use with {@link ElasticsearchIO}. */
@SuppressWarnings("nullness")
class ElasticsearchIOTestUtils {
  static final int ELASTICSEARCH_DEFAULT_PORT = 9200;
  static final String ELASTICSEARCH_PASSWORD = "superSecure";
  static final String ELASTIC_UNAME = "elastic";
  static final Set<Integer> INVALID_DOCS_IDS = new HashSet<>(Arrays.asList(6, 7));
  static final String INVALID_LONG_ID = new String(new char[513]).replace('\0', '2');
  static final String ALIAS_SUFFIX = "-aliased";

  static final String[] FAMOUS_SCIENTISTS = {
    "einstein",
    "darwin",
    "copernicus",
    "pasteur",
    "curie",
    "faraday",
    "newton",
    "bohr",
    "galilei",
    "maxwell"
  };
  static final ObjectMapper MAPPER = new ObjectMapper();
  static final int NUM_SCIENTISTS = FAMOUS_SCIENTISTS.length;
  static final String SCRIPT_SOURCE =
      "if(ctx._source.group != null) { ctx._source.group = params.id % 2 } else { ctx._source"
          + ".group = 0 }";

  /** Enumeration that specifies whether to insert malformed documents. */
  public enum InjectionMode {
    INJECT_SOME_INVALID_DOCS,
    DO_NOT_INJECT_INVALID_DOCS,
    INJECT_ONE_ID_TOO_LONG_DOC
  }

  /** Deletes the given index synchronously. */
  static void deleteIndex(ConnectionConfiguration connectionConfiguration, RestClient restClient)
      throws IOException {
    deleteIndex(restClient, connectionConfiguration.getIndex());
  }

  private static Response closeIndex(RestClient restClient, String index) throws IOException {
    Request request = new Request("POST", String.format("/%s/_close", index));
    return restClient.performRequest(request);
  }

  static void deleteIndex(RestClient restClient, String index) throws IOException {
    try {
      closeIndex(restClient, index);
      Request request = new Request("DELETE", String.format("/%s", index));
      restClient.performRequest(request);
    } catch (IOException e) {
      if (e.getMessage().contains("matches an alias")) {
        deleteIndex(restClient, index + ALIAS_SUFFIX);
        return;
      }
      // it is fine to ignore this expression as deleteIndex occurs in @before,
      // so when the first tests is run, the index does not exist yet
      if (!e.getMessage().contains("index_not_found_exception")) {
        throw e;
      }
    }
  }

  public static void createIndex(RestClient restClient, String indexName, boolean createAsAlias)
      throws IOException {
    deleteIndex(restClient, indexName);

    if (createAsAlias) {
      // The intent is that an alias by the name of @param indexName points to a newly created
      // index. This way, tests can validate that if the index targeted for reads/writes is
      // actually an alias, everything continues to work.
      String newIndexName = indexName + ALIAS_SUFFIX;
      Request request = new Request("PUT", String.format("/%s", newIndexName));
      restClient.performRequest(request);
      createIndexAlias(restClient, newIndexName, indexName);
    } else {
      Request request = new Request("PUT", String.format("/%s", indexName));
      restClient.performRequest(request);
    }
  }

  public static void createIndex(RestClient restClient, String indexName) throws IOException {
    createIndex(restClient, indexName, false);
  }

  public static void createIndexAlias(RestClient restClient, String indexName, String aliasName)
      throws IOException {
    Request request = new Request("PUT", String.format("/%s/_alias/%s", indexName, aliasName));
    restClient.performRequest(request);
  }

  public static void setDefaultTemplate(RestClient restClient) throws IOException {
    String templateUrl = "/_template/default";
    int backendVersion = getBackendVersion(restClient);

    if (backendVersion > 7) {
      templateUrl = "/_index_template/default";
    }

    Request request = new Request("PUT", templateUrl);
    String settings =
        "\"settings\": {"
            + "   \"index.number_of_shards\": 1,"
            + "   \"index.number_of_replicas\": 0,"
            + "   \"index.store.stats_refresh_interval\": 0"
            + "  }";
    String template = "\"*\",";

    if (backendVersion > 7) {
      template = "{" + settings + "}";
      settings = "";
    }

    NStringEntity body =
        new NStringEntity(
            String.format(
                "{" + "\"index_patterns\": [\"beam*\"]," + "\"template\": %s %s" + "}",
                template, settings),
            ContentType.APPLICATION_JSON);

    request.setEntity(body);
    restClient.performRequest(request);
  }

  /**
   * Synchronously deletes the target if it exists and then (re)creates it as a copy of source
   * synchronously.
   */
  static void copyIndex(RestClient restClient, String source, String target) throws IOException {
    deleteIndex(restClient, target);
    HttpEntity entity =
        new NStringEntity(
            String.format(
                "{\"source\" : { \"index\" : \"%s\" }, \"dest\" : { \"index\" : \"%s\" } }",
                source, target),
            ContentType.APPLICATION_JSON);
    Request request = new Request("POST", "/_reindex");
    flushAndRefreshAllIndices(restClient);
    request.setEntity(entity);
    restClient.performRequest(request);
  }

  /** Inserts the given number of test documents into Elasticsearch. */
  static void insertTestDocuments(
      ConnectionConfiguration connectionConfiguration, List<String> data, RestClient restClient)
      throws IOException {
    StringBuilder bulkRequest = new StringBuilder();
    int i = 0;
    String type = connectionConfiguration.getType();
    for (String document : data) {
      bulkRequest.append(
          String.format(
              "{ \"index\" : { \"_index\" : \"%s\", %s \"_id\" : \"%s\" } }%n%s%n",
              connectionConfiguration.getIndex(),
              type != null ? String.format("\"_type\" : \"%s\",", type) : "",
              i++,
              document));
    }
    String endPoint = connectionConfiguration.getBulkEndPoint();
    HttpEntity requestBody =
        new NStringEntity(bulkRequest.toString(), ContentType.APPLICATION_JSON);
    Request request = new Request("POST", endPoint);
    request.setEntity(requestBody);
    Response response = restClient.performRequest(request);
    ElasticsearchIO.createWriteReport(response.getEntity(), Collections.emptySet(), true);
    flushAndRefreshAllIndices(restClient);
  }

  /** Inserts the given number of test documents into Elasticsearch. */
  static void insertTestDocuments(
      ConnectionConfiguration connectionConfiguration, long numDocs, RestClient restClient)
      throws IOException {
    List<String> data =
        ElasticsearchIOTestUtils.createDocuments(
            numDocs, ElasticsearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
    insertTestDocuments(connectionConfiguration, data, restClient);
  }

  static void flushAndRefreshAllIndices(RestClient restClient) throws IOException {
    Request request = new Request("POST", "/_flush");
    restClient.performRequest(request);
    request = new Request("POST", "/_refresh");
    restClient.performRequest(request);
  }

  /**
   * Forces a refresh of the given index to make recently inserted documents available for search
   * using the index and type named in the connectionConfiguration.
   *
   * @param connectionConfiguration providing the index and type
   * @param restClient To use for issuing queries
   * @return The number of docs in the index
   * @throws IOException On error communicating with Elasticsearch
   */
  static long refreshIndexAndGetCurrentNumDocs(
      ConnectionConfiguration connectionConfiguration, RestClient restClient) throws IOException {
    return refreshIndexAndGetCurrentNumDocs(
        restClient,
        connectionConfiguration.getIndex(),
        connectionConfiguration.getType(),
        getBackendVersion(connectionConfiguration),
        null);
  }

  /**
   * Forces a refresh of the given index to make recently inserted documents available for search
   * using the index and type named in the connectionConfiguration.
   *
   * @param connectionConfiguration providing the index and type
   * @param restClient To use for issuing queries
   * @param urlParams Optional key/value pairs describing URL params for ES APIs
   * @return The number of docs in the index
   * @throws IOException On error communicating with Elasticsearch
   */
  static long refreshIndexAndGetCurrentNumDocs(
      ConnectionConfiguration connectionConfiguration,
      RestClient restClient,
      @Nullable Map<String, String> urlParams)
      throws IOException {
    return refreshIndexAndGetCurrentNumDocs(
        restClient,
        connectionConfiguration.getIndex(),
        connectionConfiguration.getType(),
        getBackendVersion(connectionConfiguration),
        urlParams);
  }

  static long refreshIndexAndGetCurrentNumDocs(
      RestClient restClient, String index, String type, int backendVersion) throws IOException {
    return refreshIndexAndGetCurrentNumDocs(restClient, index, type, backendVersion, null);
  }
  /**
   * Forces a refresh of the given index to make recently inserted documents available for search.
   *
   * @param restClient To use for issuing queries
   * @param index The Elasticsearch index
   * @param type The Elasticsearch type
   * @param urlParams Optional key/value pairs describing URL params for ES APIs
   * @return The number of docs in the index
   * @throws IOException On error communicating with Elasticsearch
   */
  static long refreshIndexAndGetCurrentNumDocs(
      RestClient restClient,
      String index,
      String type,
      int backendVersion,
      @Nullable Map<String, String> urlParams)
      throws IOException {
    long result = 0;
    try {
      flushAndRefreshAllIndices(restClient);

      String endPoint = generateSearchPath(index, type);
      Request request = new Request("GET", endPoint);
      if (urlParams != null) {
        request.addParameters(urlParams);
      }
      Response response = restClient.performRequest(request);
      JsonNode searchResult = ElasticsearchIO.parseResponse(response.getEntity());
      if (backendVersion >= 7) {
        result = searchResult.path("hits").path("total").path("value").asLong();
      } else {
        result = searchResult.path("hits").path("total").asLong();
      }
    } catch (IOException e) {
      // it is fine to ignore bellow exceptions because in testWriteWithBatchSize* sometimes,
      // we call upgrade before any doc have been written
      // (when there are fewer docs processed than batchSize).
      // In that cases index/type has not been created (created upon first doc insertion)
      if (!e.getMessage().contains("index_not_found_exception")) {
        throw e;
      }
    }
    return result;
  }

  /**
   * Generates a list of test documents for insertion.
   *
   * @param numDocs Number of docs to generate
   * @param injectionMode {@link InjectionMode} that specifies whether to insert malformed documents
   * @return the list of json String representing the documents
   */
  static List<String> createDocuments(long numDocs, InjectionMode injectionMode) {

    ArrayList<String> data = new ArrayList<>();
    LocalDateTime baseDateTime = LocalDateTime.now();
    for (int i = 0; i < numDocs; i++) {
      int index = i % FAMOUS_SCIENTISTS.length;
      // insert 2 malformed documents
      if (InjectionMode.INJECT_SOME_INVALID_DOCS.equals(injectionMode)
          && INVALID_DOCS_IDS.contains(i)) {
        data.add(String.format("{\"scientist\";\"%s\", \"id\":%s}", FAMOUS_SCIENTISTS[index], i));
      } else {
        data.add(
            String.format(
                "{\"scientist\":\"%s\", \"id\":%s, \"@timestamp\" : \"%s\"}",
                FAMOUS_SCIENTISTS[index], i, baseDateTime.plusSeconds(i).toString()));
      }
    }
    // insert 1 additional id too long doc. It should trigger
    // org.elasticsearch.client.ResponseException.
    if (InjectionMode.INJECT_ONE_ID_TOO_LONG_DOC.equals(injectionMode)) {
      data.add(
          String.format(
              "{\"scientist\":\"invalid_scientist\", \"id\":%s, \"@timestamp\" : \"%s\"}",
              INVALID_LONG_ID, baseDateTime));
    }
    return data;
  }

  static List<ObjectNode> createJsonDocuments(long numDocs, InjectionMode injectionMode)
      throws JsonProcessingException {
    List<String> stringData = createDocuments(numDocs, injectionMode);
    List<ObjectNode> data = new ArrayList<>();

    for (String doc : stringData) {
      data.add((ObjectNode) MAPPER.readTree(doc));
    }
    return data;
  }

  /**
   * Executes a query for the named scientist and returns the count from the result.
   *
   * @param connectionConfiguration Specifies the index and type
   * @param restClient To use to execute the call
   * @param scientistName The scientist to query for
   * @param urlParams Optional key/value pairs describing URL params for ES APIs
   * @return The count of documents found
   * @throws IOException On error talking to Elasticsearch
   */
  static int countByScientistName(
      ConnectionConfiguration connectionConfiguration,
      RestClient restClient,
      String scientistName,
      @Nullable Map<String, String> urlParams)
      throws IOException {
    return countByMatch(
        connectionConfiguration, restClient, "scientist", scientistName, urlParams, null);
  }

  /**
   * Creates a _search API path depending on ConnectionConfiguration and url params.
   *
   * @param index Optional Elasticsearch index
   * @param type Optional Elasticsearch type
   * @return The _search endpoint for the provided settings.
   */
  static String generateSearchPath(@Nullable String index, @Nullable String type) {
    StringBuilder sb = new StringBuilder();
    if (index != null) {
      sb.append("/").append(index);
    }
    if (type != null) {
      sb.append("/").append(type);
    }

    sb.append("/_search");

    return sb.toString();
  }

  /**
   * Creates a _search API path depending on ConnectionConfiguration and url params.
   *
   * @param connectionConfiguration Specifies the index and type
   * @return The _search endpoint for the provided settings.
   */
  static String generateSearchPath(ConnectionConfiguration connectionConfiguration) {
    return generateSearchPath(
        connectionConfiguration.getIndex(), connectionConfiguration.getType());
  }

  /**
   * Executes a match query for given field/value and returns the count of results.
   *
   * @param connectionConfiguration Specifies the index and type
   * @param restClient To use to execute the call
   * @param field The field to query
   * @param value The value to match
   * @param urlParams Optional key/value pairs describing URL params for ES APIs
   * @param versionNumberCountPair Optional pair of [version_number, expected_num_doc_with_version]
   * @return The count of documents in the search result
   * @throws IOException On error communicating with Elasticsearch
   */
  static int countByMatch(
      ConnectionConfiguration connectionConfiguration,
      RestClient restClient,
      String field,
      String value,
      @Nullable Map<String, String> urlParams,
      @Nullable KV<Integer, Long> versionNumberCountPair)
      throws IOException {
    String size =
        versionNumberCountPair == null ? "10" : versionNumberCountPair.getValue().toString();
    String requestBody =
        "{\n"
            + "\"size\": "
            + size
            + ",\n"
            + "\"version\" : true,\n"
            + "  \"query\" : {\"match\": {\n"
            + "    \""
            + field
            + "\": \""
            + value
            + "\"\n"
            + "  }}\n"
            + "}\n";

    String endPoint = generateSearchPath(connectionConfiguration);
    HttpEntity httpEntity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);

    Request request = new Request("GET", endPoint);
    request.setEntity(httpEntity);
    if (urlParams != null) {
      request.addParameters(urlParams);
    }

    Response response = restClient.performRequest(request);
    JsonNode searchResult = parseResponse(response.getEntity());

    if (versionNumberCountPair != null) {
      int numHits = 0;
      for (JsonNode hit : searchResult.path("hits").path("hits")) {
        if (hit.path("_version").asInt() == versionNumberCountPair.getKey()) {
          numHits++;
        }
      }
      return numHits;
    }

    if (getBackendVersion(connectionConfiguration) >= 7) {
      return searchResult.path("hits").path("total").path("value").asInt();
    } else {
      return searchResult.path("hits").path("total").asInt();
    }
  }

  static RestClient clientFromContainer(ElasticsearchContainer container) {
    return clientFromContainer(container, false);
  }

  static RestClient clientFromContainer(
      ElasticsearchContainer container, boolean isCompressionEnabled) {
    return RestClient.builder(
            new HttpHost(
                container.getContainerIpAddress(),
                container.getMappedPort(ELASTICSEARCH_DEFAULT_PORT),
                "http"))
        .setCompressionEnabled(isCompressionEnabled)
        .build();
  }

  static ConnectionConfiguration createConnectionConfig(RestClient restClient) {
    String[] hostStrings =
        restClient.getNodes().stream().map(node -> node.getHost().toURI()).toArray(String[]::new);

    return ConnectionConfiguration.create(hostStrings, getEsIndex(), ES_TYPE)
        .withSocketTimeout(120000)
        .withConnectTimeout(5000);
  }

  static ElasticsearchContainer createTestContainer(String imageTag) {
    ElasticsearchContainer container =
        new ElasticsearchContainer(
                DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch")
                    .withTag(imageTag))
            .withEnv("xpack.security.enabled", "false");

    container.withStartupTimeout(Duration.ofMinutes(3));

    return container;
  }

  static SimpleFunction<Document, Integer> mapToInputId =
      new SimpleFunction<Document, Integer>() {
        @Override
        public Integer apply(Document document) {
          try {
            // Account for intentionally invalid input json docs
            String fixedJson = document.getInputDoc().replaceAll(";", ":");
            return MAPPER.readTree(fixedJson).path("id").asInt();
          } catch (JsonProcessingException e) {
            return -1;
          }
        }
      };

  static SimpleFunction<Document, String> mapToInputIdString =
      new SimpleFunction<Document, String>() {
        @Override
        public String apply(Document document) {
          try {
            // Account for intentionally invalid input json docs
            String fixedJson = document.getInputDoc().replaceAll(";", ":");
            return MAPPER.readTree(fixedJson).path("id").asText();
          } catch (JsonProcessingException e) {
            return "-1";
          }
        }
      };
}
