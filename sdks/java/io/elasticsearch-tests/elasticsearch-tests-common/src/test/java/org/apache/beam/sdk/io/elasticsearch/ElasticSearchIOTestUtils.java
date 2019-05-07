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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

/** Test utilities to use with {@link ElasticsearchIO}. */
class ElasticSearchIOTestUtils {
  static final String[] FAMOUS_SCIENTISTS = {
    "Einstein",
    "Darwin",
    "Copernicus",
    "Pasteur",
    "Curie",
    "Faraday",
    "Newton",
    "Bohr",
    "Galilei",
    "Maxwell"
  };
  static final int NUM_SCIENTISTS = FAMOUS_SCIENTISTS.length;

  /** Enumeration that specifies whether to insert malformed documents. */
  public enum InjectionMode {
    INJECT_SOME_INVALID_DOCS,
    DO_NOT_INJECT_INVALID_DOCS
  }

  /** Deletes the given index synchronously. */
  static void deleteIndex(ConnectionConfiguration connectionConfiguration, RestClient restClient)
      throws IOException {
    deleteIndex(restClient, connectionConfiguration.getIndex());
  }

  private static void closeIndex(RestClient restClient, String index) throws IOException {
    restClient.performRequest("POST", String.format("/%s/_close", index));
  }

  private static void deleteIndex(RestClient restClient, String index) throws IOException {
    try {
      closeIndex(restClient, index);
      restClient.performRequest(
          "DELETE", String.format("/%s", index), Collections.singletonMap("refresh", "wait_for"));
    } catch (IOException e) {
      // it is fine to ignore this expression as deleteIndex occurs in @before,
      // so when the first tests is run, the index does not exist yet
      if (!e.getMessage().contains("index_not_found_exception")) {
        throw e;
      }
    }
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
    restClient.performRequest(
        "POST", "/_reindex", Collections.singletonMap("refresh", "wait_for"), entity);
  }

  /** Inserts the given number of test documents into Elasticsearch. */
  static void insertTestDocuments(
      ConnectionConfiguration connectionConfiguration, long numDocs, RestClient restClient)
      throws IOException {
    List<String> data =
        ElasticSearchIOTestUtils.createDocuments(
            numDocs, ElasticSearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
    StringBuilder bulkRequest = new StringBuilder();
    int i = 0;
    for (String document : data) {
      bulkRequest.append(
          String.format(
              "{ \"index\" : { \"_index\" : \"%s\", \"_type\" : \"%s\", \"_id\" : \"%s\" } }%n%s%n",
              connectionConfiguration.getIndex(),
              connectionConfiguration.getType(),
              i++,
              document));
    }
    String endPoint =
        String.format(
            "/%s/%s/_bulk", connectionConfiguration.getIndex(), connectionConfiguration.getType());
    HttpEntity requestBody =
        new NStringEntity(bulkRequest.toString(), ContentType.APPLICATION_JSON);
    Response response =
        restClient.performRequest(
            "POST", endPoint, Collections.singletonMap("refresh", "wait_for"), requestBody);
    ElasticsearchIO.checkForErrors(
        response.getEntity(), ElasticsearchIO.getBackendVersion(connectionConfiguration), false);
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
        restClient, connectionConfiguration.getIndex(), connectionConfiguration.getType());
  }

  /**
   * Forces a refresh of the given index to make recently inserted documents available for search.
   *
   * @param restClient To use for issuing queries
   * @param index The Elasticsearch index
   * @param type The Elasticsearch type
   * @return The number of docs in the index
   * @throws IOException On error communicating with Elasticsearch
   */
  static long refreshIndexAndGetCurrentNumDocs(RestClient restClient, String index, String type)
      throws IOException {
    long result = 0;
    try {
      String endPoint = String.format("/%s/_refresh", index);
      restClient.performRequest("POST", endPoint);

      endPoint = String.format("/%s/%s/_search", index, type);
      Response response = restClient.performRequest("GET", endPoint);
      JsonNode searchResult = ElasticsearchIO.parseResponse(response.getEntity());
      result = searchResult.path("hits").path("total").asLong();
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
    for (int i = 0; i < numDocs; i++) {
      int index = i % FAMOUS_SCIENTISTS.length;
      // insert 2 malformed documents
      if (InjectionMode.INJECT_SOME_INVALID_DOCS.equals(injectionMode) && (i == 6 || i == 7)) {
        data.add(String.format("{\"scientist\";\"%s\", \"id\":%s}", FAMOUS_SCIENTISTS[index], i));
      } else {
        data.add(String.format("{\"scientist\":\"%s\", \"id\":%s}", FAMOUS_SCIENTISTS[index], i));
      }
    }
    return data;
  }

  /**
   * Executes a query for the named scientist and returns the count from the result.
   *
   * @param connectionConfiguration Specifies the index and type
   * @param restClient To use to execute the call
   * @param scientistName The scientist to query for
   * @return The cound of documents found
   * @throws IOException On error talking to Elasticsearch
   */
  static int countByScientistName(
      ConnectionConfiguration connectionConfiguration, RestClient restClient, String scientistName)
      throws IOException {
    return countByMatch(connectionConfiguration, restClient, "scientist", scientistName);
  }

  /**
   * Executes a match query for given field/value and returns the count of results.
   *
   * @param connectionConfiguration Specifies the index and type
   * @param restClient To use to execute the call
   * @param field The field to query
   * @param value The value to match
   * @return The count of documents in the search result
   * @throws IOException On error communicating with Elasticsearch
   */
  static int countByMatch(
      ConnectionConfiguration connectionConfiguration,
      RestClient restClient,
      String field,
      String value)
      throws IOException {
    String requestBody =
        "{\n"
            + "  \"query\" : {\"match\": {\n"
            + "    \""
            + field
            + "\": \""
            + value
            + "\"\n"
            + "  }}\n"
            + "}\n";
    String endPoint =
        String.format(
            "/%s/%s/_search",
            connectionConfiguration.getIndex(), connectionConfiguration.getType());
    HttpEntity httpEntity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
    Response response =
        restClient.performRequest("GET", endPoint, Collections.emptyMap(), httpEntity);
    JsonNode searchResult = parseResponse(response.getEntity());
    return searchResult.path("hits").path("total").asInt();
  }

  public static void setIndexMapping(
      ConnectionConfiguration connectionConfiguration, RestClient restClient) throws IOException {
    String endpoint = String.format("/%s", connectionConfiguration.getIndex());
    String requestString =
        String.format(
            "{\"mappings\":{\"%s\":{\"properties\":{\"age\":{\"type\":\"long\"},"
                + " \"scientist\":{\"type\":\"%s\"}, \"id\":{\"type\":\"long\"}}}}}",
            connectionConfiguration.getType(),
            getBackendVersion(connectionConfiguration) == 2 ? "string" : "text");
    HttpEntity requestBody = new NStringEntity(requestString, ContentType.APPLICATION_JSON);
    Request request = new Request("PUT", endpoint);
    request.setEntity(requestBody);
    restClient.performRequest(request);
  }
}
