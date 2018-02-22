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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

/** Test utilities to use with {@link ElasticsearchIO}. */
class ElasticSearchIOTestUtils {

  /** Enumeration that specifies whether to insert malformed documents. */
  public enum InjectionMode {
    INJECT_SOME_INVALID_DOCS,
    DO_NOT_INJECT_INVALID_DOCS
  }

  /** Deletes the given index synchronously. */
  static void deleteIndex(ConnectionConfiguration connectionConfiguration,
      RestClient restClient) throws IOException {
    try {
      restClient.performRequest("DELETE", String.format("/%s", connectionConfiguration.getIndex()));
    } catch (IOException e) {
      // it is fine to ignore this expression as deleteIndex occurs in @before,
      // so when the first tests is run, the index does not exist yet
      if (!e.getMessage().contains("index_not_found_exception")) {
        throw e;
      }
    }
  }

  /** Inserts the given number of test documents into Elasticsearch. */
  static void insertTestDocuments(ConnectionConfiguration connectionConfiguration,
      long numDocs, RestClient restClient) throws IOException {
    List<String> data =
        ElasticSearchIOTestUtils.createDocuments(
            numDocs, ElasticSearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
    StringBuilder bulkRequest = new StringBuilder();
    int i = 0;
    for (String document : data) {
      bulkRequest.append(String.format(
          "{ \"index\" : { \"_index\" : \"%s\", \"_type\" : \"%s\", \"_id\" : \"%s\" } }%n%s%n",
          connectionConfiguration.getIndex(), connectionConfiguration.getType(), i++, document));
    }
    String endPoint = String.format("/%s/%s/_bulk", connectionConfiguration.getIndex(),
        connectionConfiguration.getType());
    HttpEntity requestBody =
        new NStringEntity(bulkRequest.toString(), ContentType.APPLICATION_JSON);
    Response response = restClient.performRequest("POST", endPoint,
        Collections.singletonMap("refresh", "true"), requestBody);
    ElasticsearchIO
        .checkForErrors(response, ElasticsearchIO.getBackendVersion(connectionConfiguration));
  }

  /**
   * Forces a refresh of the given index to make recently inserted documents available for search.
   *
   * @return The number of docs in the index
   */
  static long refreshIndexAndGetCurrentNumDocs(
      ConnectionConfiguration connectionConfiguration, RestClient restClient) throws IOException {
    long result = 0;
    try {
      String endPoint = String.format("/%s/_refresh", connectionConfiguration.getIndex());
      restClient.performRequest("POST", endPoint);

      endPoint = String.format("/%s/%s/_search", connectionConfiguration.getIndex(),
          connectionConfiguration.getType());
      Response response = restClient.performRequest("GET", endPoint);
      JsonNode searchResult = ElasticsearchIO.parseResponse(response);
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
    String[] scientists = {
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
    ArrayList<String> data = new ArrayList<>();
    for (int i = 0; i < numDocs; i++) {
      int index = i % scientists.length;
      // insert 2 malformed documents
      if (InjectionMode.INJECT_SOME_INVALID_DOCS.equals(injectionMode) && (i == 6 || i == 7)) {
        data.add(String.format("{\"scientist\";\"%s\", \"id\":%s}", scientists[index], i));
      } else {
        data.add(String.format("{\"scientist\":\"%s\", \"id\":%s}", scientists[index], i));
      }
    }
    return data;
  }
}
