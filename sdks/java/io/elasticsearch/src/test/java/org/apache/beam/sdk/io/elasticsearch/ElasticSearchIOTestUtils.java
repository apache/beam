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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.index.IndexNotFoundException;

/** Test utilities to use with {@link ElasticsearchIO}. */
class ElasticSearchIOTestUtils {

  /** Enumeration that specifies whether to insert malformed documents. */
  enum InjectionMode {
    INJECT_SOME_INVALID_DOCS,
    DO_NOT_INJECT_INVALID_DOCS
  }

  /** Deletes the given index synchronously. */
  static void deleteIndex(String index, Client client) throws Exception {
    IndicesAdminClient indices = client.admin().indices();
    IndicesExistsResponse indicesExistsResponse =
        indices.exists(new IndicesExistsRequest(index)).get();
    if (indicesExistsResponse.isExists()) {
      indices.prepareClose(index).get();
      indices.delete(Requests.deleteIndexRequest(index)).get();
    }
  }

  /** Inserts the given number of test documents into Elasticsearch. */
  static void insertTestDocuments(String index, String type, long numDocs, Client client)
      throws Exception {
    final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk().setRefresh(true);
    List<String> data =
        ElasticSearchIOTestUtils.createDocuments(
            numDocs, ElasticSearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
    for (String document : data) {
      bulkRequestBuilder.add(client.prepareIndex(index, type, null).setSource(document));
    }
    final BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
    if (bulkResponse.hasFailures()) {
      throw new IOException(
          String.format(
              "Cannot insert test documents in index %s : %s",
              index, bulkResponse.buildFailureMessage()));
    }
  }

  /**
   * Forces an upgrade of the given index to make recently inserted documents available for search.
   *
   * @return The number of docs in the index
   */
  static long upgradeIndexAndGetCurrentNumDocs(String index, String type, Client client) {
    try {
      client.admin().indices().upgrade(new UpgradeRequest(index)).actionGet();
      SearchResponse response =
          client.prepareSearch(index).setTypes(type).execute().actionGet(5000);
      return response.getHits().getTotalHits();
      // it is fine to ignore bellow exceptions because in testWriteWithBatchSize* sometimes,
      // we call upgrade before any doc have been written
      // (when there are fewer docs processed than batchSize).
      // In that cases index/type has not been created (created upon first doc insertion)
    } catch (IndexNotFoundException e) {
    } catch (java.lang.IllegalArgumentException e) {
      if (!e.getMessage().contains("No search type")) {
        throw e;
      }
    }
    return 0;
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
        data.add(String.format("{\"scientist\";\"%s\", \"id\":%d}", scientists[index], i));
      } else {
        data.add(String.format("{\"scientist\":\"%s\", \"id\":%d}", scientists[index], i));
      }
    }
    return data;
  }
}
