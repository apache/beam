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
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;

/**
 * Test class to use with ElasticSearch IO.
 */
public class ElasticSearchIOTestUtils {

  /**
   * Enumeration that specifies whether to insert malformed documents.
   */
  enum InjectionMode {
    INJECT_SOME_INVALID_DOCS,
    DO_NOT_INJECT_INVALID_DOCS;
  }

  /**
   * Method to insert test documents into the ElasticSearch instance to which client points.
   * @param index Index to insert into
   * @param type Type of documents to insert
   * @param numDocs Number of docs to insert
   * @param client Elasticsearch TCP client to use for insertion
   * @throws Exception
   */
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
   * For an upgrade of all indices to make recently inserted documents available for search.
   * @param client Elasticsearch TCP client to use for upgrade
   * @return The number of docs in the index
   */
  static long upgradeIndexAndGetCurrentNumDocs(Client client) {
    // force the index to upgrade after inserting for the inserted docs
    // to be searchable immediately
    client.admin().indices().upgrade(new UpgradeRequest()).actionGet();
    SearchResponse response = client.prepareSearch().execute().actionGet(5000);
    return response.getHits().getTotalHits();
  }

  /**
   * Generates a list of test documents for insertion.
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
      //insert 2 malformed documents
      if (InjectionMode.INJECT_SOME_INVALID_DOCS.equals(injectionMode) && (i == 6 || i == 7)) {
        data.add(String.format("{\"scientist\";\"%s\", \"id\":%d}", scientists[index], i));
      } else {
        data.add(String.format("{\"scientist\":\"%s\", \"id\":%d}", scientists[index], i));
      }
    }
    return data;
  }
}
