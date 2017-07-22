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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.SolrInputDocument;


/** Test utilities to use with {@link SolrIO}. */
public class SolrIOTestUtils {
  public static final long AVERAGE_DOC_SIZE = 25L;
  public static final long MAX_DOC_SIZE = 35L;

  static void createCollection(String collection, int numShards, SolrClient client)
      throws Exception {
    CollectionAdminRequest.createCollection(collection, numShards, 1)
        .setMaxShardsPerNode(2)
        .process(client);
  }

  /** Inserts the given number of test documents into Solr. */
  static void insertTestDocuments(String collection, long numDocs, CloudSolrClient client)
      throws IOException {
    List<SolrInputDocument> data = createDocuments(numDocs);
    try {
      client.add(collection, data);
      client.commit(collection);
    } catch (SolrServerException e) {
      throw new IOException("Failed to insert test documents in collection " + collection, e);
    }
  }


  /** Delete given collection. */
  static void deleteCollection(String collection, SolrClient client)
      throws IOException {
    try {
      CollectionAdminRequest.deleteCollection(collection).process(client);
    } catch (SolrServerException e) {
      throw new IOException(e);
    }

  }

  /** Clear given collection. */
  static void clearCollection(String collection, SolrClient client) throws IOException {
    try {
      client.deleteByQuery(collection, "*:*");
      client.commit();
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
  }

  /**
   * Forces a commit of the given collection to
   * make recently inserted documents available for search.
   *
   * @return The number of docs in the index
   */
  static long commitAndGetCurrentNumDocs(String collection, SolrClient client)
      throws IOException {
    SolrQuery solrQuery = new SolrQuery("*:*");
    solrQuery.setRows(0);
    try {
      client.commit(collection);
      return client.query(solrQuery).getResults().getNumFound();
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
  }

  /**
   * Generates a list of test documents for insertion.
   *
   * @return the list of json String representing the documents
   */
  static List<SolrInputDocument> createDocuments(long numDocs) {
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
    ArrayList<SolrInputDocument> data = new ArrayList<>();
    for (int i = 0; i < numDocs; i++) {
      int index = i % scientists.length;
      SolrInputDocument doc = new SolrInputDocument(
          "scientist", scientists[index], "id", String.valueOf(i));
      data.add(doc);
    }
    return data;
  }


}
