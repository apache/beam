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
import java.util.Set;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;

/** Test utilities to use with {@link SolrIO}. */
public class SolrIOTestUtils {
  public static final long MIN_DOC_SIZE = 30L;
  public static final long MAX_DOC_SIZE = 150L;

  static void createCollection(
      String collection, int numShards, int replicationFactor, AuthorizedSolrClient client)
      throws Exception {
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.Create.createCollection(collection, numShards, replicationFactor)
            .setMaxShardsPerNode(2);
    client.process(create);
  }

  /** Inserts the given number of test documents into Solr. */
  static void insertTestDocuments(String collection, long numDocs, AuthorizedSolrClient client)
      throws IOException {
    List<SolrInputDocument> data = createDocuments(numDocs);
    try {
      UpdateRequest updateRequest = new UpdateRequest();
      updateRequest.setAction(UpdateRequest.ACTION.COMMIT, true, true);
      updateRequest.add(data);
      client.process(collection, updateRequest);
    } catch (SolrServerException e) {
      throw new IOException("Failed to insert test documents to collection", e);
    }
  }

  /** Clear given collection. */
  static void clearCollection(String collection, AuthorizedSolrClient client) throws IOException {
    try {
      UpdateRequest updateRequest = new UpdateRequest();
      updateRequest.setAction(UpdateRequest.ACTION.COMMIT, true, true);
      updateRequest.deleteByQuery("*:*");
      client.process(collection, updateRequest);
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
  }

  /**
   * Forces a commit of the given collection to make recently inserted documents available for
   * search.
   *
   * @return The number of docs in the index
   */
  static long commitAndGetCurrentNumDocs(String collection, AuthorizedSolrClient client)
      throws IOException {
    SolrQuery solrQuery = new SolrQuery("*:*");
    solrQuery.setRows(0);
    try {
      UpdateRequest update = new UpdateRequest();
      update.setAction(UpdateRequest.ACTION.COMMIT, true, true);
      client.process(collection, update);

      return client.query(collection, new SolrQuery("*:*")).getResults().getNumFound();
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
      "Lovelace",
      "Franklin",
      "Meitner",
      "Hopper",
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
      SolrInputDocument doc = new SolrInputDocument();
      doc.setField("id", String.valueOf(i));
      doc.setField("scientist", scientists[index]);
      data.add(doc);
    }
    return data;
  }

  /** A strategy that will accept to retry on any SolrException. */
  static class LenientRetryPredicate implements SolrIO.RetryConfiguration.RetryPredicate {
    @Override
    public boolean test(Throwable throwable) {
      return throwable instanceof SolrException;
    }
  }

  /**
   * A utility which will return true if at least one thread of the given name exists and is alive.
   */
  static boolean namedThreadIsAlive(String name) {
    Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
    for (Thread t : threadSet) {
      if (t.getName().equals(name) && t.isAlive()) {
        return true;
      }
    }
    return false;
  }
}
