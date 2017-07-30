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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Closeable;
import java.io.IOException;

import org.apache.beam.sdk.io.solr.SolrIO.ConnectionConfiguration;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.params.SolrParams;

/**
 * Client for interact with Solr.
 * The target replica/collection is pre-selected
 * @param <T> type of SolrClient
 */
class AuthorizedSolrClient<T extends SolrClient> implements Closeable {
  private final T solrClient;
  private final String username;
  private final String password;

  AuthorizedSolrClient(T solrClient, ConnectionConfiguration configuration) {
    checkArgument(
        solrClient != null,
        "AuthorizedSolrClient(solrClient, configuration) "
            + "called with null solrClient");
    checkArgument(
        configuration != null,
        "AuthorizedSolrClient(solrClient, configuration) "
            + "called with null configuration");
    this.solrClient = solrClient;
    this.username = configuration.getUsername();
    this.password = configuration.getPassword();
  }

  QueryResponse query(SolrParams solrParams)
      throws IOException, SolrServerException {
    QueryRequest query = new QueryRequest(solrParams);
    return process(query);
  }

  <T2 extends SolrResponse> T2 process(SolrRequest<T2> request)
      throws IOException, SolrServerException {
    request.setBasicAuthCredentials(username, password);
    return request.process(solrClient);
  }

  static ClusterState getClusterState(
      AuthorizedSolrClient<CloudSolrClient> authorizedSolrClient) {
    authorizedSolrClient.solrClient.connect();
    return authorizedSolrClient.solrClient.getZkStateReader().getClusterState();
  }

  @Override public void close() throws IOException {
    solrClient.close();
  }
}
