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
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.params.SolrParams;

/**
 * Client for interact with Solr.
 * @param <ClientT> type of SolrClient
 */
class AuthorizedSolrClient<ClientT extends SolrClient> implements Closeable {
  private final ClientT solrClient;
  private final String username;
  private final String password;

  AuthorizedSolrClient(ClientT solrClient, ConnectionConfiguration configuration) {
    checkArgument(
        solrClient != null,
        "solrClient can not be null");
    checkArgument(
        configuration != null,
        "configuration can not be null");
    this.solrClient = solrClient;
    this.username = configuration.getUsername();
    this.password = configuration.getPassword();
  }

  QueryResponse query(String collection, SolrParams solrParams)
      throws IOException, SolrServerException {
    QueryRequest query = new QueryRequest(solrParams);
    return process(collection, query);
  }

  <ResponseT extends SolrResponse> ResponseT process(String collection,
      SolrRequest<ResponseT> request) throws IOException, SolrServerException {
    request.setBasicAuthCredentials(username, password);
    return request.process(solrClient, collection);
  }

  CoreAdminResponse process(CoreAdminRequest request)
      throws IOException, SolrServerException {
    return process(null, request);
  }

  SolrResponse process(CollectionAdminRequest request)
      throws IOException, SolrServerException {
    return process(null, request);
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
