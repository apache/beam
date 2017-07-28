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

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.params.SolrParams;

/**
 * Client for interact with SolrCloud.
 */
class AuthorizedCloudSolrClient extends AuthorizedSolrClient<CloudSolrClient> {

  AuthorizedCloudSolrClient(CloudSolrClient solrClient,
      SolrIO.ConnectionConfiguration configuration) {
    super(solrClient, configuration);
  }

  DocCollection getDocCollection(String collection){
    solrClient.connect();
    return solrClient.getZkStateReader().getClusterState().getCollection(collection);
  }

  QueryResponse query(String collection, SolrParams solrParams)
      throws IOException, SolrServerException {
    QueryRequest query = new QueryRequest(solrParams);
    return process(collection, query);
  }

  <T extends SolrResponse> T process(String collection, SolrRequest<T> request)
      throws IOException, SolrServerException {
    request.setBasicAuthCredentials(username, password);
    return request.process(solrClient, collection);
  }
}
