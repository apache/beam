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
public class AuthorizedCloudSolrClient extends AuthorizedSolrClient<CloudSolrClient> {

  public AuthorizedCloudSolrClient(CloudSolrClient solrClient,
      SolrIO.ConnectionConfiguration configuration) {
    super(solrClient, configuration);
  }

  public DocCollection getDocCollection(String collection){
    solrClient.connect();
    return solrClient.getZkStateReader().getClusterState().getCollection(collection);
  }

  public QueryResponse query(String collection, SolrParams solrParams)
      throws IOException, SolrServerException {
    QueryRequest query = new QueryRequest(solrParams);
    return process(collection, query);
  }

  public <T extends SolrResponse> T process(String collection, SolrRequest<T> request)
      throws IOException, SolrServerException {
    request.setBasicAuthCredentials(username, password);
    return request.process(solrClient, collection);
  }
}
