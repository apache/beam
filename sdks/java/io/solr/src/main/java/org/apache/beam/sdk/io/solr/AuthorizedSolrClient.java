package org.apache.beam.sdk.io.solr;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Closeable;
import java.io.IOException;

import org.apache.beam.sdk.io.solr.SolrIO.ConnectionConfiguration;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.SolrParams;

/**
 * Client for interact with a replica in Solr.
 * @param <T> type of SolrClient
 */
class AuthorizedSolrClient<T extends SolrClient> implements Closeable {
  protected T solrClient;
  protected String username;
  protected String password;

  public AuthorizedSolrClient(T solrClient, ConnectionConfiguration configuration) {
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

  public QueryResponse query(SolrParams solrParams)
      throws IOException, SolrServerException {
    QueryRequest query = new QueryRequest(solrParams);
    return process(query);
  }

  public <T extends SolrResponse> T process(SolrRequest<T> request)
      throws IOException, SolrServerException {
    request.setBasicAuthCredentials(username, password);
    return request.process(solrClient);
  }

  @Override public void close() throws IOException {
    solrClient.close();
  }
}
