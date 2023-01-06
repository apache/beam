/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it.elasticsearch;

import static com.google.cloud.teleport.it.elasticsearch.ElasticsearchUtils.checkValidIndexName;

import com.google.cloud.teleport.it.testcontainers.TestContainerResourceManager;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Default class for implementation of {@link ElasticsearchResourceManager} interface.
 *
 * <p>The class supports many indices per resource manager object.
 *
 * <p>The index name is formed using testId. The index name will be "{prefix}-{testId}-{ISO8601
 * time, microsecond precision}".
 *
 * <p>The class is thread-safe.
 */
public class DefaultElasticsearchResourceManager
    extends TestContainerResourceManager<GenericContainer<?>>
    implements ElasticsearchResourceManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultElasticsearchResourceManager.class);

  private static final String DEFAULT_ELASTICSEARCH_CONTAINER_NAME =
      "docker.elastic.co/elasticsearch/elasticsearch-oss";

  // A list of available Elasticsearch Docker image tags can be found at
  // https://hub.docker.com/_/elasticsearch/tags
  private static final String DEFAULT_ELASTICSEARCH_CONTAINER_TAG = "7.9.2";

  // 9200 is the default port that Elasticsearch is configured to listen on
  private static final int ELASTICSEARCH_INTERNAL_PORT = 9200;

  private final RestHighLevelClient elasticsearchClient;
  private final String connectionString;
  private final List<String> managedIndexNames = new ArrayList<>();

  private DefaultElasticsearchResourceManager(DefaultElasticsearchResourceManager.Builder builder) {
    this(
        /*elasticsearchClient=*/ null,
        builder.useStaticContainer
            ? null
            : new ElasticsearchContainer(
                DockerImageName.parse(builder.containerImageName)
                    .withTag(builder.containerImageTag)),
        builder);
  }

  @VisibleForTesting
  DefaultElasticsearchResourceManager(
      RestHighLevelClient elasticsearchClient,
      ElasticsearchContainer container,
      DefaultElasticsearchResourceManager.Builder builder) {
    super(container, builder);

    this.connectionString =
        "http://" + this.getHost() + ":" + container.getMappedPort(ELASTICSEARCH_INTERNAL_PORT);

    RestClientBuilder restClientBuilder =
        RestClient.builder(HttpHost.create(container.getHttpHostAddress()));
    this.elasticsearchClient =
        elasticsearchClient != null
            ? elasticsearchClient
            : new RestHighLevelClient(restClientBuilder);
  }

  public static DefaultElasticsearchResourceManager.Builder builder(String testId) {
    return new DefaultElasticsearchResourceManager.Builder(testId);
  }

  @Override
  public synchronized String getUri() {
    return connectionString;
  }

  private synchronized boolean indexExists(String indexName) throws IOException {
    // Check index name
    checkValidIndexName(indexName);

    return elasticsearchClient
        .indices()
        .exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
  }

  @Override
  public synchronized boolean createIndex(String indexName) {
    LOG.info("Creating index using name '{}'.", indexName);

    try {
      // Check to see if the index exists
      if (indexExists(indexName)) {
        return false;
      }

      managedIndexNames.add(indexName);

      return elasticsearchClient
          .indices()
          .create(new CreateIndexRequest(indexName), RequestOptions.DEFAULT)
          .isAcknowledged();
    } catch (Exception e) {
      throw new ElasticsearchResourceManagerException("Error creating index.", e);
    }
  }

  @Override
  public synchronized boolean insertDocuments(
      String indexName, Map<String, Map<String, Object>> documents) {
    LOG.info("Attempting to write {} documents to {}.", documents.size(), indexName);

    try {
      BulkRequest request = new BulkRequest();

      for (Map.Entry<String, Map<String, Object>> documentEntry : documents.entrySet()) {
        request.add(
            new IndexRequest(indexName)
                .id(documentEntry.getKey())
                .source(documentEntry.getValue()));
      }

      BulkResponse bulkResponse = elasticsearchClient.bulk(request, RequestOptions.DEFAULT);
      if (!bulkResponse.hasFailures()) {
        LOG.info("Successfully wrote {} documents to {}", documents.size(), indexName);
        return true;
      }

      return false;
    } catch (Exception e) {
      throw new ElasticsearchResourceManagerException("Error inserting documents.", e);
    }
  }

  @Override
  public synchronized List<Map<String, Object>> fetchAll(String indexName) {
    LOG.info("Reading all documents from {}.", indexName);

    try {
      // Elasticsearch is near realtime, so refresh will guarantee reads are consistent
      elasticsearchClient.indices().refresh(new RefreshRequest(indexName), RequestOptions.DEFAULT);

      SearchRequest searchRequest =
          new SearchRequest(indexName).source(new SearchSourceBuilder().size(10_000));
      return Arrays.stream(
              elasticsearchClient.search(searchRequest, RequestOptions.DEFAULT).getHits().getHits())
          .map(SearchHit::getSourceAsMap)
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new ElasticsearchResourceManagerException("Error reading index " + indexName + ".", e);
    }
  }

  @Override
  public long count(String indexName) {
    LOG.info("Fetching count from {}.", indexName);

    try {
      // Elasticsearch is near realtime, so refresh will guarantee reads are consistent
      elasticsearchClient.indices().refresh(new RefreshRequest(indexName), RequestOptions.DEFAULT);

      return elasticsearchClient
          .count(new CountRequest(indexName), RequestOptions.DEFAULT)
          .getCount();
    } catch (Exception e) {
      throw new ElasticsearchResourceManagerException(
          "Error fetching count from " + indexName + ".", e);
    }
  }

  @Override
  public synchronized boolean cleanupAll() {
    LOG.info("Attempting to cleanup Elasticsearch manager.");

    boolean producedError = false;

    // First, delete the database if it was not given as a static argument
    if (!managedIndexNames.isEmpty()) {
      try {
        elasticsearchClient
            .indices()
            .delete(
                new DeleteIndexRequest(
                    managedIndexNames.toArray(new String[managedIndexNames.size()])),
                RequestOptions.DEFAULT);
      } catch (Exception e) {
        LOG.error("Failed to delete Elasticsearch indices {}.", managedIndexNames, e);
        producedError = true;
      }
    }

    // Throw Exception at the end if there were any errors
    if (producedError || !super.cleanupAll()) {
      throw new ElasticsearchResourceManagerException(
          "Failed to delete resources. Check above for errors.");
    }

    LOG.info("Elasticsearch manager successfully cleaned up.");

    return true;
  }

  /** Builder for {@link DefaultElasticsearchResourceManager}. */
  public static final class Builder
      extends TestContainerResourceManager.Builder<DefaultElasticsearchResourceManager> {

    private Builder(String testId) {
      super(testId);
      this.containerImageName = DEFAULT_ELASTICSEARCH_CONTAINER_NAME;
      this.containerImageTag = DEFAULT_ELASTICSEARCH_CONTAINER_TAG;
    }

    @Override
    public DefaultElasticsearchResourceManager build() {
      return new DefaultElasticsearchResourceManager(this);
    }
  }
}
