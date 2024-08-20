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
package org.apache.beam.it.elasticsearch;

import static org.apache.beam.it.elasticsearch.ElasticsearchUtils.checkValidIndexName;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.it.common.ResourceManager;
import org.apache.beam.it.testcontainers.TestContainerResourceManager;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.http.HttpHost;
import org.checkerframework.checker.nullness.qual.Nullable;
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
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Client for managing Elasticsearch resources.
 *
 * <p>The class supports many indices per resource manager object.
 *
 * <p>The index name is formed using testId. The index name will be "{prefix}-{testId}-{ISO8601
 * time, microsecond precision}".
 *
 * <p>The class is thread-safe.
 */
public class ElasticsearchResourceManager extends TestContainerResourceManager<GenericContainer<?>>
    implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchResourceManager.class);

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

  private ElasticsearchResourceManager(Builder builder) {
    this(/* elasticsearchClient= */ null, buildContainer(builder), builder);
  }

  /**
   * Create the {@link ElasticsearchContainer} instance for the given builder.
   *
   * <p>The method override the wait strategy from the base container using the same regex, but
   * increase the timeout to 2 minutes.
   */
  private static ElasticsearchContainer buildContainer(Builder builder) {
    ElasticsearchContainer container =
        new ElasticsearchContainer(
            DockerImageName.parse(builder.containerImageName).withTag(builder.containerImageTag));

    //
    // The regex is based on Elasticsearch container, but it's not exposed anywhere.
    String regex = ".*(\"message\":\\s?\"started[\\s?|\"].*|] started\n$)";
    Duration startupTimeout = Duration.ofMinutes(2);
    container.setWaitStrategy(
        new LogMessageWaitStrategy().withRegEx(regex).withStartupTimeout(startupTimeout));

    return container;
  }

  @VisibleForTesting
  @SuppressWarnings("nullness")
  ElasticsearchResourceManager(
      @Nullable RestHighLevelClient elasticsearchClient,
      ElasticsearchContainer container,
      Builder builder) {
    super(container, builder);

    this.connectionString =
        "http://" + this.getHost() + ":" + this.getPort(ELASTICSEARCH_INTERNAL_PORT);

    RestClientBuilder restClientBuilder =
        RestClient.builder(HttpHost.create(container.getHttpHostAddress()));
    this.elasticsearchClient =
        elasticsearchClient != null
            ? elasticsearchClient
            : new RestHighLevelClient(restClientBuilder);
  }

  public static Builder builder(String testId) {
    return new Builder(testId);
  }

  /** Returns the URI connection string to the Elasticsearch service. */
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

  /**
   * Creates an index on Elasticsearch.
   *
   * @param indexName Name of the index to create.
   * @return A boolean indicating whether the resource was created.
   * @throws ElasticsearchResourceManagerException if there is an error creating the collection in
   *     Elasticsearch.
   */
  public synchronized boolean createIndex(String indexName)
      throws ElasticsearchResourceManagerException {
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

  /**
   * Inserts the given Documents into a collection.
   *
   * <p>Note: Implementations may do collection creation here, if one does not already exist.
   *
   * @param indexName The name of the index to insert the documents into.
   * @param documents A map of (id, document) to insert into the collection.
   * @return A boolean indicating whether the Documents were inserted successfully.
   * @throws ElasticsearchResourceManagerException if there is an error inserting the documents.
   */
  public synchronized boolean insertDocuments(
      String indexName, Map<String, Map<String, Object>> documents)
      throws ElasticsearchResourceManagerException {
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

  /**
   * Reads all the documents in an index.
   *
   * @param indexName The name of the index to read from.
   * @return An iterable of all the Documents in the collection.
   * @throws ElasticsearchResourceManagerException if there is an error reading the data.
   */
  public synchronized List<Map<String, Object>> fetchAll(String indexName)
      throws ElasticsearchResourceManagerException {
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

  /**
   * Gets the count of documents in an index.
   *
   * @param indexName The name of the index to read from.
   * @return The number of documents for the given index
   * @throws ElasticsearchResourceManagerException if there is an error reading the data.
   */
  public long count(String indexName) throws ElasticsearchResourceManagerException {
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

  /**
   * Deletes all created resources and cleans up the Elasticsearch client, making the manager object
   * unusable.
   *
   * @throws ElasticsearchResourceManagerException if there is an error deleting the Elasticsearch
   *     resources.
   */
  @Override
  public synchronized void cleanupAll() throws ElasticsearchResourceManagerException {
    LOG.info("Attempting to cleanup Elasticsearch manager.");

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
        throw new RuntimeException("Failed deleting Elasticsearch indices", e);
      }
    }

    super.cleanupAll();

    LOG.info("Elasticsearch manager successfully cleaned up.");
  }

  /** Builder for {@link ElasticsearchResourceManager}. */
  public static final class Builder
      extends TestContainerResourceManager.Builder<ElasticsearchResourceManager> {

    private Builder(String testId) {
      super(testId, DEFAULT_ELASTICSEARCH_CONTAINER_NAME, DEFAULT_ELASTICSEARCH_CONTAINER_TAG);
    }

    @Override
    public ElasticsearchResourceManager build() {
      return new ElasticsearchResourceManager(this);
    }
  }
}
