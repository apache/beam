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

import java.util.List;
import java.util.Map;

/** Interface for managing Elasticsearch resources in integration tests. */
public interface ElasticsearchResourceManager {

  /** Returns the URI connection string to the Elasticsearch service. */
  String getUri();

  /**
   * Creates an index on Elasticsearch.
   *
   * @param indexName Name of the index to create.
   * @return A boolean indicating whether the resource was created.
   * @throws ElasticsearchResourceManagerException if there is an error creating the collection in
   *     Elasticsearch.
   */
  boolean createIndex(String indexName);

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
  boolean insertDocuments(String indexName, Map<String, Map<String, Object>> documents);

  /**
   * Reads all the documents in an index.
   *
   * @param indexName The name of the index to read from.
   * @return An iterable of all the Documents in the collection.
   * @throws ElasticsearchResourceManagerException if there is an error reading the data.
   */
  List<Map<String, Object>> fetchAll(String indexName);

  /**
   * Gets the count of documents in an index.
   *
   * @param indexName The name of the index to read from.
   * @return The number of documents for the given index
   * @throws ElasticsearchResourceManagerException if there is an error reading the data.
   */
  long count(String indexName);

  /**
   * Deletes all created resources and cleans up the Elasticsearch client, making the manager object
   * unusable.
   *
   * @throws ElasticsearchResourceManagerException if there is an error deleting the Elasticsearch
   *     resources.
   */
  boolean cleanupAll();
}
