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
package com.google.cloud.teleport.it.kafka;

import java.util.Set;

/** Interface for managing Kafka resources in integration tests. */
public interface KafkaResourceManager {
  /**
   * Returns a list of names of the topics that this kafka manager will operate in.
   *
   * @return names of the kafka topics.
   */
  Set<String> getTopicNames();

  /** Returns the kafka boostrap server connection string. */
  String getBootstrapServers();

  /**
   * Deletes all created resources and cleans up the Kafka client, making the manager object
   * unusable.
   *
   * @throws KafkaResourceManagerException if there is an error deleting the Kafka resources.
   */
  boolean cleanupAll();

  /**
   * Creates a kafka topic.
   *
   * <p>Note: Implementations may do topic creation here, if one does not already exist.
   *
   * @param topicName Topic name to associate with the given kafka instance.
   * @return A boolean indicating whether the resource was created.
   * @throws KafkaResourceManagerException if there is an error creating the kafka topic.
   */
  boolean createTopic(String topicName);
}
