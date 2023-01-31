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
package com.google.cloud.teleport.it.pubsub;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;

/**
 * Default class for implementation of {@link PubsubPublisherFactory} interface.
 *
 * <p>The class provides an interaction with the real Pub/Sub client, with operations related to
 * creating a Pub/Sub Publisher.
 */
class DefaultPubsubPublisherFactory implements PubsubPublisherFactory {

  private final CredentialsProvider credentialsProvider;

  DefaultPubsubPublisherFactory(CredentialsProvider credentialsProvider) {
    this.credentialsProvider = credentialsProvider;
  }

  @Override
  public Publisher createPublisher(TopicName topic) {
    try {
      return Publisher.newBuilder(topic).setCredentialsProvider(credentialsProvider).build();
    } catch (IOException e) {
      throw new PubsubResourceManagerException("Error creating publisher for topic", e);
    }
  }
}
