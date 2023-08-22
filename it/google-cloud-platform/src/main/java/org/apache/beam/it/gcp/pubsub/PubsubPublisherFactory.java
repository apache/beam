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
package org.apache.beam.it.gcp.pubsub;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;

/**
 * Client for building Pub/Sub publishers.
 *
 * <p>The class provides an interaction with the real Pub/Sub client, with operations related to
 * creating a Pub/Sub Publisher.
 */
public class PubsubPublisherFactory {

  private final CredentialsProvider credentialsProvider;

  PubsubPublisherFactory(CredentialsProvider credentialsProvider) {
    this.credentialsProvider = credentialsProvider;
  }

  /** Create a {@link Publisher} instance for the given topic reference. */
  public Publisher createPublisher(TopicName topic) {
    try {
      return Publisher.newBuilder(topic).setCredentialsProvider(credentialsProvider).build();
    } catch (IOException e) {
      throw new PubsubResourceManagerException("Error creating publisher for topic", e);
    }
  }
}
