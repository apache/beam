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
package org.apache.beam.examples.complete.game.injector;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.api.services.pubsub.model.Topic;
import java.io.IOException;

class InjectorUtils {

  private static final String APP_NAME = "injector";

  /** Builds a new Pubsub client and returns it. */
  public static Pubsub getClient(final HttpTransport httpTransport, final JsonFactory jsonFactory)
      throws IOException {
    checkNotNull(httpTransport);
    checkNotNull(jsonFactory);
    GoogleCredential credential =
        GoogleCredential.getApplicationDefault(httpTransport, jsonFactory);
    if (credential.createScopedRequired()) {
      credential = credential.createScoped(PubsubScopes.all());
    }
    if (credential.getClientAuthentication() != null) {
      System.out.println(
          "\n***Warning! You are not using service account credentials to "
              + "authenticate.\nYou need to use service account credentials for this example,"
              + "\nsince user-level credentials do not have enough pubsub quota,\nand so you will run "
              + "out of PubSub quota very quickly.\nSee "
              + "https://developers.google.com/identity/protocols/application-default-credentials.");
      System.exit(1);
    }
    HttpRequestInitializer initializer = new RetryHttpInitializerWrapper(credential);
    return new Pubsub.Builder(httpTransport, jsonFactory, initializer)
        .setApplicationName(APP_NAME)
        .build();
  }

  /** Builds a new Pubsub client with default HttpTransport and JsonFactory and returns it. */
  public static Pubsub getClient() throws IOException {
    return getClient(Utils.getDefaultTransport(), Utils.getDefaultJsonFactory());
  }

  /** Returns the fully qualified topic name for Pub/Sub. */
  public static String getFullyQualifiedTopicName(final String project, final String topic) {
    return String.format("projects/%s/topics/%s", project, topic);
  }

  /** Create a topic if it doesn't exist. */
  public static void createTopic(Pubsub client, String fullTopicName) throws IOException {
    System.out.println("fullTopicName " + fullTopicName);
    try {
      client.projects().topics().get(fullTopicName).execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
        Topic topic = client.projects().topics().create(fullTopicName, new Topic()).execute();
        System.out.printf("Topic %s was created.%n", topic.getName());
      }
    }
  }
}
