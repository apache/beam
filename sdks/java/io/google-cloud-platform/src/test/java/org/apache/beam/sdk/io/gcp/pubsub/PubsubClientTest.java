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

package org.apache.beam.sdk.io.gcp.pubsub;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.ProjectPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for helper classes and methods in PubsubClient.
 */
@RunWith(JUnit4.class)
public class PubsubClientTest {

  //
  // Paths
  //

  @Test
  public void projectPathFromIdWellFormed() {
    ProjectPath path = PubsubClient.projectPathFromId("test");
    assertEquals("projects/test", path.getPath());
  }

  @Test
  public void subscriptionPathFromNameWellFormed() {
    SubscriptionPath path = PubsubClient.subscriptionPathFromName("test", "something");
    assertEquals("projects/test/subscriptions/something", path.getPath());
    assertEquals("/subscriptions/test/something", path.getV1Beta1Path());
  }

  @Test
  public void topicPathFromNameWellFormed() {
    TopicPath path = PubsubClient.topicPathFromName("test", "something");
    assertEquals("projects/test/topics/something", path.getPath());
    assertEquals("/topics/test/something", path.getV1Beta1Path());
  }
}
