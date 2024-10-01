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
package org.apache.beam.it.kafka;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Answers;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.testcontainers.containers.KafkaContainer;

/** Unit tests for {@link KafkaResourceManager}. */
@RunWith(JUnit4.class)
public final class KafkaResourceManagerTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private KafkaContainer container;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private AdminClient kafkaClient;

  private static final String TEST_ID = "test-id";
  private static final String HOST = "localhost";
  private static final String TOPIC_NAME = "topic-name";
  private static final int KAFKA_PORT = 9093;
  private static final int MAPPED_PORT = 10000;

  private KafkaResourceManager testManager;

  @Before
  public void setUp() throws IOException {
    doReturn(container).when(container).withLogConsumer(any());
    testManager =
        new KafkaResourceManager(kafkaClient, container, KafkaResourceManager.builder(TEST_ID));
  }

  @Test
  public void testCreateResourceManagerBuilderReturnsKafkaResourceManager() throws IOException {
    assertThat(
            KafkaResourceManager.builder(TEST_ID)
                .useStaticContainer()
                .setHost(HOST)
                .setPort(KAFKA_PORT)
                .build())
        .isInstanceOf(KafkaResourceManager.class);
  }

  @Test
  public void testGetBootstrapServersShouldReturnCorrectValue() throws IOException {
    when(container.getHost()).thenReturn(HOST);
    when(container.getMappedPort(KAFKA_PORT)).thenReturn(MAPPED_PORT);
    assertThat(
            new KafkaResourceManager(kafkaClient, container, KafkaResourceManager.builder(TEST_ID))
                .getBootstrapServers())
        .matches("PLAINTEXT://" + HOST + ":" + MAPPED_PORT);
  }

  @Test
  public void testGetTopicNameShouldReturnCorrectValue() {
    for (String topicName : testManager.getTopicNames()) {
      assertThat(topicName).matches(TEST_ID + "-\\d" + "-\\d{8}-\\d{6}-\\d{6}");
    }
  }

  @Test
  public void testSetTopicNamesAndSetNumTopicsExclusive() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            KafkaResourceManager.builder(TEST_ID)
                .setTopicNames(ImmutableSet.of(TOPIC_NAME))
                .setNumTopics(1));
  }

  @Test
  public void testCreateTopicZeroPartitionsThrowErrors() {
    assertThrows(IllegalArgumentException.class, () -> testManager.createTopic(TOPIC_NAME, 0));
  }

  @Test
  public void testCreateTopicShouldThrowErrorWhenKafkaFailsToListTopics()
      throws ExecutionException, InterruptedException {
    when(kafkaClient.listTopics().names().get())
        .thenThrow(new ExecutionException(new RuntimeException("list topic future fails")));

    assertThrows(KafkaResourceManagerException.class, () -> testManager.createTopic(TOPIC_NAME, 1));
  }

  @Test
  public void testCreateTopicShouldThrowErrorWhenKafkaFailsToCreateTopic()
      throws ExecutionException, InterruptedException {
    when(kafkaClient.createTopics(any(Collection.class)).all().get())
        .thenThrow(new ExecutionException(new RuntimeException("create topic future fails")));

    assertThrows(KafkaResourceManagerException.class, () -> testManager.createTopic(TOPIC_NAME, 1));
  }

  @Test
  public void testCreateTopicShouldReturnTopicIfTopicExists()
      throws ExecutionException, InterruptedException {
    when(kafkaClient.listTopics().names().get()).thenReturn(Collections.singleton(TOPIC_NAME));

    assertNotNull(testManager.createTopic(TOPIC_NAME, 1));
  }

  @Test
  public void testCreateTopicShouldWork() throws ExecutionException, InterruptedException {
    when(kafkaClient.createTopics(anyCollection()).all().get()).thenReturn(null);

    assertNotNull(testManager.createTopic(TOPIC_NAME, 1));
  }

  @Test
  public void testCleanupAllShouldNotDropStaticTopic() throws IOException {
    KafkaResourceManager.Builder builder =
        KafkaResourceManager.builder(TEST_ID).setTopicNames(Collections.singleton(TOPIC_NAME));
    KafkaResourceManager tm = new KafkaResourceManager(kafkaClient, container, builder);

    tm.cleanupAll();

    verify(kafkaClient, never()).deleteTopics(anyCollection());
  }

  @Test
  public void testCleanupShouldDropNonStaticTopic() throws IOException {
    final int numTopics = 3;
    KafkaResourceManager.Builder builder =
        KafkaResourceManager.builder(TEST_ID).setNumTopics(numTopics);
    KafkaResourceManager tm = new KafkaResourceManager(kafkaClient, container, builder);

    tm.cleanupAll();
    verify(kafkaClient)
        .deleteTopics(
            argThat((ArgumentMatcher<Collection<String>>) list -> list.size() == numTopics));
  }

  @Test
  public void testCleanupAllShouldThrowErrorWhenKafkaClientFailsToDeleteTopic()
      throws ExecutionException, InterruptedException {
    when(kafkaClient.deleteTopics(anyCollection()).all().get())
        .thenThrow(new ExecutionException(new RuntimeException("delete topic future fails")));

    assertThrows(KafkaResourceManagerException.class, () -> testManager.cleanupAll());
  }
}
