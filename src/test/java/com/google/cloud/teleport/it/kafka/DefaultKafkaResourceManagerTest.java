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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.testcontainers.containers.KafkaContainer;

/** Unit tests for {@link com.google.cloud.teleport.it.kafka.DefaultKafkaResourceManager}. */
@RunWith(JUnit4.class)
public final class DefaultKafkaResourceManagerTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private KafkaContainer container;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private AdminClient kafkaClient;

  private static final String TEST_ID = "test-id";
  private static final String HOST = "localhost";
  private static final String TOPIC_NAME = "topic-name";
  private static final int KAFKA_PORT = 9092;
  private static final int MAPPED_PORT = 10000;

  private DefaultKafkaResourceManager testManager;

  @Before
  public void setUp() throws IOException, InterruptedException {
    when(container.getHost()).thenReturn(HOST);
    when(container.getMappedPort(KAFKA_PORT)).thenReturn(MAPPED_PORT);
    when(container.getBootstrapServers())
        .thenReturn(String.format("PLAINTEXT://%s:%d", HOST, MAPPED_PORT));

    testManager =
        new DefaultKafkaResourceManager(
            kafkaClient, container, DefaultKafkaResourceManager.builder(TEST_ID));
  }

  @Test
  public void testCreateResourceManagerBuilderReturnsDefaultKafkaResourceManager()
      throws IOException {
    assertThat(
            DefaultKafkaResourceManager.builder(TEST_ID)
                .useStaticContainer()
                .setHost(HOST)
                .setPort(KAFKA_PORT)
                .build())
        .isInstanceOf(DefaultKafkaResourceManager.class);
  }

  @Test
  public void testGetBootstrapServersShouldReturnCorrectValue() {
    assertThat(testManager.getBootstrapServers())
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
            DefaultKafkaResourceManager.builder(TEST_ID)
                .setTopicNames(ImmutableSet.of(TOPIC_NAME))
                .setNumTopics(1));
  }

  @Test
  public void testCreateTopicShouldThrowErrorWhenKafkaFailsToListTopics()
      throws ExecutionException, InterruptedException {
    when(kafkaClient.listTopics().names().get())
        .thenThrow(new ExecutionException(new RuntimeException("list topic future fails")));

    assertThrows(KafkaResourceManagerException.class, () -> testManager.createTopic(TOPIC_NAME));
  }

  @Test
  public void testCreateTopicShouldThrowErrorWhenKafkaFailsToCreateTopic()
      throws ExecutionException, InterruptedException {
    when(kafkaClient.listTopics().names().get()).thenReturn(new HashSet<String>());
    when(kafkaClient.createTopics(any(Collection.class)).all().get())
        .thenThrow(new ExecutionException(new RuntimeException("create topic future fails")));

    assertThrows(KafkaResourceManagerException.class, () -> testManager.createTopic(TOPIC_NAME));
  }

  @Test
  public void testCreateTopicShouldReturnFalseIfTopicExists()
      throws ExecutionException, InterruptedException {
    when(kafkaClient.listTopics().names().get()).thenReturn(Collections.singleton(TOPIC_NAME));

    assertFalse(testManager.createTopic(TOPIC_NAME));
  }

  @Test
  public void testCreateTopicShouldWork() throws ExecutionException, InterruptedException {
    when(kafkaClient.listTopics().names().get()).thenReturn(new HashSet<String>());
    when(kafkaClient.createTopics(any(Collection.class)).all().get()).thenReturn(null);

    assertTrue(testManager.createTopic(TOPIC_NAME));
  }

  @Test
  public void testCleanupAllShouldNotDropStaticTopic() throws IOException {
    DefaultKafkaResourceManager.Builder builder =
        DefaultKafkaResourceManager.builder(TEST_ID)
            .setTopicNames(Collections.singleton(TOPIC_NAME));
    DefaultKafkaResourceManager tm =
        new DefaultKafkaResourceManager(kafkaClient, container, builder);

    assertThat(tm.cleanupAll()).isEqualTo(true);

    verify(kafkaClient, never()).deleteTopics(any(Collection.class));
  }

  @Test
  public void testCleanupShouldDropNonStaticTopic() throws IOException {
    final int numTopics = 3;
    DefaultKafkaResourceManager.Builder builder =
        DefaultKafkaResourceManager.builder(TEST_ID).setNumTopics(numTopics);
    DefaultKafkaResourceManager tm =
        new DefaultKafkaResourceManager(kafkaClient, container, builder);

    assertThat(tm.cleanupAll()).isEqualTo(true);
    verify(kafkaClient).deleteTopics(argThat(list -> list.size() == numTopics));
  }

  @Test
  public void testCleanupAllShouldThrowErrorWhenKafkaClientFailsToDeleteTopic()
      throws ExecutionException, InterruptedException {
    when(kafkaClient.deleteTopics(any(Collection.class)).all().get())
        .thenThrow(new ExecutionException(new RuntimeException("delete topic future fails")));

    assertThrows(KafkaResourceManagerException.class, () -> testManager.cleanupAll());
  }
}
