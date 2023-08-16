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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

/** Unit tests for {@link ElasticsearchResourceManager}. */
@RunWith(JUnit4.class)
public class ElasticsearchResourceManagerTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private RestHighLevelClient elasticsearchClient;

  @Mock private ElasticsearchContainer container;

  private static final String TEST_ID = "test-id";
  private static final String INDEX_NAME = "index-name";
  private static final String HOST = "localhost";
  private static final int ELASTICSEARCH_PORT = 9200;
  private static final int MAPPED_PORT = 10000;

  private ElasticsearchResourceManager testManager;

  @Before
  public void setUp() {
    when(container.getHttpHostAddress()).thenReturn(HOST + ":" + MAPPED_PORT);
    testManager =
        new ElasticsearchResourceManager(
            elasticsearchClient, container, ElasticsearchResourceManager.builder(TEST_ID));
  }

  @Test
  public void testGetUriShouldReturnCorrectValue() {
    when(container.getHost()).thenReturn(HOST);
    when(container.getMappedPort(ELASTICSEARCH_PORT)).thenReturn(MAPPED_PORT);

    assertThat(
            new ElasticsearchResourceManager(
                    elasticsearchClient, container, ElasticsearchResourceManager.builder(TEST_ID))
                .getUri())
        .matches("http://" + HOST + ":" + MAPPED_PORT);
  }

  @Test
  public void testCreateIndexShouldThrowErrorWhenCollectionNameIsInvalid() {
    assertThrows(
        ElasticsearchResourceManagerException.class, () -> testManager.createIndex("invalid#name"));
  }

  @Test
  public void testCreateCollectionShouldThrowErrorWhenElasticsearchFailsToGetDB()
      throws IOException {
    when(elasticsearchClient
            .indices()
            .exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(IllegalArgumentException.class);

    assertThrows(
        ElasticsearchResourceManagerException.class, () -> testManager.createIndex(INDEX_NAME));
  }

  @Test
  public void testCreateCollectionShouldReturnTrueIfElasticsearchDoesNotThrowAnyError()
      throws IOException {
    when(elasticsearchClient
            .indices()
            .exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(false);
    when(elasticsearchClient
            .indices()
            .create(any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT))
            .isAcknowledged())
        .thenReturn(true);

    assertThat(testManager.createIndex(INDEX_NAME)).isEqualTo(true);
    verify(elasticsearchClient.indices())
        .exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT));
  }

  @Test
  public void testInsertDocumentsShouldInsert() throws IOException {
    assertThat(
            testManager.insertDocuments(
                INDEX_NAME, ImmutableMap.of("1", ImmutableMap.of("company", "Google LLC"))))
        .isEqualTo(true);

    verify(elasticsearchClient).bulk(any(BulkRequest.class), eq(RequestOptions.DEFAULT));
  }

  @Test
  public void testCountDocumentsShouldReturnInt() throws IOException {
    when(elasticsearchClient.count(any(CountRequest.class), eq(RequestOptions.DEFAULT)).getCount())
        .thenReturn(100L);

    assertThat(testManager.count(INDEX_NAME)).isEqualTo(100L);
  }

  @Test
  public void testCleanupShouldDropNonStaticIndex() throws IOException {
    when(elasticsearchClient
            .indices()
            .exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(false);
    when(elasticsearchClient
            .indices()
            .create(any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT))
            .isAcknowledged())
        .thenReturn(true);

    boolean index = testManager.createIndex("dummy-index");
    assertThat(index).isTrue();
    testManager.cleanupAll();

    verify(elasticsearchClient.indices())
        .delete(any(DeleteIndexRequest.class), eq(RequestOptions.DEFAULT));
  }

  @Test
  public void testCleanupAllShouldDropStaticIndex() throws IOException {
    ElasticsearchResourceManager.Builder builder = ElasticsearchResourceManager.builder(TEST_ID);
    builder.setHost("localhost").setPort(9200).useStaticContainer();

    ElasticsearchResourceManager tm =
        new ElasticsearchResourceManager(elasticsearchClient, container, builder);

    when(elasticsearchClient
            .indices()
            .exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(false);
    when(elasticsearchClient
            .indices()
            .create(any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT))
            .isAcknowledged())
        .thenReturn(true);

    boolean index = tm.createIndex("dummy-index");
    assertThat(index).isTrue();
    tm.cleanupAll();

    verify(elasticsearchClient.indices())
        .delete(any(DeleteIndexRequest.class), eq(RequestOptions.DEFAULT));
  }
}
