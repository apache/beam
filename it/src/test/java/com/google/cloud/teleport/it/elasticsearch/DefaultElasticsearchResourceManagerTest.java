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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
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

/** Unit tests for {@link DefaultElasticsearchResourceManager}. */
@RunWith(JUnit4.class)
public class DefaultElasticsearchResourceManagerTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private RestHighLevelClient elasticsearchClient;

  @Mock private IndicesClient elasticsearchIndicesClient;
  @Mock private ElasticsearchContainer container;
  @Mock private CreateIndexResponse createIndexResponse;
  @Mock private CountResponse countResponse;

  private static final String TEST_ID = "test-id";
  private static final String INDEX_NAME = "index-name";
  private static final String HOST = "localhost";
  private static final int ELASTICSEARCH_PORT = 9200;
  private static final int MAPPED_PORT = 10000;

  private DefaultElasticsearchResourceManager testManager;

  @Before
  public void setUp() throws IOException, InterruptedException {
    when(container.getHost()).thenReturn(HOST);
    when(container.getMappedPort(ELASTICSEARCH_PORT)).thenReturn(MAPPED_PORT);
    when(container.getHttpHostAddress()).thenReturn(HOST + ":" + MAPPED_PORT);
    when(elasticsearchClient.indices()).thenReturn(elasticsearchIndicesClient);

    testManager =
        new DefaultElasticsearchResourceManager(
            elasticsearchClient, container, DefaultElasticsearchResourceManager.builder(TEST_ID));
  }

  @Test
  public void testGetUriShouldReturnCorrectValue() {
    assertThat(testManager.getUri()).matches("http://" + HOST + ":" + MAPPED_PORT);
  }

  @Test
  public void testCreateIndexShouldThrowErrorWhenCollectionNameIsInvalid() {
    assertThrows(
        ElasticsearchResourceManagerException.class, () -> testManager.createIndex("invalid#name"));
  }

  @Test
  public void testCreateCollectionShouldThrowErrorWhenElasticsearchFailsToGetDB()
      throws IOException {
    doThrow(IllegalArgumentException.class)
        .when(elasticsearchIndicesClient)
        .exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT));

    assertThrows(
        ElasticsearchResourceManagerException.class, () -> testManager.createIndex(INDEX_NAME));
  }

  @Test
  public void testCreateCollectionShouldReturnTrueIfElasticsearchDoesNotThrowAnyError()
      throws IOException {
    when(elasticsearchIndicesClient.exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(false);
    when(createIndexResponse.isAcknowledged()).thenReturn(true);
    when(elasticsearchIndicesClient.create(
            any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(createIndexResponse);

    assertThat(testManager.createIndex(INDEX_NAME)).isEqualTo(true);
    verify(elasticsearchClient.indices())
        .exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT));
  }

  @Test
  public void testInsertDocumentsShouldInsert() throws IOException {
    assertThat(
            testManager.insertDocuments(INDEX_NAME, Map.of("1", Map.of("company", "Google LLC"))))
        .isEqualTo(true);

    verify(elasticsearchClient).bulk(any(BulkRequest.class), eq(RequestOptions.DEFAULT));
  }

  @Test
  public void testCountDocumentsShouldReturnInt() throws IOException {
    when(elasticsearchClient.count(any(CountRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(countResponse);
    when(countResponse.getCount()).thenReturn(100L);

    assertThat(testManager.count(INDEX_NAME)).isEqualTo(100L);
  }

  @Test
  public void testCleanupShouldDropNonStaticIndex() throws IOException {
    when(elasticsearchIndicesClient.create(
            any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(createIndexResponse);
    when(elasticsearchIndicesClient.exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(false);
    when(createIndexResponse.isAcknowledged()).thenReturn(true);

    boolean index = testManager.createIndex("dummy-index");
    assertThat(index).isTrue();
    assertThat(testManager.cleanupAll()).isEqualTo(true);

    verify(elasticsearchIndicesClient)
        .delete(any(DeleteIndexRequest.class), eq(RequestOptions.DEFAULT));
  }

  @Test
  public void testCleanupAllShouldDropStaticIndex() throws IOException {
    DefaultElasticsearchResourceManager.Builder builder =
        DefaultElasticsearchResourceManager.builder(TEST_ID);
    builder.setHost("localhost").setPort(9200).useStaticContainer();

    DefaultElasticsearchResourceManager tm =
        new DefaultElasticsearchResourceManager(elasticsearchClient, container, builder);

    when(elasticsearchIndicesClient.create(
            any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(createIndexResponse);
    when(elasticsearchIndicesClient.exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(false);
    when(createIndexResponse.isAcknowledged()).thenReturn(true);

    boolean index = tm.createIndex("dummy-index");
    assertThat(index).isTrue();
    assertThat(tm.cleanupAll()).isEqualTo(true);

    verify(elasticsearchIndicesClient)
        .delete(any(DeleteIndexRequest.class), eq(RequestOptions.DEFAULT));
  }
}
