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

import com.google.cloud.teleport.it.testcontainers.TestContainersIntegrationTest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link DefaultElasticsearchResourceManager}. */
@Category(TestContainersIntegrationTest.class)
@RunWith(JUnit4.class)
public class DefaultElasticsearchResourceManagerIT {

  private DefaultElasticsearchResourceManager elasticsearchResourceManager;

  @Before
  public void setUp() {
    elasticsearchResourceManager = DefaultElasticsearchResourceManager.builder("dummy").build();
  }

  @Test
  public void testResourceManagerE2E() {
    boolean createIndex = elasticsearchResourceManager.createIndex("dummy-insert");
    assertThat(createIndex).isTrue();

    Map<String, Map<String, Object>> records = new HashMap<>();
    records.put("1", Map.of("company", "Google"));
    records.put("2", Map.of("company", "Alphabet"));

    boolean insertDocuments = elasticsearchResourceManager.insertDocuments("dummy-insert", records);
    assertThat(insertDocuments).isTrue();

    long count = elasticsearchResourceManager.count("dummy-insert");
    assertThat(count).isEqualTo(2L);

    List<Map<String, Object>> fetchRecords = elasticsearchResourceManager.fetchAll("dummy-insert");
    assertThat(fetchRecords).hasSize(2);
    assertThat(fetchRecords)
        .containsExactlyElementsIn(
            List.of(Map.of("company", "Google"), Map.of("company", "Alphabet")));
  }

  @After
  public void tearDown() {
    elasticsearchResourceManager.cleanupAll();
  }
}
