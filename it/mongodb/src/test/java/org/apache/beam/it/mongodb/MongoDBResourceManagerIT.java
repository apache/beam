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
package org.apache.beam.it.mongodb;

import static com.google.common.truth.Truth.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.testcontainers.TestContainersIntegrationTest;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link MongoDBResourceManager}. */
@Category(TestContainersIntegrationTest.class)
@RunWith(JUnit4.class)
public class MongoDBResourceManagerIT {

  public static final String COLLECTION_NAME = "dummy-collection";
  private MongoDBResourceManager mongoResourceManager;

  @Before
  public void setUp() {
    mongoResourceManager = MongoDBResourceManager.builder("dummy").build();
  }

  @Test
  public void testResourceManagerE2E() {
    boolean createIndex = mongoResourceManager.createCollection(COLLECTION_NAME);
    assertThat(createIndex).isTrue();

    List<Document> documents = new ArrayList<>();
    documents.add(new Document(ImmutableMap.of("id", 1, "company", "Google")));
    documents.add(new Document(ImmutableMap.of("id", 2, "company", "Alphabet")));

    boolean insertDocuments = mongoResourceManager.insertDocuments(COLLECTION_NAME, documents);
    assertThat(insertDocuments).isTrue();

    List<Document> fetchRecords = mongoResourceManager.readCollection(COLLECTION_NAME);

    assertThat(fetchRecords).hasSize(2);
    assertThat(fetchRecords).containsExactlyElementsIn(documents);
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(mongoResourceManager);
  }
}
