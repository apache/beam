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
package org.apache.beam.it.neo4j;

import static com.google.common.truth.Truth.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.testcontainers.TestContainersIntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link Neo4jResourceManager}. */
@Category(TestContainersIntegrationTest.class)
@RunWith(JUnit4.class)
public class Neo4jResourceManagerIT {

  private Neo4jResourceManager neo4jResourceManager;

  @Before
  public void setUp() {
    neo4jResourceManager =
        Neo4jResourceManager.builder("placeholder")
            .setDatabaseName("neo4j", DatabaseWaitOptions.waitDatabase())
            .setAdminPassword("password")
            .build();
  }

  @Test
  public void testResourceManagerE2E() {
    neo4jResourceManager.run(
        "CREATE (:Hello {whom: $whom})", Collections.singletonMap("whom", "world"));

    List<Map<String, Object>> results =
        neo4jResourceManager.run("MATCH (h:Hello) RETURN h.whom AS whom");

    assertThat(results).hasSize(1);
    assertThat(results)
        .containsExactlyElementsIn(
            Collections.singletonList(Collections.singletonMap("whom", "world")));
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(neo4jResourceManager);
  }
}
