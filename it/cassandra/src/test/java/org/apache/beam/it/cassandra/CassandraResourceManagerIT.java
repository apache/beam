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
package org.apache.beam.it.cassandra;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.cassandra.matchers.CassandraAsserts.assertThatCassandraRecords;

import com.datastax.oss.driver.api.core.cql.Row;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.testcontainers.TestContainersIntegrationTest;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link CassandraResourceManager}. */
@Category(TestContainersIntegrationTest.class)
@RunWith(JUnit4.class)
public class CassandraResourceManagerIT {

  private CassandraResourceManager cassandraResourceManager;

  @Before
  public void setUp() throws IOException {
    cassandraResourceManager = CassandraResourceManager.builder("dummy").build();
  }

  @Test
  public void testResourceManagerE2E() {

    List<Map<String, Object>> records = new ArrayList<>();
    records.add(ImmutableMap.of("id", 1, "company", "Google"));
    records.add(ImmutableMap.of("id", 2, "company", "Alphabet"));

    cassandraResourceManager.executeStatement(
        "CREATE TABLE dummy_insert ( id int PRIMARY KEY, company text )");

    boolean insertDocuments = cassandraResourceManager.insertDocuments("dummy_insert", records);
    assertThat(insertDocuments).isTrue();

    Iterable<Row> fetchRecords = cassandraResourceManager.readTable("dummy_insert");
    assertThatCassandraRecords(fetchRecords).hasRows(2);
    assertThatCassandraRecords(fetchRecords).hasRecordsUnordered(records);
  }

  @After
  public void tearDown() {
    if (cassandraResourceManager != null) {
      cassandraResourceManager.cleanupAll();
    }
  }
}
