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
package org.apache.beam.it.gcp.bigtable;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.gcp.bigtable.matchers.BigtableAsserts.assertThatBigtableRecords;

import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.GCPBaseIT;
import org.apache.beam.it.gcp.GoogleCloudIntegrationTest;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(GoogleCloudIntegrationTest.class)
@RunWith(JUnit4.class)
public class BigtableResourceManagerIT extends GCPBaseIT {

  private static final String TABLE_ID = "dummy-table";
  private static final String COLUMN_FAMILY = "dummy-cf";
  private BigtableResourceManager bigtableResourceManager;

  @Before
  public void setUp() throws IOException {
    bigtableResourceManager =
        BigtableResourceManager.builder("dummy", PROJECT)
            .setCredentialsProvider(credentialsProvider)
            .build();
  }

  @Test
  public void testResourceManagerE2E() {
    bigtableResourceManager.createTable(TABLE_ID, ImmutableList.of(COLUMN_FAMILY));

    List<RowMutation> mutations = new ArrayList<>();
    mutations.add(
        RowMutation.create(TABLE_ID, "row_0").setCell(COLUMN_FAMILY, "company", "Google"));
    mutations.add(
        RowMutation.create(TABLE_ID, "row_1").setCell(COLUMN_FAMILY, "company", "Alphabet"));
    bigtableResourceManager.write(mutations);

    List<Row> fetchRecords = bigtableResourceManager.readTable(TABLE_ID);

    assertThat(fetchRecords).hasSize(2);
    assertThatBigtableRecords(fetchRecords, COLUMN_FAMILY)
        .hasRecordsUnordered(
            ImmutableList.of(
                Collections.singletonMap("company", "Google"),
                Collections.singletonMap("company", "Alphabet")));
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigtableResourceManager);
  }
}
