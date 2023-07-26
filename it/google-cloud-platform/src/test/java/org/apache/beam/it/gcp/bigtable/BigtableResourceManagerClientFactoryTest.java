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

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import java.io.IOException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link BigtableResourceManagerClientFactory}. */
@RunWith(JUnit4.class)
public class BigtableResourceManagerClientFactoryTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private CredentialsProvider credentialsProvider;

  private static final String INSTANCE_ID = "table-id";
  private static final String PROJECT_ID = "test-project";
  private BigtableResourceManagerClientFactory client;

  @Before
  public void setup() throws IOException {
    BigtableInstanceAdminSettings bigtableInstanceAdminSettings =
        BigtableInstanceAdminSettings.newBuilder()
            .setProjectId(PROJECT_ID)
            .setCredentialsProvider(credentialsProvider)
            .build();
    BigtableTableAdminSettings bigtableTableAdminSettings =
        BigtableTableAdminSettings.newBuilder()
            .setProjectId(PROJECT_ID)
            .setInstanceId(INSTANCE_ID)
            .setCredentialsProvider(credentialsProvider)
            .build();
    BigtableDataSettings bigtableDataSettings =
        BigtableDataSettings.newBuilder()
            .setProjectId(PROJECT_ID)
            .setInstanceId(INSTANCE_ID)
            .setCredentialsProvider(credentialsProvider)
            .build();

    client =
        new BigtableResourceManagerClientFactory(
            bigtableInstanceAdminSettings, bigtableTableAdminSettings, bigtableDataSettings);
  }

  @Test
  public void testClientFactoryGettersReturnCorrectClasses() {
    try (BigtableInstanceAdminClient instanceAdminClient = client.bigtableInstanceAdminClient()) {
      assertThat(instanceAdminClient).isInstanceOf(BigtableInstanceAdminClient.class);
    }
    try (BigtableTableAdminClient tableAdminClient = client.bigtableTableAdminClient()) {
      assertThat(tableAdminClient).isInstanceOf(BigtableTableAdminClient.class);
    }
    try (BigtableDataClient dataClient = client.bigtableDataClient()) {
      assertThat(dataClient).isInstanceOf(BigtableDataClient.class);
    }
  }

  @Test
  public void testClientFactoryGettersReturnClientsWithCorrectIds() {
    try (BigtableInstanceAdminClient instanceAdminClient = client.bigtableInstanceAdminClient()) {
      assertThat(instanceAdminClient.getProjectId()).isEqualTo(PROJECT_ID);
    }
    try (BigtableTableAdminClient tableAdminClient = client.bigtableTableAdminClient()) {
      assertThat(tableAdminClient.getProjectId()).isEqualTo(PROJECT_ID);
    }
  }
}
