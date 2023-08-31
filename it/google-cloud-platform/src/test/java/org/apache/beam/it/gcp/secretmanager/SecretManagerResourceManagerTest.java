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
package org.apache.beam.it.gcp.secretmanager;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.secretmanager.v1.ProjectName;
import com.google.cloud.secretmanager.v1.Secret;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretName;
import java.io.IOException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link SecretManagerResourceManager}. */
@RunWith(JUnit4.class)
public final class SecretManagerResourceManagerTest {
  private static final String PROJECT_ID = "testProject";
  private static final String SECRET_ID = "testSecretId";
  private static final String SECRET_DATA = "testSecretData";

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Mock private SecretManagerServiceClient secretManagerServiceClient;
  private SecretManagerResourceManager testManager;

  @Captor private ArgumentCaptor<Secret> secretCaptor;
  @Captor private ArgumentCaptor<String> secretNameStringCaptor;

  @Captor private ArgumentCaptor<SecretName> secretNameClassCaptor;
  @Captor private ArgumentCaptor<ProjectName> projectNameCaptor;

  @Before
  public void setUp() throws IOException {
    testManager = new SecretManagerResourceManager(PROJECT_ID, secretManagerServiceClient);
  }

  @Test
  public void testBuilderWithInvalidProjectShouldFail() {

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> SecretManagerResourceManager.builder("", null));
    assertThat(exception).hasMessageThat().contains("projectId can not be empty");
  }

  @Test
  public void testCreateSecretWithInvalidNameShouldFail() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> testManager.createSecret("", SECRET_DATA));
    assertThat(exception).hasMessageThat().contains("secretId can not be empty");
  }

  @Test
  public void testAddSecretVersionWithInvalidNameShouldFail() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> testManager.addSecretVersion(SECRET_ID, ""));
    assertThat(exception).hasMessageThat().contains("secretData can not be empty");
  }

  @Test
  public void testAccessSecretWithInvalidNameShouldFail() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> testManager.accessSecret(""));
    assertThat(exception).hasMessageThat().contains("secretVersion can not be empty");
  }

  @Test
  public void testCreateSecretShouldCreate() {
    Secret secret = Secret.getDefaultInstance();
    when(secretManagerServiceClient.createSecret(
            any(ProjectName.class), any(String.class), any(Secret.class)))
        .thenReturn(secret);

    testManager.createSecret(SECRET_ID, SECRET_DATA);

    verify(secretManagerServiceClient)
        .createSecret(
            projectNameCaptor.capture(), secretNameStringCaptor.capture(), secretCaptor.capture());
    ProjectName actualProjectName = projectNameCaptor.getValue();
    assertThat(actualProjectName.getProject()).isEqualTo(PROJECT_ID);
    assertThat(secretNameStringCaptor.getValue()).matches(SECRET_ID);
  }

  @Test
  public void testCleanupTopicsShouldDeleteTopics() {
    Secret secret = Secret.getDefaultInstance();
    when(secretManagerServiceClient.createSecret(
            any(ProjectName.class), any(String.class), any(Secret.class)))
        .thenReturn(secret);

    testManager.createSecret("secret_id_test_1", "secret_data_test_1");
    testManager.cleanupAll();

    verify(secretManagerServiceClient, times(1)).deleteSecret(secretNameClassCaptor.capture());
    assertThat(secretNameClassCaptor.getAllValues()).hasSize(1);
  }
}
