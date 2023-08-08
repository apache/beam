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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.ProjectName;
import com.google.cloud.secretmanager.v1.Replication;
import com.google.cloud.secretmanager.v1.Secret;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretName;
import com.google.cloud.secretmanager.v1.SecretPayload;
import com.google.cloud.secretmanager.v1.SecretVersion;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.it.common.ResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default class for implementation of {@link ResourceManager} interface.
 *
 * <p>The class provides an interaction with the real GCP Secret Manager client, with operations
 * related to management of Secrets in the SecretManager.
 */
public class SecretManagerResourceManager implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(SecretManagerResourceManager.class);

  private final String projectId;

  private final SecretManagerServiceClient secretManagerServiceClient;

  private final Set<String> createdSecretIds;

  private SecretManagerResourceManager(Builder builder) throws IOException {
    this(builder.projectId, SecretManagerServiceClient.create());
  }

  @VisibleForTesting
  public SecretManagerResourceManager(
      String projectId, SecretManagerServiceClient secretManagerServiceClient) {
    this.projectId = projectId;
    this.secretManagerServiceClient = secretManagerServiceClient;
    this.createdSecretIds = Collections.synchronizedSet(new HashSet<>());
  }

  public static Builder builder(String projectId, CredentialsProvider credentialsProvider) {
    checkArgument(!projectId.isEmpty(), "projectId can not be empty");
    return new Builder(projectId, credentialsProvider);
  }

  /**
   * Creates a Secret with given name on GCP Secret Manager.
   *
   * @param secretId Secret ID of the secret to be created
   * @param secretData Value of the secret to be added
   */
  public void createSecret(String secretId, String secretData) {
    checkArgument(!secretId.isEmpty(), "secretId can not be empty");
    checkArgument(!secretData.isEmpty(), "secretData can not be empty");
    try {
      checkIsUsable();
      ProjectName projectName = ProjectName.of(projectId);

      // Create the parent secret.
      Secret secret =
          Secret.newBuilder()
              .setReplication(
                  Replication.newBuilder()
                      .setAutomatic(Replication.Automatic.newBuilder().build())
                      .build())
              .build();

      Secret createdSecret = secretManagerServiceClient.createSecret(projectName, secretId, secret);

      // Add a secret version.
      SecretPayload payload =
          SecretPayload.newBuilder().setData(ByteString.copyFromUtf8(secretData)).build();
      secretManagerServiceClient.addSecretVersion(createdSecret.getName(), payload);

      createdSecretIds.add(secretId);

      LOG.info("Created secret successfully.");

    } catch (Exception e) {
      throw new SecretManagerResourceManagerException("Error while creating secret", e);
    }
  }

  /**
   * Creates a new SecretVersion containing secret data and attaches it to an existing Secret.
   *
   * @param secretId Parent secret to which version will be added
   * @param secretData Value of the secret to be added
   */
  public void addSecretVersion(String secretId, String secretData) {
    checkArgument(!secretId.isEmpty(), "secretId can not be empty");
    checkArgument(!secretData.isEmpty(), "secretData can not be empty");
    checkIsUsable();
    try {
      SecretName parent = SecretName.of(projectId, secretId);
      SecretPayload secretPayload =
          SecretPayload.newBuilder().setData(ByteString.copyFromUtf8(secretData)).build();
      secretManagerServiceClient.addSecretVersion(parent, secretPayload);
    } catch (Exception e) {
      throw new SecretManagerResourceManagerException("Error while adding version to a secret", e);
    }
  }

  /**
   * Calls Secret Manager with a Secret Version and returns the secret value.
   *
   * @param secretVersion Secret Version of the form
   *     projects/{project}/secrets/{secret}/versions/{secret_version}
   * @return the secret value in Secret Manager
   */
  public String accessSecret(String secretVersion) {

    checkArgument(!secretVersion.isEmpty(), "secretVersion can not be empty");
    checkIsUsable();
    try {
      SecretVersionName secretVersionName;

      if (SecretVersionName.isParsableFrom(secretVersion)) {
        secretVersionName = SecretVersionName.parse(secretVersion);

      } else {
        throw new IllegalArgumentException(
            "Provided Secret must be in the form"
                + " projects/{project}/secrets/{secret}/versions/{secret_version}");
      }
      AccessSecretVersionResponse response =
          secretManagerServiceClient.accessSecretVersion(secretVersionName);
      return response.getPayload().getData().toStringUtf8();

    } catch (Exception e) {
      throw new SecretManagerResourceManagerException("Error while accessing a secret version", e);
    }
  }

  /**
   * Enables a SecretVersion. Sets the state of the SecretVersion to ENABLED.
   *
   * @param secretVersion The resource name of the SecretVersion to destroy in the format of
   *     projects/{project}/secrets/{secret}/versions/{secret_version}
   */
  public void enableSecretVersion(String secretVersion) {
    checkIsUsable();
    if (!SecretVersionName.isParsableFrom(secretVersion)) {
      throw new IllegalArgumentException(
          "Provided Secret must be in the form"
              + " projects/{project}/secrets/{secret}/versions/{secret_version}");
    }
    SecretVersionName secretVersionName = SecretVersionName.parse(secretVersion);
    SecretVersion response = secretManagerServiceClient.enableSecretVersion(secretVersionName);
    LOG.info("The current state of secret version is '{}'", response.getState().toString());
  }

  /**
   * Disables a SecretVersion. Sets the state of the SecretVersion to DISABLED.
   *
   * @param secretVersion The resource name of the SecretVersion to destroy in the format of
   *     projects/{project}/secrets/{secret}/versions/{secret_version}
   */
  public void disableSecretVersion(String secretVersion) {
    checkIsUsable();
    if (!SecretVersionName.isParsableFrom(secretVersion)) {
      throw new IllegalArgumentException(
          "Provided Secret must be in the form"
              + " projects/{project}/secrets/{secret}/versions/{secret_version}");
    }
    SecretVersionName secretVersionName = SecretVersionName.parse(secretVersion);
    SecretVersion response = secretManagerServiceClient.disableSecretVersion(secretVersionName);
    LOG.info("The current state of secret version is '{}'", response.getState().toString());
  }

  /**
   * Sets the state of the SecretVersion to DESTROYED and irrevocably destroys the secret data.
   *
   * @param secretVersion The resource name of the SecretVersion to destroy in the format of
   *     projects/{project}/secrets/{secret}/versions/{secret_version}
   */
  public void destroySecretVersion(String secretVersion) {
    checkIsUsable();
    if (!SecretVersionName.isParsableFrom(secretVersion)) {
      throw new IllegalArgumentException(
          "Provided Secret must be in the form"
              + " projects/{project}/secrets/{secret}/versions/{secret_version}");
    }
    SecretVersionName secretVersionName = SecretVersionName.parse(secretVersion);
    SecretVersion response = secretManagerServiceClient.destroySecretVersion(secretVersionName);
    LOG.info("The current state of secret version is '{}'", response.getState().toString());
  }

  /**
   * Deletes a Secret.
   *
   * @param secretId The resource name of the SecretVersion to delete in the format of
   *     projects/{project}/secrets/{secret}/versions/{secret_version}
   */
  public void deleteSecret(String secretId) {
    checkIsUsable();
    try {
      SecretName secret = SecretName.of(projectId, secretId);
      secretManagerServiceClient.deleteSecret(secret);
      createdSecretIds.remove(secretId);

      LOG.info("Successfully deleted secret");
    } catch (Exception e) {
      throw new SecretManagerResourceManagerException("Error while deleting a secret", e);
    }
  }

  @Override
  public synchronized void cleanupAll() {

    LOG.info("Attempting to cleanup manager.");

    try {
      for (String secretId : createdSecretIds) {
        LOG.info("Deleting secretId '{}'", secretId);
        deleteSecret(secretId);
      }

    } finally {
      secretManagerServiceClient.close();
    }

    LOG.info("Manager successfully cleaned up.");
  }

  /**
   * Check if the clients started by this instance are still usable, and throwing {@link
   * IllegalStateException} otherwise.
   */
  private void checkIsUsable() throws IllegalStateException {
    if (isNotUsable()) {
      throw new IllegalStateException("Manager has cleaned up resources and is unusable.");
    }
  }

  private boolean isNotUsable() {
    return secretManagerServiceClient.isShutdown() || secretManagerServiceClient.isTerminated();
  }

  /** Builder for {@link SecretManagerResourceManager}. */
  public static final class Builder {
    private final String projectId;

    private CredentialsProvider credentialsProvider;

    private Builder(String projectId, CredentialsProvider credentialsProvider) {
      this.projectId = projectId;
      this.credentialsProvider = credentialsProvider;
    }

    public Builder credentialsProvider(CredentialsProvider credentialsProvider) {
      this.credentialsProvider = credentialsProvider;
      return this;
    }

    public SecretManagerResourceManager build() throws IOException {
      if (credentialsProvider == null) {
        throw new IllegalArgumentException(
            "Unable to find credentials. Please provide credentials to authenticate to GCP");
      }
      return new SecretManagerResourceManager(this);
    }
  }
}
