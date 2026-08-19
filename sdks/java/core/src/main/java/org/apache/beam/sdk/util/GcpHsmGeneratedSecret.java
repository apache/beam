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
package org.apache.beam.sdk.util;

import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.kms.v1.EncryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.ProjectName;
import com.google.cloud.secretmanager.v1.Replication;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretName;
import com.google.cloud.secretmanager.v1.SecretPayload;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.crypto.tink.subtle.Hkdf;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link org.apache.beam.sdk.util.Secret} manager implementation that generates a secret using
 * entropy from a GCP HSM key and stores it in Google Cloud Secret Manager. If the secret already
 * exists, it will be retrieved.
 */
public class GcpHsmGeneratedSecret extends Secret {
  private static final Logger LOG = LoggerFactory.getLogger(GcpHsmGeneratedSecret.class);
  private final String projectId;
  private final String locationId;
  private final String keyRingId;
  private final String keyId;
  private final String secretId;

  private final SecureRandom random = new SecureRandom();

  public GcpHsmGeneratedSecret(
      String projectId, String locationId, String keyRingId, String keyId, String jobName) {
    this.projectId = projectId;
    this.locationId = locationId;
    this.keyRingId = keyRingId;
    this.keyId = keyId;
    this.secretId = "HsmGeneratedSecret_" + jobName;
  }

  /** Initialize GcpHsmGeneratedSecret from a map specification. */
  static GcpHsmGeneratedSecret fromMap(Map<String, String> specMap) {
    Set<String> allowedKeys =
        new HashSet<>(
            Arrays.asList("project_id", "location_id", "key_ring_id", "key_id", "job_name"));
    Set<String> invalid = new HashSet<>(specMap.keySet());
    invalid.removeAll(allowedKeys);
    if (!invalid.isEmpty()) {
      List<String> sortedInvalid = new ArrayList<>(invalid);
      Collections.sort(sortedInvalid);
      throw new IllegalArgumentException(
          "Invalid secret parameter " + String.join(", ", sortedInvalid));
    }
    String locationId =
        Preconditions.checkNotNull(
            specMap.get("location_id"),
            "location_id must contain a valid value for locationId parameter");
    String keyRingId =
        Preconditions.checkNotNull(
            specMap.get("key_ring_id"),
            "key_ring_id must contain a valid value for keyRingId parameter");
    String keyId =
        Preconditions.checkNotNull(
            specMap.get("key_id"), "key_id must contain a valid value for keyId parameter");
    String jobName =
        Preconditions.checkNotNull(
            specMap.get("job_name"), "job_name must contain a valid value for jobName parameter");
    String projectId =
        GcpSecret.resolveGcpProjectId(specMap.get("project_id"), "job '" + jobName + "'");
    return new GcpHsmGeneratedSecret(projectId, locationId, keyRingId, keyId, jobName);
  }

  /**
   * Returns the secret as a byte array. Assumes that the current active service account has
   * permissions to read the secret.
   *
   * @return The secret as a byte array.
   */
  @Override
  public byte[] getSecretBytes() {
    try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
      SecretVersionName secretVersionName = SecretVersionName.of(projectId, secretId, "1");

      try {
        AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);
        return response.getPayload().getData().toByteArray();
      } catch (NotFoundException e) {
        LOG.info(
            "Secret version {} not found. Creating new secret and version.",
            secretVersionName.toString());
      }

      ProjectName projectName = ProjectName.of(projectId);
      SecretName secretName = SecretName.of(projectId, secretId);
      try {
        com.google.cloud.secretmanager.v1.Secret secret =
            com.google.cloud.secretmanager.v1.Secret.newBuilder()
                .setReplication(
                    Replication.newBuilder()
                        .setAutomatic(Replication.Automatic.newBuilder().build()))
                .build();
        client.createSecret(projectName, secretId, secret);
      } catch (AlreadyExistsException e) {
        LOG.info("Secret {} already exists. Adding new version.", secretName.toString());
      }

      byte[] newKey = generateDek();

      try {
        // Always retrieve remote secret as source-of-truth in case another thread created it
        AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);
        return response.getPayload().getData().toByteArray();
      } catch (NotFoundException e) {
        LOG.info(
            "Secret version {} not found after re-check. Creating new secret and version.",
            secretVersionName.toString());
      }

      SecretPayload payload =
          SecretPayload.newBuilder().setData(ByteString.copyFrom(newKey)).build();
      client.addSecretVersion(secretName, payload);
      AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);
      return response.getPayload().getData().toByteArray();

    } catch (IOException | GeneralSecurityException e) {
      throw new RuntimeException("Failed to retrieve or create secret bytes", e);
    }
  }

  private byte[] generateDek() throws IOException, GeneralSecurityException {
    int dekSize = 32;
    try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {
      // 1. Generate nonce_one. This doesn't need to have baked in randomness since the
      // actual randomness comes from KMS.
      byte[] nonceOne = new byte[dekSize];
      random.nextBytes(nonceOne);

      // 2. Encrypt to get nonce_two
      CryptoKeyName keyName = CryptoKeyName.of(projectId, locationId, keyRingId, keyId);
      EncryptResponse response = client.encrypt(keyName, ByteString.copyFrom(nonceOne));
      byte[] nonceTwo = response.getCiphertext().toByteArray();

      // 3. Generate DK
      byte[] dk = new byte[dekSize];
      random.nextBytes(dk);

      // 4. Derive DEK using HKDF
      byte[] dek = Hkdf.computeHkdf("HmacSha256", dk, nonceTwo, new byte[0], dekSize);

      // 5. Base64 encode
      return Base64.getUrlEncoder().encode(dek);
    }
  }

  /**
   * Returns the project ID of the secret.
   *
   * @return The project ID as a String.
   */
  public String getProjectId() {
    return projectId;
  }

  /**
   * Returns the location ID of the secret.
   *
   * @return The location ID as a String.
   */
  public String getLocationId() {
    return locationId;
  }

  /**
   * Returns the key ring ID of the secret.
   *
   * @return The key ring ID as a String.
   */
  public String getKeyRingId() {
    return keyRingId;
  }

  /**
   * Returns the key ID of the secret.
   *
   * @return The key ID as a String.
   */
  public String getKeyId() {
    return keyId;
  }

  /**
   * Returns the secret ID of the secret.
   *
   * @return The secret ID as a String.
   */
  public String getSecretId() {
    return secretId;
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof GcpHsmGeneratedSecret)) {
      return false;
    }
    GcpHsmGeneratedSecret other = (GcpHsmGeneratedSecret) obj;
    return Objects.equals(this.projectId, other.projectId)
        && Objects.equals(this.locationId, other.locationId)
        && Objects.equals(this.keyRingId, other.keyRingId)
        && Objects.equals(this.keyId, other.keyId)
        && Objects.equals(this.secretId, other.secretId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(projectId, locationId, keyRingId, keyId, secretId);
  }
}
