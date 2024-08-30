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
package org.apache.beam.it.gcp.kms;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.kms.v1.CryptoKey;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.kms.v1.CryptoKeyVersion;
import com.google.cloud.kms.v1.CryptoKeyVersionTemplate;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.EncryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.kms.v1.KeyRing;
import com.google.cloud.kms.v1.KeyRingName;
import com.google.cloud.kms.v1.LocationName;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import java.util.stream.StreamSupport;
import org.apache.beam.it.common.ResourceManager;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for managing Cloud KMS resources.
 *
 * <p>The class supports one keyring and multiple crypto keys per keyring object.
 *
 * <p>The class is thread-safe.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/27438)
})
public class KMSResourceManager implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(KMSResourceManager.class);

  private static final String DEFAULT_KMS_REGION = "us-central1";

  private final String projectId;
  private final String region;
  private final KMSClientFactory clientFactory;

  private KeyRing keyRing;

  private KMSResourceManager(Builder builder) {
    this(new KMSClientFactory(builder.credentialsProvider), builder);
  }

  @VisibleForTesting
  KMSResourceManager(KMSClientFactory clientFactory, Builder builder) {
    this.clientFactory = clientFactory;
    this.projectId = builder.projectId;
    this.region = builder.region;
    this.keyRing = null;
  }

  public static Builder builder(String projectId, CredentialsProvider credentialsProvider) {
    return new Builder(projectId, credentialsProvider);
  }

  /**
   * Creates a keyring in KMS if it does not exist.
   *
   * @param keyRingId The name of the keyring to create.
   */
  private void maybeCreateKeyRing(String keyRingId) {

    try (KeyManagementServiceClient client = clientFactory.getKMSClient()) {

      LocationName locationName = LocationName.of(projectId, region);
      KeyRing keyRingToCreate = KeyRing.newBuilder().build();

      LOG.info("Checking if keyring {} already exists in KMS.", keyRingId);

      String newKeyName = KeyRingName.of(projectId, region, keyRingId).toString();
      Optional<KeyRing> existingKeyRing =
          StreamSupport.stream(client.listKeyRings(locationName).iterateAll().spliterator(), false)
              .filter(kRing -> kRing.getName().equals(newKeyName))
              .findFirst();

      // Create the keyring if it does not exist, otherwise, return the found keyring.
      if (!existingKeyRing.isPresent()) {
        LOG.info("Keyring {} does not exist. Creating the keyring in KMS.", keyRingId);
        this.keyRing = client.createKeyRing(locationName, keyRingId, keyRingToCreate);
        LOG.info("Created keyring {}.", keyRing.getName());
      } else {
        LOG.info("Keyring {} already exists. Retrieving the keyring from KMS.", keyRingId);
        this.keyRing = existingKeyRing.get();
        LOG.info("Retrieved keyring {}.", keyRing.getName());
      }
    }
  }

  /**
   * Retrieves a KMS crypto key, creating it if it does not exist. If the given keyring also does
   * not already exist, it will be created.
   *
   * @param keyRingId The name of the keyring to insert the key to.
   * @param keyName The name of the KMS crypto key to retrieve.
   * @return the created CryptoKey.
   */
  public synchronized CryptoKey getOrCreateCryptoKey(String keyRingId, String keyName) {

    // Get the keyring, creating it if it does not already exist
    if (keyRing == null) {
      maybeCreateKeyRing(keyRingId);
    }

    try (KeyManagementServiceClient client = clientFactory.getKMSClient()) {

      // Build the symmetric key to create.
      CryptoKey keyToCreate =
          CryptoKey.newBuilder()
              .setPurpose(CryptoKey.CryptoKeyPurpose.ENCRYPT_DECRYPT)
              .setVersionTemplate(
                  CryptoKeyVersionTemplate.newBuilder()
                      .setAlgorithm(
                          CryptoKeyVersion.CryptoKeyVersionAlgorithm.GOOGLE_SYMMETRIC_ENCRYPTION))
              .build();

      LOG.info("Checking if symmetric key {} already exists in KMS.", keyName);

      // Loop through the existing keys in the given keyring to see if the
      // key already exists.
      String newKeyName = CryptoKeyName.of(projectId, region, keyRingId, keyName).toString();
      Optional<CryptoKey> existingKey =
          StreamSupport.stream(
                  client.listCryptoKeys(keyRing.getName()).iterateAll().spliterator(), false)
              .filter(kRing -> kRing.getName().equals(newKeyName))
              .findFirst();

      // Create the symmetric key if it does not exist, otherwise, return the found key.
      CryptoKey cryptoKey;
      if (!existingKey.isPresent()) {
        LOG.info("Symmetric key {} does not exist. Creating the key in KMS.", keyName);
        cryptoKey = client.createCryptoKey(keyRing.getName(), keyName, keyToCreate);
        LOG.info("Created symmetric key {}.", cryptoKey.getName());
      } else {
        LOG.info("Symmetric key {} already exists. Retrieving the key from KMS.", keyName);
        cryptoKey = existingKey.get();
        LOG.info("Retrieved symmetric key {}.", cryptoKey.getName());
      }

      return cryptoKey;
    }
  }

  /**
   * Encrypt the given message using the crypto key specified. The given crypto key should exist
   * within the given keyring.
   *
   * <p>The given message should be in UTF-8 String format.
   *
   * <p>The resulting ciphertext is encoded in Base64 String format.
   *
   * @param keyRingId The name of the keyring that contains the given key.
   * @param keyId The name of the key to use for encryption.
   * @param message The message to encrypt.
   * @return The ciphertext of the encrypted message.
   */
  public synchronized String encrypt(String keyRingId, String keyId, String message) {

    CryptoKeyName keyName = CryptoKeyName.of(projectId, region, keyRingId, keyId);
    LOG.info("Encrypting given message using key {}.", keyName.toString());

    try (KeyManagementServiceClient client = clientFactory.getKMSClient()) {

      EncryptResponse response = client.encrypt(keyName, ByteString.copyFromUtf8(message));

      LOG.info("Successfully encrypted message.");
      return new String(
          Base64.getEncoder().encode(response.getCiphertext().toByteArray()),
          StandardCharsets.UTF_8);
    }
  }

  /**
   * Decrypt the given ciphertext using the crypto key specified. The given crypto key should exist
   * within the given keyring.
   *
   * <p>The given ciphertext should be in Base64 String format.
   *
   * <p>The resulting decrypted message is encoded in UTF-8 String format.
   *
   * @param keyRingId The name of the keyring that contains the given key.
   * @param keyId The name of the key to use for decryption.
   * @param ciphertext The ciphertext to decrypt.
   * @return The decrypted message.
   */
  public synchronized String decrypt(String keyRingId, String keyId, String ciphertext) {

    CryptoKeyName keyName = CryptoKeyName.of(projectId, region, keyRingId, keyId);
    LOG.info("Decrypting given ciphertext using key {}.", keyName.toString());

    try (KeyManagementServiceClient client = clientFactory.getKMSClient()) {

      DecryptResponse response =
          client.decrypt(
              keyName,
              ByteString.copyFrom(
                  Base64.getDecoder().decode(ciphertext.getBytes(StandardCharsets.UTF_8))));

      LOG.info("Successfully decrypted ciphertext.");
      return response.getPlaintext().toStringUtf8();
    }
  }

  @Override
  public void cleanupAll() {
    LOG.info("Not cleaning up KMS keys.");
  }

  /** Builder for {@link KMSResourceManager}. */
  public static final class Builder {
    private final String projectId;
    private CredentialsProvider credentialsProvider;
    private String region;

    private Builder(String projectId, CredentialsProvider credentialsProvider) {
      this.projectId = projectId;
      this.region = DEFAULT_KMS_REGION;
      this.credentialsProvider = credentialsProvider;
    }

    /**
     * Set the GCP credentials provider to connect to the project defined in the builder.
     *
     * @param credentialsProvider The GCP CredentialsProvider.
     * @return this builder with the CredentialsProvider set.
     */
    public Builder setCredentialsProvider(CredentialsProvider credentialsProvider) {
      this.credentialsProvider = credentialsProvider;
      return this;
    }

    /**
     * Set the GCP region that the keys should be configured to write to. Defaults to {@value
     * #DEFAULT_KMS_REGION}.
     *
     * @param region The region to use.
     * @return this builder with the GCS region set.
     */
    public Builder setRegion(String region) {
      this.region = region;
      return this;
    }

    public KMSResourceManager build() {
      return new KMSResourceManager(this);
    }
  }
}
