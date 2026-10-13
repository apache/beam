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
package org.apache.beam.sdk.io.iceberg;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Map;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.util.ByteBuffers;

/**
 * An in-memory {@link KeyManagementClient} for tests, so that table encryption can be exercised
 * without reaching a real KMS service.
 *
 * <p>Keys are wrapped with AES-GCM under a hard-coded master key, mirroring what Iceberg's own
 * {@code UnitestKMS} test fixture does.
 */
public class TestKms implements KeyManagementClient {
  /** Table key ID to use as the {@code encryption.key-id} table property. */
  public static final String MASTER_KEY_ID = "beam-test-master-key";

  private static final Map<String, byte[]> MASTER_KEYS =
      ImmutableMap.of(MASTER_KEY_ID, "0123456789012345".getBytes(StandardCharsets.UTF_8));

  private static final String TRANSFORMATION = "AES/GCM/NoPadding";
  private static final int NONCE_LENGTH = 12;
  private static final int TAG_LENGTH_BITS = 128;

  @Override
  public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId) {
    byte[] nonce = new byte[NONCE_LENGTH];
    new SecureRandom().nextBytes(nonce);
    byte[] ciphertext =
        crypt(Cipher.ENCRYPT_MODE, wrappingKeyId, nonce, ByteBuffers.toByteArray(key));

    // prepend the nonce so unwrapKey can recover it
    return ByteBuffer.allocate(nonce.length + ciphertext.length).put(nonce).put(ciphertext).flip();
  }

  @Override
  public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId) {
    byte[] wrapped = ByteBuffers.toByteArray(wrappedKey);
    byte[] nonce = new byte[NONCE_LENGTH];
    System.arraycopy(wrapped, 0, nonce, 0, NONCE_LENGTH);
    byte[] ciphertext = new byte[wrapped.length - NONCE_LENGTH];
    System.arraycopy(wrapped, NONCE_LENGTH, ciphertext, 0, ciphertext.length);

    return ByteBuffer.wrap(crypt(Cipher.DECRYPT_MODE, wrappingKeyId, nonce, ciphertext));
  }

  @Override
  public void initialize(Map<String, String> properties) {}

  private static byte[] crypt(int mode, String wrappingKeyId, byte[] nonce, byte[] input) {
    byte[] masterKey = MASTER_KEYS.get(wrappingKeyId);
    if (masterKey == null) {
      throw new IllegalArgumentException("Unknown master key ID: " + wrappingKeyId);
    }
    try {
      Cipher cipher = Cipher.getInstance(TRANSFORMATION);
      cipher.init(
          mode, new SecretKeySpec(masterKey, "AES"), new GCMParameterSpec(TAG_LENGTH_BITS, nonce));
      return cipher.doFinal(input);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to " + (mode == Cipher.ENCRYPT_MODE ? "wrap" : "unwrap"), e);
    }
  }
}
