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
package org.apache.beam.sdk.io.snowflake.credentials;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;

/** POJO for handling Key-Pair authentication against Snowflake. */
public class KeyPairSnowflakeCredentials implements SnowflakeCredentials {
  private final String username;
  private final PrivateKey privateKey;

  public KeyPairSnowflakeCredentials(
      String username, String privateKeyPath, String privateKeyPassphrase) {
    this.username = username;
    this.privateKey = getPrivateKey(privateKeyPath, privateKeyPassphrase);
  }

  public KeyPairSnowflakeCredentials(String username, PrivateKey privateKey) {
    this.username = username;
    this.privateKey = privateKey;
  }

  @Override
  public SnowflakeIO.DataSourceConfiguration createSnowflakeDataSourceConfiguration() {
    return SnowflakeIO.DataSourceConfiguration.create(this);
  }

  private PrivateKey getPrivateKey(String privateKeyPath, String privateKeyPassphrase) {
    try {
      byte[] keyBytes = Files.readAllBytes(Paths.get(privateKeyPath));

      String encrypted = new String(keyBytes, Charset.defaultCharset());
      encrypted = encrypted.replace("-----BEGIN ENCRYPTED PRIVATE KEY-----", "");
      encrypted = encrypted.replace("-----END ENCRYPTED PRIVATE KEY-----", "");
      EncryptedPrivateKeyInfo pkInfo =
          new EncryptedPrivateKeyInfo(Base64.getMimeDecoder().decode(encrypted));
      PBEKeySpec keySpec = new PBEKeySpec(privateKeyPassphrase.toCharArray());
      SecretKeyFactory pbeKeyFactory = SecretKeyFactory.getInstance(pkInfo.getAlgName());
      PKCS8EncodedKeySpec encodedKeySpec = pkInfo.getKeySpec(pbeKeyFactory.generateSecret(keySpec));

      KeyFactory keyFactory = KeyFactory.getInstance("RSA");
      return keyFactory.generatePrivate(encodedKeySpec);
    } catch (IOException
        | NoSuchAlgorithmException
        | InvalidKeySpecException
        | InvalidKeyException ex) {
      throw new RuntimeException("Can't create PrivateKey from options");
    }
  }

  public String getUsername() {
    return username;
  }

  public PrivateKey getPrivateKey() {
    return privateKey;
  }
}
