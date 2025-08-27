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
package org.apache.beam.sdk.io.kafka;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.security.auth.login.Configuration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosConsumerFactoryFn extends FileAwareFactoryFn<Consumer<byte[], byte[]>> {
  private static final String LOCAL_FACTORY_TYPE = "kerberos";
  private String krb5ConfigGcsPath = "";
  private static volatile String localKrb5ConfPath = "";

  private static final Object lock = new Object();

  // Standard Kafka property for SASL JAAS configuration
  private static final String JAAS_CONFIG_PROPERTY = "sasl.jaas.config";
  private static final String KEYTAB_SECRET_PREFIX = "keyTab=\"secretValue:";
  private static final Pattern KEYTAB_SECRET_PATTERN =
      Pattern.compile("(keyTab=\"secretValue:[^\"]+)");

  private static final Logger LOG = LoggerFactory.getLogger(KerberosConsumerFactoryFn.class);

  public KerberosConsumerFactoryFn(String krb5ConfigGcsPath) {
    super("kerberos");
    this.krb5ConfigGcsPath = krb5ConfigGcsPath;
  }

  @Override
  protected Consumer<byte[], byte[]> createObject(Map<String, Object> config) {
    // This will be called after the config map processing has occurred. Therefore, we know that the
    // property will have
    // had it's value replaced with a local directory. We don't need to worry about the GCS prefix
    // in this case.
    LOG.info("config when creating the objects: {}", config);
    try {
      String jaasConfig = (String) config.get(JAAS_CONFIG_PROPERTY);
      String localKeytabPath = "";
      if (jaasConfig != null && !jaasConfig.isEmpty()) {
        localKeytabPath =
            jaasConfig.substring(
                jaasConfig.indexOf("keyTab=\"") + 8, jaasConfig.lastIndexOf("\" principal"));
      }

      // Set the permissions on the file to be as strict as possible for security reasons. The
      // keytab contains
      // sensitive information and should be as locked down as possible.
      Path path = Paths.get(localKeytabPath);
      Set<PosixFilePermission> perms = new HashSet<>();
      perms.add(PosixFilePermission.OWNER_READ);
      Files.setPosixFilePermissions(path, perms);
    } catch (IOException e) {
      throw new RuntimeException(
          "Could not access keytab file. Make sure that the sasl.jaas.config config property "
              + "is set correctly.",
          e);
    }
    return new KafkaConsumer<>(config);
  }

  @Override
  protected void downloadAndProcessExtraFiles() throws IOException {
    synchronized (lock) {
      // we only want a new krb5 file if there is not already one present.
      if (localKrb5ConfPath.isEmpty()) {
        if (this.krb5ConfigGcsPath != null && !this.krb5ConfigGcsPath.isEmpty()) {
          String localPath =
              super.getBaseDirectory() + "/" + LOCAL_FACTORY_TYPE + "/" + "krb5.conf";
          localKrb5ConfPath = downloadGcsFile(this.krb5ConfigGcsPath, localPath);

          System.setProperty("java.security.krb5.conf", localKrb5ConfPath);
          Configuration.getConfiguration().refresh();
          LOG.info(
              "Successfully set and refreshed java.security.krb5.conf to {}", localKrb5ConfPath);
        }
      }
    }
  }

  @Override
  protected String processSecret(String originalValue, String secretId, byte[] secretValue)
      throws RuntimeException {
    Matcher matcher = KEYTAB_SECRET_PATTERN.matcher(originalValue);
    String localFileString = "";
    while (matcher.find()) {
      String currentSecretId = matcher.group(1);
      if (currentSecretId == null || currentSecretId.isEmpty()) {
        throw new RuntimeException(
            "Error matching values. Secret was discovered but its value is null");
      }
      currentSecretId = currentSecretId.substring(KEYTAB_SECRET_PREFIX.length());
      LOG.info("currentSecretId: {} and secretId: {}", currentSecretId, secretId);
      if (!currentSecretId.equals(secretId)) {
        // A sasl.jaas.config can contain multiple keytabs in one string. Therefore, we must assume
        // that there can
        // also be multiple keytab secrets in the same string. If the currently matched secret does
        // not equal
        // the secret that we are processing (passed in via secretId) then we do not want to create
        // a keytab file and
        // overwrite it.
        continue;
      }
      String filename = "kafka-client-" + UUID.randomUUID().toString() + ".keytab";

      localFileString = super.getBaseDirectory() + "/" + LOCAL_FACTORY_TYPE + "/" + filename;
      Path localFilePath = Paths.get(localFileString);
      Path parentDir = localFilePath.getParent();
      try {
        if (parentDir != null) {
          Files.createDirectories(parentDir);
        }
        Files.write(localFilePath, secretValue);
        if (!new File(localFileString).canRead()) {
          LOG.info("The file is not readable");
        }
        LOG.info("Successfully wrote file to path: {}", localFilePath);
      } catch (IOException e) {
        throw new RuntimeException("Unable to create the keytab file for the provided secret.");
      }
    }
    // if no localFile was created, then we can assume that the secret is meant to be kept as a
    // value.
    LOG.info("LocalFilestring: {}", localFileString);
    return localFileString.isEmpty()
        ? new String(secretValue, StandardCharsets.UTF_8)
        : localFileString;
  }
}
