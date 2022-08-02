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
package org.apache.beam.examples.complete.kafkatopubsub.kafka.consumer;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class to create Kafka Consumer with configured SSL. */
public class SslConsumerFactoryFn
    implements SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> {
  private final Map<String, String> sslConfig;
  private static final String TRUSTSTORE_LOCAL_PATH = "/tmp/kafka.truststore.jks";
  private static final String KEYSTORE_LOCAL_PATH = "/tmp/kafka.keystore.jks";

  /* Logger for class.*/
  private static final Logger LOG = LoggerFactory.getLogger(SslConsumerFactoryFn.class);

  public SslConsumerFactoryFn(Map<String, String> sslConfig) {
    this.sslConfig = sslConfig;
  }

  @SuppressWarnings("nullness")
  @Override
  public Consumer<byte[], byte[]> apply(Map<String, Object> config) {
    String truststoreLocation = sslConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
    String keystoreLocation = sslConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
    if (truststoreLocation == null || keystoreLocation == null) {
      LOG.warn("Not enough information to configure SSL");
      return new KafkaConsumer<>(config);
    }

    try {
      if (truststoreLocation.startsWith("gs://")) {
        getGcsFileAsLocal(truststoreLocation, TRUSTSTORE_LOCAL_PATH);
        sslConfig.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, TRUSTSTORE_LOCAL_PATH);
      } else {
        checkFileExists(truststoreLocation);
      }

      if (keystoreLocation.startsWith("gs://")) {
        getGcsFileAsLocal(keystoreLocation, KEYSTORE_LOCAL_PATH);
        sslConfig.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, KEYSTORE_LOCAL_PATH);
      } else {
        checkFileExists(keystoreLocation);
      }
    } catch (IOException e) {
      LOG.error("Failed to retrieve data for SSL", e);
      return new KafkaConsumer<>(config);
    }

    config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name());
    config.put(
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
        sslConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
    config.put(
        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
        sslConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
    config.put(
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
        sslConfig.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
    config.put(
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
        sslConfig.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
    config.put(
        SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslConfig.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG));

    return new KafkaConsumer<>(config);
  }

  private void checkFileExists(String filePath) throws IOException {
    LOG.info(
        "Trying to get file: {} locally. Local files don't support when in using distribute runner",
        filePath);
    File f = new File(filePath);
    if (f.exists()) {
      LOG.debug("{} exists", f.getAbsolutePath());
    } else {
      LOG.error("{} does not exist", f.getAbsolutePath());
      throw new IOException();
    }
  }

  /**
   * Reads a file from GCS and writes it locally.
   *
   * @param gcsFilePath path to file in GCS in format "gs://your-bucket/path/to/file"
   * @param outputFilePath path where to save file locally
   * @throws IOException thrown if not able to read or write file
   */
  public static void getGcsFileAsLocal(String gcsFilePath, String outputFilePath)
      throws IOException {
    LOG.info("Reading contents from GCS file: {}", gcsFilePath);
    Set<StandardOpenOption> options = new HashSet<>(2);
    options.add(StandardOpenOption.CREATE);
    options.add(StandardOpenOption.APPEND);
    // Copy the GCS file into a local file and will throw an I/O exception in case file not found.
    try (ReadableByteChannel readerChannel =
        FileSystems.open(FileSystems.matchSingleFileSpec(gcsFilePath).resourceId())) {
      try (FileChannel writeChannel = FileChannel.open(Paths.get(outputFilePath), options)) {
        writeChannel.transferFrom(readerChannel, 0, Long.MAX_VALUE);
      }
    }
  }
}
