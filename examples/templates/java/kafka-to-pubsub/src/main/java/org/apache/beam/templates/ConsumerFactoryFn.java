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
package org.apache.beam.templates;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class to create Kafka Consumer with configured SSL. */
public class ConsumerFactoryFn
    implements SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> {
  private final Map<String, String> sslConfig;

  /* Logger for class.*/
  private static final Logger LOG = LoggerFactory.getLogger(ConsumerFactoryFn.class);

  public ConsumerFactoryFn(Map<String, String> sslConfig) {
    this.sslConfig = sslConfig;
  }

  @Override
  public Consumer<byte[], byte[]> apply(Map<String, Object> config) {
    try {
      checkFileExists(sslConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
      checkFileExists(sslConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
    } catch (IOException | NullPointerException e) {
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
    File f = new File(filePath);
    if (f.exists()) {
      LOG.debug("{} exists", f.getAbsolutePath());
    } else {
      LOG.error("{} does not exist", f.getAbsolutePath());
      throw new IOException();
    }
  }
}
