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
package org.apache.beam.examples.complete.kafkatopubsub.options;

import org.apache.beam.examples.complete.kafkatopubsub.transforms.FormatTransform;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface KafkaToPubsubOptions extends PipelineOptions {
  @Description(
      "Comma Separated list of Kafka Bootstrap Servers (e.g: server1:[port],server2:[port]).")
  @Validation.Required
  String getBootstrapServers();

  void setBootstrapServers(String value);

  @Description(
      "Comma Separated list of Kafka topic(s) to read the input from (e.g: topic1,topic2).")
  @Validation.Required
  String getInputTopics();

  void setInputTopics(String value);

  @Description(
      "The Cloud Pub/Sub topic to publish to. "
          + "The name should be in the format of "
          + "projects/<project-id>/topics/<topic-name>.")
  @Validation.Required
  String getOutputTopic();

  void setOutputTopic(String outputTopic);

  @Description(
      "Format which will be writen to output Pub/Sub topic. Supported formats: AVRO, PUBSUB")
  @Validation.Required
  FormatTransform.FORMAT getOutputFormat();

  void setOutputFormat(FormatTransform.FORMAT outputFormat);

  @Description("URL to credentials in Vault")
  String getSecretStoreUrl();

  void setSecretStoreUrl(String secretStoreUrl);

  @Description("Vault token")
  String getVaultToken();

  void setVaultToken(String vaultToken);

  @Description("The path to the trust store file")
  String getTruststorePath();

  void setTruststorePath(String truststorePath);

  @Description("The path to the key store file")
  String getKeystorePath();

  void setKeystorePath(String keystorePath);

  @Description("The password for the trust store password")
  String getTruststorePassword();

  void setTruststorePassword(String truststorePassword);

  @Description("The store password for the key store password")
  String getKeystorePassword();

  void setKeystorePassword(String keystorePassword);

  @Description("The password of the private key in the key store file")
  String getKeyPassword();

  void setKeyPassword(String keyPassword);

  @Description(
      "Additional kafka consumer configs to be applied to Kafka Consumer (e.g. key1=value1;key2=value2).")
  @Nullable
  String getKafkaConsumerConfig();

  void setKafkaConsumerConfig(String kafkaConfig);
}
