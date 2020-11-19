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
package org.apache.beam.templates.kafka.consumer;

import static org.apache.beam.templates.KafkaPubsubConstants.BUCKET;
import static org.apache.beam.templates.KafkaPubsubConstants.KAFKA_CREDENTIALS;
import static org.apache.beam.templates.KafkaPubsubConstants.PASSWORD;
import static org.apache.beam.templates.KafkaPubsubConstants.SSL_CREDENTIALS;
import static org.apache.beam.templates.KafkaPubsubConstants.USERNAME;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.JsonObject;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.JsonParser;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.scram.ScramMechanism;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities for construction of Kafka Consumer. */
public class Utils {

  /* Logger for class.*/
  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  /**
   * Retrieves all credentials from HashiCorp Vault secret storage.
   *
   * @param secretStoreUrl url to the secret storage that contains a credentials for Kafka
   * @param token Vault token to access the secret storage
   * @return credentials for Kafka consumer config
   */
  public static Map<String, Map<String, String>> getKafkaCredentialsFromVault(
      String secretStoreUrl, String token) {
    Map<String, Map<String, String>> credentialMap = new HashMap<>();

    JsonObject credentials = null;
    try {
      HttpClient client = HttpClientBuilder.create().build();
      HttpGet request = new HttpGet(secretStoreUrl);
      request.addHeader("X-Vault-Token", token);
      HttpResponse response = client.execute(request);
      String json = EntityUtils.toString(response.getEntity(), "UTF-8");

      /*
       Vault's response JSON has a specific schema, where the actual data is placed under
       {data: {data: <actual data>}}.
       Example:
         {
           "request_id": "6a0bb14b-ef24-256c-3edf-cfd52ad1d60d",
           "lease_id": "",
           "renewable": false,
           "lease_duration": 0,
           "data": {
             "data": {
               "bucket": "kafka_to_pubsub_test",
               "key_password": "secret",
               "keystore_password": "secret",
               "keystore_path": "ssl_cert/kafka.keystore.jks",
               "password": "admin-secret",
               "truststore_password": "secret",
               "truststore_path": "ssl_cert/kafka.truststore.jks",
               "username": "admin"
             },
             "metadata": {
               "created_time": "2020-10-20T11:43:11.109186969Z",
               "deletion_time": "",
               "destroyed": false,
               "version": 8
             }
           },
           "wrap_info": null,
           "warnings": null,
           "auth": null
         }
      */
      // Parse security properties from the response JSON
      credentials =
          JsonParser.parseString(json)
              .getAsJsonObject()
              .get("data")
              .getAsJsonObject()
              .getAsJsonObject("data");
    } catch (IOException e) {
      LOG.error("Failed to retrieve credentials from Vault.", e);
    }

    if (credentials != null) {
      // Username and password for Kafka authorization
      credentialMap.put(KAFKA_CREDENTIALS, new HashMap<>());

      if (credentials.has(USERNAME) && credentials.has(PASSWORD)) {
        credentialMap.get(KAFKA_CREDENTIALS).put(USERNAME, credentials.get(USERNAME).getAsString());
        credentialMap.get(KAFKA_CREDENTIALS).put(PASSWORD, credentials.get(PASSWORD).getAsString());
      } else {
        LOG.warn(
            "There are no username and/or password for Kafka in Vault."
                + "Trying to initiate an unauthorized connection.");
      }

      // SSL truststore, keystore, and password
      try {
        Map<String, String> sslCredentials = new HashMap<>();
        String[] configNames = {
          BUCKET,
          SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
          SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
          SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
          SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
          SslConfigs.SSL_KEY_PASSWORD_CONFIG
        };
        for (String configName : configNames) {
          sslCredentials.put(configName, credentials.get(configName).getAsString());
        }
        credentialMap.put(SSL_CREDENTIALS, sslCredentials);
      } catch (NullPointerException e) {
        LOG.warn(
            "There is no enough information to configure SSL."
                + "Trying to initiate an unsecure connection.",
            e);
      }
    }

    return credentialMap;
  }

  /**
   * Configures Kafka consumer for authorized connection.
   *
   * @param props username and password for Kafka
   * @return configuration set of parameters for Kafka
   */
  public static Map<String, Object> configureKafka(Map<String, String> props) {
    // Create the configuration for Kafka
    Map<String, Object> config = new HashMap<>();
    if (props != null && props.containsKey(USERNAME) && props.containsKey(PASSWORD)) {
      config.put(SaslConfigs.SASL_MECHANISM, ScramMechanism.SCRAM_SHA_512.mechanismName());
      config.put(
          SaslConfigs.SASL_JAAS_CONFIG,
          String.format(
              "org.apache.kafka.common.security.scram.ScramLoginModule required "
                  + "username=\"%s\" password=\"%s\";",
              props.get(USERNAME), props.get(PASSWORD)));
    }
    return config;
  }
}
