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
package org.apache.beam.sdk.io.pulsar;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class PulsarIOUtils {

  private static final Logger LOG = LoggerFactory.getLogger(PulsarIOUtils.class);
  static final String LOCAL_SERVICE_URL = "pulsar://localhost:6650";
  static final long DEFAULT_CONSUMER_POLLING_TIMEOUT = 2L;

  static final SerializableFunction<String, PulsarClient> PULSAR_CLIENT_SERIALIZABLE_FUNCTION =
      input -> {
        try {
          return PulsarClient.builder().serviceUrl(input).build();
        } catch (PulsarClientException e) {
          throw new RuntimeException(e);
        }
      };

  static final SerializableFunction<String, PulsarAdmin> PULSAR_ADMIN_SERIALIZABLE_FUNCTION =
      input -> {
        try {
          return PulsarAdmin.builder()
              .serviceHttpUrl(input)
              .allowTlsInsecureConnection(false)
              .build();
        } catch (PulsarClientException e) {
          throw new RuntimeException(e);
        }
      };
}
