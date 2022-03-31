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
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class PulsarIOUtils {

  private static final Logger LOG = LoggerFactory.getLogger(PulsarIOUtils.class);
  public static final String SERVICE_HTTP_URL = "http://localhost:8080";
  public static final String SERVICE_URL = "pulsar://localhost:6650";

  static final SerializableFunction<String, PulsarClient> PULSAR_CLIENT_SERIALIZABLE_FUNCTION =
      new SerializableFunction<String, PulsarClient>() {
        @Override
        public PulsarClient apply(String input) {
          try {
            return PulsarClient.builder().serviceUrl(input).build();
          } catch (PulsarClientException e) {
            LOG.error(e.getMessage());
            throw new RuntimeException(e);
          }
        }
      };
}
