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
package org.apache.beam.sdk.io.sparkreceiver;

import io.cdap.plugin.salesforce.plugin.source.streaming.SalesforceReceiver;
import io.cdap.plugin.salesforce.plugin.source.streaming.SalesforceStreamingSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import java.util.function.Consumer;

public class CdapPluginMappingUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CdapPluginMappingUtils.class);

  public static Optional<SalesforceReceiver> getProxyReceiverForSalesforce(
      SalesforceStreamingSourceConfig config, Consumer<Object[]> consumer) {
    ProxyReceiverBuilder<String, SalesforceReceiver> builder =
        new ProxyReceiverBuilder<>(SalesforceReceiver.class);

    try {
      return Optional.of(
          builder
              .withConstructorArgs(config.getAuthenticatorCredentials(), config.getPushTopicName())
              .withCustomStoreConsumer(consumer)
              .build());
    } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
      LOG.error("Can not build proxy Spark Receiver", e);
    }
    return Optional.empty();
  }
}
