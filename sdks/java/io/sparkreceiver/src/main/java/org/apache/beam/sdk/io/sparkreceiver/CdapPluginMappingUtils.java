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

import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.plugin.salesforce.plugin.source.streaming.SalesforceReceiver;
import io.cdap.plugin.salesforce.plugin.source.streaming.SalesforceStreamingSourceConfig;
import java.lang.reflect.InvocationTargetException;
import java.util.function.Consumer;
import org.apache.beam.sdk.io.sparkreceiver.hubspot.source.streaming.HubspotReceiver;
import org.apache.beam.sdk.io.sparkreceiver.hubspot.source.streaming.HubspotStreamingSourceConfig;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that performs mapping for CDAP classes and is used to obtain the appropriate
 * implementations.
 */
@SuppressWarnings("rawtypes")
public class CdapPluginMappingUtils {

  private static final Logger LOG = LoggerFactory.getLogger(CdapPluginMappingUtils.class);

  public static Receiver getSparkReceiver(PluginConfig config) {
    if (config instanceof SalesforceStreamingSourceConfig) {
      return getSparkReceiverForSalesforce((SalesforceStreamingSourceConfig) config);
    } else if (config instanceof HubspotStreamingSourceConfig) {
      return getSparkReceiverForHubspot((HubspotStreamingSourceConfig) config);
    } else {
      return null;
    }
  }

  public static SalesforceReceiver getSparkReceiverForSalesforce(
      SalesforceStreamingSourceConfig config) {
    ProxyReceiverBuilder<String, SalesforceReceiver> builder =
        new ProxyReceiverBuilder<>(SalesforceReceiver.class);

    try {
      return builder
          .withConstructorArgs(config.getAuthenticatorCredentials(), config.getPushTopicName())
          .build();
    } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
      LOG.error("Can not build proxy Spark Receiver", e);
    }
    return null;
  }

  public static HubspotReceiver getSparkReceiverForHubspot(
      HubspotStreamingSourceConfig config) {
    ProxyReceiverBuilder<String, HubspotReceiver> builder =
        new ProxyReceiverBuilder<>(HubspotReceiver.class);
    try {
      return builder.withConstructorArgs(config).build();
    } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
      LOG.error("Can not build proxy Spark Receiver", e);
    }
    return null;
  }
}
