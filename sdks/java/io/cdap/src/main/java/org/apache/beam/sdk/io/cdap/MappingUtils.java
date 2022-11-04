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
package org.apache.beam.sdk.io.cdap;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.hubspot.sink.batch.HubspotBatchSink;
import io.cdap.plugin.hubspot.sink.batch.HubspotOutputFormat;
import io.cdap.plugin.hubspot.source.batch.HubspotBatchSource;
import io.cdap.plugin.hubspot.source.batch.HubspotInputFormat;
import io.cdap.plugin.hubspot.source.batch.HubspotInputFormatProvider;
import io.cdap.plugin.salesforce.plugin.sink.batch.SalesforceBatchSink;
import io.cdap.plugin.salesforce.plugin.sink.batch.SalesforceOutputFormat;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceBatchSource;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceInputFormat;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceInputFormatProvider;
import io.cdap.plugin.servicenow.source.ServiceNowInputFormat;
import io.cdap.plugin.servicenow.source.ServiceNowSource;
import io.cdap.plugin.zendesk.source.batch.ZendeskBatchSource;
import io.cdap.plugin.zendesk.source.batch.ZendeskInputFormat;
import io.cdap.plugin.zendesk.source.batch.ZendeskInputFormatProvider;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.sparkreceiver.ReceiverBuilder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.streaming.receiver.Receiver;

/** Util class for mapping plugins. */
public class MappingUtils {

  private static final Map<
          Class<?>, Pair<SerializableFunction<?, Long>, Class<? extends Receiver<?>>>>
      REGISTERED_PLUGINS;

  static {
    REGISTERED_PLUGINS = new HashMap<>();
  }

  /** Gets a {@link Plugin} by its class. */
  static Plugin getPluginByClass(Class<?> pluginClass) {
    checkArgument(pluginClass != null, "Plugin class can not be null!");
    if (pluginClass.equals(SalesforceBatchSource.class)) {
      return Plugin.createBatch(
          pluginClass, SalesforceInputFormat.class, SalesforceInputFormatProvider.class);
    } else if (pluginClass.equals(HubspotBatchSource.class)) {
      return Plugin.createBatch(
          pluginClass, HubspotInputFormat.class, HubspotInputFormatProvider.class);
    } else if (pluginClass.equals(ZendeskBatchSource.class)) {
      return Plugin.createBatch(
          pluginClass, ZendeskInputFormat.class, ZendeskInputFormatProvider.class);
    } else if (pluginClass.equals(HubspotBatchSink.class)) {
      return Plugin.createBatch(
          pluginClass, HubspotOutputFormat.class, SourceInputFormatProvider.class);
    } else if (pluginClass.equals(SalesforceBatchSink.class)) {
      return Plugin.createBatch(
          pluginClass, SalesforceOutputFormat.class, SalesforceInputFormatProvider.class);
    } else if (pluginClass.equals(ServiceNowSource.class)) {
      return Plugin.createBatch(
          pluginClass, ServiceNowInputFormat.class, SourceInputFormatProvider.class);
    }
    throw new UnsupportedOperationException(
        String.format("Given plugin class '%s' is not supported!", pluginClass.getName()));
  }

  /** Gets a {@link ReceiverBuilder} by CDAP {@link Plugin} class. */
  @SuppressWarnings("unchecked")
  static <V> ReceiverBuilder<V, ? extends Receiver<V>> getReceiverBuilderByPluginClass(
      Class<?> pluginClass, PluginConfig pluginConfig) {
    checkArgument(pluginClass != null, "Plugin class can not be null!");
    checkArgument(pluginConfig != null, "Plugin config can not be null!");
    if (REGISTERED_PLUGINS.containsKey(pluginClass)) {
      Class<? extends Receiver<V>> receiverClass =
          (Class<? extends Receiver<V>>) REGISTERED_PLUGINS.get(pluginClass).getRight();
      return new ReceiverBuilder<>(receiverClass).withConstructorArgs(pluginConfig);
    }
    throw new UnsupportedOperationException(
        String.format("Given plugin class '%s' is not supported!", pluginClass.getName()));
  }

  /**
   * Register new CDAP Streaming {@link Plugin} class providing corresponding {@param getOffsetFn}
   * and {@param receiverBuilder} params.
   */
  public static <V> void registerStreamingPlugin(
      Class<?> pluginClass,
      SerializableFunction<V, Long> getOffsetFn,
      Class<? extends Receiver<V>> receiverClass) {
    REGISTERED_PLUGINS.put(pluginClass, new ImmutablePair<>(getOffsetFn, receiverClass));
  }

  /**
   * Gets a {@link SerializableFunction} that defines how to get record offset for CDAP {@link
   * Plugin} class.
   */
  @SuppressWarnings("unchecked")
  static <V> SerializableFunction<V, Long> getOffsetFnForPluginClass(Class<?> pluginClass) {
    if (REGISTERED_PLUGINS.containsKey(pluginClass)) {
      return (SerializableFunction<V, Long>) REGISTERED_PLUGINS.get(pluginClass).getLeft();
    }
    throw new UnsupportedOperationException(
        String.format("Given plugin class '%s' is not supported!", pluginClass.getName()));
  }
}
