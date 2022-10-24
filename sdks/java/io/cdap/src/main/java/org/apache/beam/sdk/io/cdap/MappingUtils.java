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

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.gson.Gson;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.hubspot.sink.batch.HubspotBatchSink;
import io.cdap.plugin.hubspot.sink.batch.HubspotOutputFormat;
import io.cdap.plugin.hubspot.source.batch.HubspotBatchSource;
import io.cdap.plugin.hubspot.source.batch.HubspotInputFormat;
import io.cdap.plugin.hubspot.source.batch.HubspotInputFormatProvider;
import io.cdap.plugin.hubspot.source.streaming.HubspotReceiver;
import io.cdap.plugin.hubspot.source.streaming.HubspotStreamingSource;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.reflect.TypeToken;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Util class for mapping plugins. */
public class MappingUtils {

  private static final Logger LOG = LoggerFactory.getLogger(MappingUtils.class);
  private static final String HUBSPOT_ID_FIELD = "vid";
  private static final Gson GSON = new Gson();

  private static final Map<
          Class<?>, Pair<SerializableFunction<?, Long>, ReceiverBuilder<?, ? extends Receiver<?>>>>
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
    } else if (pluginClass.equals(ServiceNowSource.class)) {
      return Plugin.createBatch(
          pluginClass, ServiceNowInputFormat.class, SourceInputFormatProvider.class);
    } else if (pluginClass.equals(HubspotStreamingSource.class)) {
      return Plugin.createStreaming(pluginClass);
    }
    throw new UnsupportedOperationException(
        String.format("Given plugin class '%s' is not supported!", pluginClass.getName()));
  }

  /** Gets a {@link ReceiverBuilder} by CDAP {@link Plugin} class. */
  @SuppressWarnings("unchecked")
  static <V> ReceiverBuilder<V, ? extends Receiver<V>> getReceiverBuilderByPluginClass(
      Class<?> pluginClass, PluginConfig pluginConfig, Class<V> valueClass) {
    checkArgument(pluginClass != null, "Plugin class can not be null!");
    checkArgument(pluginConfig != null, "Plugin config can not be null!");
    checkArgument(valueClass != null, "Value class can not be null!");
    if (pluginClass.equals(HubspotStreamingSource.class) && String.class.equals(valueClass)) {
      ReceiverBuilder<?, ? extends Receiver<?>> receiverBuilder =
          new ReceiverBuilder<>(HubspotReceiver.class).withConstructorArgs(pluginConfig);
      return (ReceiverBuilder<V, ? extends Receiver<V>>) receiverBuilder;
    }
    if (REGISTERED_PLUGINS.containsKey(pluginClass)) {
      return (ReceiverBuilder<V, ? extends Receiver<V>>)
          REGISTERED_PLUGINS.get(pluginClass).getRight();
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
      ReceiverBuilder<V, ? extends Receiver<V>> receiverBuilder) {
    REGISTERED_PLUGINS.put(pluginClass, new ImmutablePair<>(getOffsetFn, receiverBuilder));
  }

  private static SerializableFunction<String, Long> getOffsetFnForHubspot() {
    return input -> {
      if (input != null) {
        try {
          HashMap<String, Object> json =
              GSON.fromJson(input, new TypeToken<HashMap<String, Object>>() {}.getType());
          checkArgumentNotNull(json, "Can not get JSON from Hubspot input string");
          Object id = json.get(HUBSPOT_ID_FIELD);
          checkArgumentNotNull(id, "Can not get ID from Hubspot input string");
          return ((Integer) id).longValue();
        } catch (Exception e) {
          LOG.error("Can not get offset from json", e);
        }
      }
      return 0L;
    };
  }

  /**
   * Gets a {@link SerializableFunction} that defines how to get record offset for CDAP {@link
   * Plugin} class.
   */
  @SuppressWarnings("unchecked")
  static <V> SerializableFunction<V, Long> getOffsetFnForPluginClass(
      Class<?> pluginClass, Class<V> valueClass) {
    if (pluginClass.equals(HubspotStreamingSource.class) && String.class.equals(valueClass)) {
      return (SerializableFunction<V, Long>) getOffsetFnForHubspot();
    }
    if (REGISTERED_PLUGINS.containsKey(pluginClass)) {
      return (SerializableFunction<V, Long>) REGISTERED_PLUGINS.get(pluginClass).getLeft();
    }
    throw new UnsupportedOperationException(
        String.format("Given plugin class '%s' is not supported!", pluginClass.getName()));
  }
}
