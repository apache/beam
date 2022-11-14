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
package org.apache.beam.examples.complete.cdap.transforms;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.gson.JsonElement;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.hubspot.common.SourceHubspotConfig;
import io.cdap.plugin.hubspot.source.batch.HubspotBatchSource;
import io.cdap.plugin.hubspot.source.streaming.HubspotReceiver;
import io.cdap.plugin.hubspot.source.streaming.HubspotStreamingSource;
import io.cdap.plugin.hubspot.source.streaming.HubspotStreamingSourceConfig;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceBatchSource;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceSourceConfig;
import io.cdap.plugin.servicenow.source.ServiceNowSource;
import io.cdap.plugin.servicenow.source.ServiceNowSourceConfig;
import io.cdap.plugin.zendesk.source.batch.ZendeskBatchSource;
import io.cdap.plugin.zendesk.source.batch.ZendeskBatchSourceConfig;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.beam.examples.complete.cdap.utils.GetOffsetUtils;
import org.apache.beam.sdk.io.cdap.CdapIO;
import org.apache.beam.sdk.io.cdap.ConfigWrapper;
import org.apache.beam.sdk.io.cdap.Plugin;
import org.apache.hadoop.io.NullWritable;

/** Different input transformations over the processed data in the pipeline. */
public class FormatInputTransform {

  /**
   * Configures Cdap Zendesk Read transform.
   *
   * @param pluginConfigParams Cdap Zendesk plugin config parameters
   * @return configured Read transform
   */
  public static CdapIO.Read<NullWritable, StructuredRecord> readFromCdapZendesk(
      Map<String, Object> pluginConfigParams) {

    final ZendeskBatchSourceConfig pluginConfig =
        new ConfigWrapper<>(ZendeskBatchSourceConfig.class).withParams(pluginConfigParams).build();

    checkStateNotNull(pluginConfig, "Plugin config can't be null.");

    return CdapIO.<NullWritable, StructuredRecord>read()
        .withCdapPluginClass(ZendeskBatchSource.class)
        .withPluginConfig(pluginConfig)
        .withKeyClass(NullWritable.class)
        .withValueClass(StructuredRecord.class);
  }

  /**
   * Configures Cdap Hubspot Read transform.
   *
   * @param pluginConfigParams Cdap Hubspot plugin config parameters
   * @return configured Read transform
   */
  public static CdapIO.Read<NullWritable, JsonElement> readFromCdapHubspot(
      Map<String, Object> pluginConfigParams) {

    final SourceHubspotConfig pluginConfig =
        new ConfigWrapper<>(SourceHubspotConfig.class).withParams(pluginConfigParams).build();

    checkStateNotNull(pluginConfig, "Plugin config can't be null.");

    return CdapIO.<NullWritable, JsonElement>read()
        .withCdapPluginClass(HubspotBatchSource.class)
        .withPluginConfig(pluginConfig)
        .withKeyClass(NullWritable.class)
        .withValueClass(JsonElement.class);
  }

  /**
   * Configures Cdap Hubspot Streaming Read transform.
   *
   * @param pluginConfigParams Cdap Hubspot plugin config parameters
   * @param pullFrequencySec Delay in seconds between polling for new records updates
   * @param startOffset Inclusive start offset from which the reading should be started
   * @return configured Read transform
   */
  public static CdapIO.Read<NullWritable, String> readFromCdapHubspotStreaming(
      Map<String, Object> pluginConfigParams, Long pullFrequencySec, Long startOffset) {

    final HubspotStreamingSourceConfig pluginConfig =
        new ConfigWrapper<>(HubspotStreamingSourceConfig.class)
            .withParams(pluginConfigParams)
            .build();
    checkStateNotNull(pluginConfig, "Plugin config can't be null.");

    CdapIO.Read<NullWritable, String> read =
        CdapIO.<NullWritable, String>read()
            .withCdapPlugin(
                Plugin.createStreaming(
                    HubspotStreamingSource.class,
                    GetOffsetUtils.getOffsetFnForHubspot(),
                    HubspotReceiver.class))
            .withPluginConfig(pluginConfig)
            .withKeyClass(NullWritable.class)
            .withValueClass(String.class);
    if (pullFrequencySec != null) {
      read = read.withPullFrequencySec(pullFrequencySec);
    }
    if (startOffset != null) {
      read = read.withStartOffset(startOffset);
    }
    return read;
  }

  /**
   * Configures Cdap ServiceNow Read transform.
   *
   * @param pluginConfigParams Cdap ServiceNow plugin config parameters
   * @return configured Read transform
   */
  public static CdapIO.Read<NullWritable, StructuredRecord> readFromCdapServiceNow(
      Map<String, Object> pluginConfigParams) {

    final ServiceNowSourceConfig pluginConfig =
        new ConfigWrapper<>(ServiceNowSourceConfig.class).withParams(pluginConfigParams).build();

    checkStateNotNull(pluginConfig, "Plugin config can't be null.");

    return CdapIO.<NullWritable, StructuredRecord>read()
        .withCdapPluginClass(ServiceNowSource.class)
        .withPluginConfig(pluginConfig)
        .withKeyClass(NullWritable.class)
        .withValueClass(StructuredRecord.class);
  }

  /**
   * Configures Cdap Salesforce Read transform.
   *
   * @param pluginConfigParams Cdap Salesforce plugin config parameters
   * @return configured Read transform
   */
  @SuppressWarnings("rawtypes")
  public static CdapIO.Read<Schema, LinkedHashMap> readFromCdapSalesforce(
      Map<String, Object> pluginConfigParams) {

    final SalesforceSourceConfig pluginConfig =
        new ConfigWrapper<>(SalesforceSourceConfig.class).withParams(pluginConfigParams).build();

    checkStateNotNull(pluginConfig, "Plugin config can't be null.");

    return CdapIO.<Schema, LinkedHashMap>read()
        .withCdapPluginClass(SalesforceBatchSource.class)
        .withPluginConfig(pluginConfig)
        .withKeyClass(Schema.class)
        .withValueClass(LinkedHashMap.class);
  }
}
