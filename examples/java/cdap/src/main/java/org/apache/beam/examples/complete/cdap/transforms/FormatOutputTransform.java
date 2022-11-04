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

import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.plugin.hubspot.sink.batch.HubspotBatchSink;
import io.cdap.plugin.hubspot.sink.batch.SinkHubspotConfig;
import io.cdap.plugin.salesforce.plugin.sink.batch.CSVRecord;
import io.cdap.plugin.salesforce.plugin.sink.batch.SalesforceBatchSink;
import io.cdap.plugin.salesforce.plugin.sink.batch.SalesforceSinkConfig;
import java.util.Map;
import org.apache.beam.sdk.io.cdap.CdapIO;
import org.apache.beam.sdk.io.cdap.ConfigWrapper;
import org.apache.hadoop.io.NullWritable;

/** Different output transformations over the processed data in the pipeline. */
public class FormatOutputTransform {

  /**
   * Configures Cdap Hubspot Write transform.
   *
   * @param pluginConfigParams Cdap Hubspot plugin config parameters
   * @return configured Write transform to Cdap Hubspot
   */
  public static CdapIO.Write<NullWritable, String> writeToCdapHubspot(
      Map<String, Object> pluginConfigParams, String locksDirPath) {
    final PluginConfig pluginConfig =
        new ConfigWrapper<>(SinkHubspotConfig.class).withParams(pluginConfigParams).build();

    checkStateNotNull(pluginConfig, "Plugin config can't be null.");
    Class<? extends BatchSink<?, ?, ?>> pluginClass = HubspotBatchSink.class;

    return CdapIO.<NullWritable, String>write()
        .withCdapPluginClass(pluginClass)
        .withPluginConfig(pluginConfig)
        .withKeyClass(NullWritable.class)
        .withValueClass(String.class)
        .withLocksDirPath(locksDirPath);
  }

  /**
   * Configures Cdap Salesforce Write transform.
   *
   * @param pluginConfigParams Cdap Salesforce plugin config parameters
   * @return configured Write transform to Cdap Salesforce
   */
  public static CdapIO.Write<NullWritable, CSVRecord> writeToCdapSalesforce(
      Map<String, Object> pluginConfigParams, String locksDirPath) {
    final PluginConfig pluginConfig =
        new ConfigWrapper<>(SalesforceSinkConfig.class).withParams(pluginConfigParams).build();

    checkStateNotNull(pluginConfig, "Plugin config can't be null.");
    Class<? extends BatchSink<?, ?, ?>> pluginClass = SalesforceBatchSink.class;

    return CdapIO.<NullWritable, CSVRecord>write()
        .withCdapPluginClass(pluginClass)
        .withPluginConfig(pluginConfig)
        .withKeyClass(NullWritable.class)
        .withValueClass(CSVRecord.class)
        .withLocksDirPath(locksDirPath);
  }
}
