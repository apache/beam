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
package org.apache.beam.examples.complete.cdap.servicenow.transforms;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.plugin.servicenow.source.ServiceNowSource;
import io.cdap.plugin.servicenow.source.ServiceNowSourceConfig;
import java.util.Map;
import org.apache.beam.sdk.io.cdap.CdapIO;
import org.apache.beam.sdk.io.cdap.ConfigWrapper;
import org.apache.hadoop.io.NullWritable;

/** Different input transformations over the processed data in the pipeline. */
public class FormatInputTransform {

  /**
   * Configures Cdap ServiceNow Read transform.
   *
   * @param pluginConfigParams Cdap ServiceNow plugin config parameters
   * @return configured Read transform
   */
  public static CdapIO.Read<NullWritable, StructuredRecord> readFromCdapServiceNow(
      Map<String, Object> pluginConfigParams) {

    final PluginConfig pluginConfig =
        new ConfigWrapper<>(ServiceNowSourceConfig.class).withParams(pluginConfigParams).build();

    checkStateNotNull(pluginConfig, "Plugin config can't be null.");

    return CdapIO.<NullWritable, StructuredRecord>read()
        .withCdapPluginClass(ServiceNowSource.class)
        .withPluginConfig(pluginConfig)
        .withKeyClass(NullWritable.class)
        .withValueClass(StructuredRecord.class);
  }
}
