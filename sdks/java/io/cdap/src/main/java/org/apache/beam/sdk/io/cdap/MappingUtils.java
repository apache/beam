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

import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.hubspot.sink.batch.HubspotBatchSink;
import io.cdap.plugin.hubspot.sink.batch.HubspotOutputFormat;
import io.cdap.plugin.hubspot.source.batch.HubspotBatchSource;
import io.cdap.plugin.hubspot.source.batch.HubspotInputFormat;
import io.cdap.plugin.hubspot.source.batch.HubspotInputFormatProvider;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceBatchSource;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceInputFormat;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceInputFormatProvider;
import io.cdap.plugin.servicenow.source.ServiceNowInputFormat;
import io.cdap.plugin.servicenow.source.ServiceNowSource;
import io.cdap.plugin.zendesk.source.batch.ZendeskBatchSource;
import io.cdap.plugin.zendesk.source.batch.ZendeskInputFormat;
import io.cdap.plugin.zendesk.source.batch.ZendeskInputFormatProvider;

public class MappingUtils {

  public static Plugin getPluginByClass(Class<?> pluginClass) {
    checkArgument(pluginClass != null, "Plugin class can not be null!");
    if (pluginClass.equals(SalesforceBatchSource.class)) {
      return Plugin.create(
          pluginClass, SalesforceInputFormat.class, SalesforceInputFormatProvider.class);
    } else if (pluginClass.equals(HubspotBatchSource.class)) {
      return Plugin.create(pluginClass, HubspotInputFormat.class, HubspotInputFormatProvider.class);
    } else if (pluginClass.equals(ZendeskBatchSource.class)) {
      return Plugin.create(pluginClass, ZendeskInputFormat.class, ZendeskInputFormatProvider.class);
    } else if (pluginClass.equals(HubspotBatchSink.class)) {
      return Plugin.create(pluginClass, HubspotOutputFormat.class, SourceInputFormatProvider.class);
    } else if (pluginClass.equals(ServiceNowSource.class)) {
      return Plugin.create(
          pluginClass, ServiceNowInputFormat.class, SourceInputFormatProvider.class);
    }
    throw new UnsupportedOperationException(
        String.format("Given plugin class '%s' is not supported!", pluginClass.getName()));
  }
}
