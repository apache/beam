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
package org.apache.beam.examples.complete.cdap.hubspot.utils;

import io.cdap.plugin.common.Constants;
import io.cdap.plugin.hubspot.common.BaseHubspotConfig;
import io.cdap.plugin.hubspot.source.streaming.HubspotStreamingSourceConfig;
import io.cdap.plugin.hubspot.source.streaming.PullFrequency;
import java.util.Map;
import org.apache.beam.examples.complete.cdap.hubspot.options.CdapHubspotOptions;
import org.apache.beam.examples.complete.cdap.hubspot.options.CdapHubspotStreamingSourceOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * Class for converting CDAP {@link org.apache.beam.sdk.options.PipelineOptions} to map for {@link
 * org.apache.beam.sdk.io.cdap.ConfigWrapper}.
 */
public class PluginConfigOptionsConverter {

  /** Returns map of parameters for Cdap Hubspot plugins. */
  public static Map<String, Object> hubspotOptionsToParamsMap(CdapHubspotOptions options) {
    String apiServerUrl = options.getApiServerUrl();
    ImmutableMap.Builder<String, Object> builder =
        ImmutableMap.<String, Object>builder()
            .put(
                BaseHubspotConfig.API_SERVER_URL,
                apiServerUrl != null ? apiServerUrl : BaseHubspotConfig.DEFAULT_API_SERVER_URL)
            .put(BaseHubspotConfig.AUTHORIZATION_TOKEN, options.getAuthToken())
            .put(BaseHubspotConfig.OBJECT_TYPE, options.getObjectType())
            .put(Constants.Reference.REFERENCE_NAME, options.getReferenceName());
    if (options instanceof CdapHubspotStreamingSourceOptions) {
      // Hubspot PullFrequency value will be ignored as pull frequency is implemented differently in
      // CdapIO, but it still needs to be passed for the plugin to work correctly.
      builder.put(HubspotStreamingSourceConfig.PULL_FREQUENCY, PullFrequency.MINUTES_15.getName());
    }
    return builder.build();
  }
}
