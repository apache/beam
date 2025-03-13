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
package org.apache.beam.examples.complete.cdap.zendesk.utils;

import io.cdap.plugin.common.Constants;
import io.cdap.plugin.zendesk.source.batch.ZendeskBatchSourceConfig;
import io.cdap.plugin.zendesk.source.common.config.BaseZendeskSourceConfig;
import java.util.Map;
import org.apache.beam.examples.complete.cdap.zendesk.options.CdapZendeskOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * Class for converting CDAP {@link org.apache.beam.sdk.options.PipelineOptions} to map for {@link
 * org.apache.beam.sdk.io.cdap.ConfigWrapper}.
 */
public class PluginConfigOptionsConverter {

  /** Returns map of parameters for Cdap Zendesk plugin. */
  public static Map<String, Object> zendeskOptionsToParamsMap(CdapZendeskOptions zendeskOptions) {
    return ImmutableMap.<String, Object>builder()
        .put(Constants.Reference.REFERENCE_NAME, zendeskOptions.getReferenceName())
        .put(BaseZendeskSourceConfig.PROPERTY_ADMIN_EMAIL, zendeskOptions.getAdminEmail())
        .put(BaseZendeskSourceConfig.PROPERTY_API_TOKEN, zendeskOptions.getApiToken())
        .put(ZendeskBatchSourceConfig.PROPERTY_URL, zendeskOptions.getZendeskBaseUrl())
        .put(ZendeskBatchSourceConfig.PROPERTY_SUBDOMAINS, zendeskOptions.getSubdomains())
        .put(ZendeskBatchSourceConfig.PROPERTY_MAX_RETRY_COUNT, zendeskOptions.getMaxRetryCount())
        .put(ZendeskBatchSourceConfig.PROPERTY_MAX_RETRY_WAIT, zendeskOptions.getMaxRetryWait())
        .put(
            ZendeskBatchSourceConfig.PROPERTY_MAX_RETRY_JITTER_WAIT,
            zendeskOptions.getMaxRetryJitterWait())
        .put(ZendeskBatchSourceConfig.PROPERTY_CONNECT_TIMEOUT, zendeskOptions.getConnectTimeout())
        .put(ZendeskBatchSourceConfig.PROPERTY_READ_TIMEOUT, zendeskOptions.getReadTimeout())
        .put(BaseZendeskSourceConfig.PROPERTY_OBJECTS_TO_PULL, zendeskOptions.getObjectsToPull())
        .build();
  }
}
