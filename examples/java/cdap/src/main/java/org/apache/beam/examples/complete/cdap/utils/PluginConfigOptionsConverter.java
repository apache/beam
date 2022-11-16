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
package org.apache.beam.examples.complete.cdap.utils;

import io.cdap.plugin.common.Constants;
import io.cdap.plugin.hubspot.common.BaseHubspotConfig;
import io.cdap.plugin.hubspot.source.streaming.HubspotStreamingSourceConfig;
import io.cdap.plugin.hubspot.source.streaming.PullFrequency;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.plugin.sink.batch.ErrorHandling;
import io.cdap.plugin.salesforce.plugin.sink.batch.SalesforceSinkConfig;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import io.cdap.plugin.servicenow.source.util.ServiceNowConstants;
import io.cdap.plugin.zendesk.source.batch.ZendeskBatchSourceConfig;
import io.cdap.plugin.zendesk.source.common.config.BaseZendeskSourceConfig;
import java.util.Map;
import org.apache.beam.examples.complete.cdap.options.*;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * Class for converting CDAP {@link org.apache.beam.sdk.options.PipelineOptions} to map for {@link
 * org.apache.beam.sdk.io.cdap.ConfigWrapper}.
 */
public class PluginConfigOptionsConverter {

  //  private static final Logger LOG = LoggerFactory.getLogger(PluginConfigOptionsConverter.class);

  private static final String SALESFORCE_STREAMING_PUSH_TOPIC_NAME = "pushTopicName";
  private static final String SALESFORCE_PUSH_TOPIC_NOTIFY_CREATE = "pushTopicNotifyCreate";
  private static final String SALESFORCE_PUSH_TOPIC_NOTIFY_UPDATE = "pushTopicNotifyUpdate";
  private static final String SALESFORCE_PUSH_TOPIC_NOTIFY_DELETE = "pushTopicNotifyDelete";
  private static final String SALESFORCE_PUSH_TOPIC_NOTIFY_FOR_FIELDS = "pushTopicNotifyForFields";
  private static final String SALESFORCE_REFERENCED_NOTIFY_FOR_FIELDS = "Referenced";
  private static final String SALESFORCE_ENABLED_NOTIFY = "Enabled";

  public static Map<String, Object> hubspotOptionsToParamsMap(CdapHubspotOptions options) {
    String apiServerUrl = options.getApiServerUrl();
    ImmutableMap.Builder<String, Object> builder =
        ImmutableMap.<String, Object>builder()
            .put(
                BaseHubspotConfig.API_SERVER_URL,
                apiServerUrl != null ? apiServerUrl : BaseHubspotConfig.DEFAULT_API_SERVER_URL)
            .put(BaseHubspotConfig.API_KEY, options.getApiKey())
            .put(BaseHubspotConfig.OBJECT_TYPE, options.getObjectType())
            .put(Constants.Reference.REFERENCE_NAME, options.getReferenceName());
    if (options instanceof CdapHubspotStreamingSourceOptions) {
      // Hubspot PullFrequency value will be ignored as pull frequency is implemented differently in
      // CdapIO, but it still needs to be passed for the plugin to work correctly.
      builder.put(HubspotStreamingSourceConfig.PULL_FREQUENCY, PullFrequency.MINUTES_15.getName());
    }
    return builder.build();
  }

  public static Map<String, Object> serviceNowOptionsToParamsMap(CdapServiceNowOptions options) {
    return ImmutableMap.<String, Object>builder()
        .put(ServiceNowConstants.PROPERTY_CLIENT_ID, options.getClientId())
        .put(ServiceNowConstants.PROPERTY_CLIENT_SECRET, options.getClientSecret())
        .put(ServiceNowConstants.PROPERTY_USER, options.getUser())
        .put(ServiceNowConstants.PROPERTY_PASSWORD, options.getPassword())
        .put(ServiceNowConstants.PROPERTY_API_ENDPOINT, options.getRestApiEndpoint())
        .put(ServiceNowConstants.PROPERTY_QUERY_MODE, options.getQueryMode())
        .put(ServiceNowConstants.PROPERTY_TABLE_NAME, options.getTableName())
        .put(ServiceNowConstants.PROPERTY_VALUE_TYPE, options.getValueType())
        .put(Constants.Reference.REFERENCE_NAME, options.getReferenceName())
        .build();
  }

  public static Map<String, Object> salesforceStreamingSourceOptionsToParamsMap(
      CdapSalesforceStreamingSourceOptions options) {
    return ImmutableMap.<String, Object>builder()
        .put(Constants.Reference.REFERENCE_NAME, options.getReferenceName())
        .put(SALESFORCE_STREAMING_PUSH_TOPIC_NAME, options.getPushTopicName())
        .put(SalesforceConstants.PROPERTY_USERNAME, options.getUsername())
        .put(SalesforceConstants.PROPERTY_PASSWORD, options.getPassword())
        .put(SalesforceConstants.PROPERTY_SECURITY_TOKEN, options.getSecurityToken())
        .put(SalesforceConstants.PROPERTY_CONSUMER_KEY, options.getConsumerKey())
        .put(SalesforceConstants.PROPERTY_CONSUMER_SECRET, options.getConsumerSecret())
        .put(SalesforceConstants.PROPERTY_LOGIN_URL, options.getLoginUrl())
        .put(SalesforceSourceConstants.PROPERTY_SOBJECT_NAME, options.getSObjectName())
        .put(SALESFORCE_PUSH_TOPIC_NOTIFY_CREATE, SALESFORCE_ENABLED_NOTIFY)
        .put(SALESFORCE_PUSH_TOPIC_NOTIFY_UPDATE, SALESFORCE_ENABLED_NOTIFY)
        .put(SALESFORCE_PUSH_TOPIC_NOTIFY_DELETE, SALESFORCE_ENABLED_NOTIFY)
        .put(SALESFORCE_PUSH_TOPIC_NOTIFY_FOR_FIELDS, SALESFORCE_REFERENCED_NOTIFY_FOR_FIELDS)
        .build();
  }

  public static Map<String, Object> salesforceBatchSourceOptionsToParamsMap(
      CdapSalesforceSourceOptions options) {
    return ImmutableMap.<String, Object>builder()
        .put(Constants.Reference.REFERENCE_NAME, options.getReferenceName())
        .put(SalesforceConstants.PROPERTY_USERNAME, options.getUsername())
        .put(SalesforceConstants.PROPERTY_PASSWORD, options.getPassword())
        .put(SalesforceConstants.PROPERTY_SECURITY_TOKEN, options.getSecurityToken())
        .put(SalesforceConstants.PROPERTY_CONSUMER_KEY, options.getConsumerKey())
        .put(SalesforceConstants.PROPERTY_CONSUMER_SECRET, options.getConsumerSecret())
        .put(SalesforceConstants.PROPERTY_LOGIN_URL, options.getLoginUrl())
        .put(SalesforceSourceConstants.PROPERTY_SOBJECT_NAME, options.getSObjectName())
        .build();
  }

  public static Map<String, Object> salesforceBatchSinkOptionsToParamsMap(
      CdapSalesforceSinkOptions options) {
    return ImmutableMap.<String, Object>builder()
        .put(Constants.Reference.REFERENCE_NAME, options.getReferenceName())
        .put(SalesforceConstants.PROPERTY_USERNAME, options.getUsername())
        .put(SalesforceConstants.PROPERTY_PASSWORD, options.getPassword())
        .put(SalesforceConstants.PROPERTY_SECURITY_TOKEN, options.getSecurityToken())
        .put(SalesforceConstants.PROPERTY_CONSUMER_KEY, options.getConsumerKey())
        .put(SalesforceConstants.PROPERTY_CONSUMER_SECRET, options.getConsumerSecret())
        .put(SalesforceConstants.PROPERTY_LOGIN_URL, options.getLoginUrl())
        .put(SalesforceSinkConfig.PROPERTY_SOBJECT, options.getsObject())
        .put(SalesforceSinkConfig.PROPERTY_OPERATION, options.getOperation())
        .put(
            SalesforceSinkConfig.PROPERTY_ERROR_HANDLING,
            ErrorHandling.valueOf(options.getErrorHandling()).getValue())
        .put(SalesforceSinkConfig.PROPERTY_MAX_BYTES_PER_BATCH, options.getMaxBytesPerBatch())
        .put(SalesforceSinkConfig.PROPERTY_MAX_RECORDS_PER_BATCH, options.getMaxRecordsPerBatch())
        .build();
  }

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
