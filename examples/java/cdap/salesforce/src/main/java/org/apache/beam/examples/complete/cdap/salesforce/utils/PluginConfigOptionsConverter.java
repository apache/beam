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
package org.apache.beam.examples.complete.cdap.salesforce.utils;

import io.cdap.plugin.common.Constants;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.plugin.sink.batch.ErrorHandling;
import io.cdap.plugin.salesforce.plugin.sink.batch.SalesforceSinkConfig;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import java.util.Map;
import org.apache.beam.examples.complete.cdap.salesforce.options.CdapSalesforceSinkOptions;
import org.apache.beam.examples.complete.cdap.salesforce.options.CdapSalesforceSourceOptions;
import org.apache.beam.examples.complete.cdap.salesforce.options.CdapSalesforceStreamingSourceOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * Class for converting CDAP {@link org.apache.beam.sdk.options.PipelineOptions} to map for {@link
 * org.apache.beam.sdk.io.cdap.ConfigWrapper}.
 */
public class PluginConfigOptionsConverter {

  private static final String SALESFORCE_STREAMING_PUSH_TOPIC_NAME = "pushTopicName";
  private static final String SALESFORCE_PUSH_TOPIC_NOTIFY_CREATE = "pushTopicNotifyCreate";
  private static final String SALESFORCE_PUSH_TOPIC_NOTIFY_UPDATE = "pushTopicNotifyUpdate";
  private static final String SALESFORCE_PUSH_TOPIC_NOTIFY_DELETE = "pushTopicNotifyDelete";
  private static final String SALESFORCE_PUSH_TOPIC_NOTIFY_FOR_FIELDS = "pushTopicNotifyForFields";
  private static final String SALESFORCE_REFERENCED_NOTIFY_FOR_FIELDS = "Referenced";
  private static final String SALESFORCE_ENABLED_NOTIFY = "Enabled";

  /** Returns map of parameters for Cdap Salesforce streaming source plugin. */
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

  /** Returns map of parameters for Cdap Salesforce batch source plugin. */
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

  /** Returns map of parameters for Cdap Salesforce batch sink plugin. */
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
}
