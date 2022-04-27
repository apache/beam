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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceBatchSource;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceInputFormat;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceInputFormatProvider;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceSourceConfig;
import java.util.HashMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.io.MapWritable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class PluginTest {

  private static final Logger LOG = LoggerFactory.getLogger(PluginTest.class);

  private static final ImmutableMap<String, String> TEST_SALESFORCE_PARAMS_MAP =
      ImmutableMap.<String, String>builder()
          .put("sObjectName", "sObject")
          .put("datetimeAfter", "datetime")
          .put("consumerKey", "key")
          .put("consumerSecret", "secret")
          .put("username", "user")
          .put("password", "password")
          .put("loginUrl", "https://www.google.com")
          .put("referenceName", "some reference name")
          .build();

  private static final HashMap<String, Object> TEST_SALESFORCE_PARAMS_MAP_OBJ =
      new HashMap<>(TEST_SALESFORCE_PARAMS_MAP);

  private static final String REFERENCE_NAME_PARAM_NAME = "referenceName";

  /** Config for Salesforce Batch Source plugin. */
  public SalesforceSourceConfig salesforceSourceConfig =
      new ConfigWrapper<>(SalesforceSourceConfig.class)
          .withParams(TEST_SALESFORCE_PARAMS_MAP_OBJ)
          .setParam(REFERENCE_NAME_PARAM_NAME, "some reference name")
          .build();

  @Test
  public void testBuildingSourcePluginWithCDAPClasses() {
    try {
      Plugin salesforceSourcePlugin =
          Plugin.create(
                  SalesforceBatchSource.class,
                  SalesforceInputFormat.class,
                  SalesforceInputFormatProvider.class)
              .withConfig(salesforceSourceConfig)
              .withHadoopConfiguration(Schema.class, MapWritable.class);

      assertEquals(SalesforceBatchSource.class, salesforceSourcePlugin.getPluginClass());
      assertEquals(SalesforceInputFormat.class, salesforceSourcePlugin.getFormatClass());
      assertEquals(
          SalesforceInputFormatProvider.class, salesforceSourcePlugin.getFormatProviderClass());
      assertEquals(salesforceSourceConfig, salesforceSourcePlugin.getPluginConfig());
      assertEquals(
          SalesforceInputFormat.class,
          salesforceSourcePlugin
              .getHadoopConfiguration()
              .getClass(
                  PluginConstants.Hadoop.SOURCE.getFormatClass(),
                  PluginConstants.Format.INPUT.getFormatClass()));
    } catch (Exception e) {
      LOG.error("Error occurred while building the Salesforce Source Plugin", e);
      fail();
    }
  }

  @Test
  public void testSettingPluginType() {
    Plugin salesforceSourcePlugin =
        Plugin.create(
                SalesforceBatchSource.class,
                SalesforceInputFormat.class,
                SalesforceInputFormatProvider.class)
            .withConfig(salesforceSourceConfig)
            .withHadoopConfiguration(Schema.class, MapWritable.class);

    assertEquals(PluginConstants.PluginType.SOURCE, salesforceSourcePlugin.getPluginType());
  }

  @Test
  @SuppressWarnings("UnusedVariable")
  public void testSettingPluginTypeFailed() {
    try {
      Plugin salesforceSourcePlugin =
          Plugin.create(Object.class, Object.class, Object.class)
              .withConfig(salesforceSourceConfig)
              .withHadoopConfiguration(Schema.class, MapWritable.class);
      fail("This should have thrown an exception");
    } catch (Exception e) {
      assertEquals("Provided class should be source or sink plugin", e.getMessage());
    }
  }
}
