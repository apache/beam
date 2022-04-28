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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceSourceConfig;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test class for {@link PluginConfigInstantiationUtils}. */
@RunWith(JUnit4.class)
public class PluginConfigInstantiationUtilsTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(PluginConfigInstantiationUtilsTest.class);

  private static final ImmutableMap<String, Object> TEST_SALESFORCE_PARAMS_MAP =
      ImmutableMap.<String, java.lang.Object>builder()
          .put("sObjectName", "sObject")
          .put("datetimeAfter", "datetime")
          .put("consumerKey", "key")
          .put("consumerSecret", "secret")
          .put("username", "user")
          .put("password", "password")
          .put("loginUrl", "https://www.google.com")
          .put("referenceName", "oldReference")
          .build();

  @Test
  public void testBuildingPluginConfigFromParamsMap() {
    try {
      SalesforceSourceConfig config =
          PluginConfigInstantiationUtils.getPluginConfig(
              TEST_SALESFORCE_PARAMS_MAP, SalesforceSourceConfig.class);
      assertNotNull(config);
      validateSalesforceConfigObject(TEST_SALESFORCE_PARAMS_MAP, config);
    } catch (Exception e) {
      LOG.error("Error occurred while building the config object", e);
      fail();
    }
  }

  @Test
  public void testBuildingPluginConfigFromEmptyParamsMap() {
    try {
      SalesforceSourceConfig config =
          PluginConfigInstantiationUtils.getPluginConfig(
              new HashMap<>(), SalesforceSourceConfig.class);
      assertNotNull(config);
    } catch (Exception e) {
      LOG.error("Error occurred while building the config object", e);
      fail();
    }
  }

  @Test
  public void testBuildingPluginConfigFromNullClassFail() {
    try {
      PluginConfigInstantiationUtils.getPluginConfig(TEST_SALESFORCE_PARAMS_MAP, null);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Config class must be not null!", e.getMessage());
    }
  }

  private static void validateSalesforceConfigObject(
      Map<String, Object> params, SalesforceSourceConfig config) {
    assertEquals(
        params.get(SalesforceSourceConstants.PROPERTY_DATETIME_AFTER), config.getDatetimeAfter());
    assertEquals(
        params.get(SalesforceSourceConstants.PROPERTY_SOBJECT_NAME), config.getSObjectName());
    assertEquals(params.get(SalesforceConstants.PROPERTY_CONSUMER_KEY), config.getConsumerKey());
    assertEquals(
        params.get(SalesforceConstants.PROPERTY_CONSUMER_SECRET), config.getConsumerSecret());
    assertEquals(params.get(SalesforceConstants.PROPERTY_USERNAME), config.getUsername());
    assertEquals(params.get(SalesforceConstants.PROPERTY_PASSWORD), config.getPassword());
    assertEquals(params.get(SalesforceConstants.PROPERTY_LOGIN_URL), config.getLoginUrl());
  }
}
