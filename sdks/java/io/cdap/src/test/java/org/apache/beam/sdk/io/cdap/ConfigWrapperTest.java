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
import java.io.File;
import java.util.Map;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test class for {@link ConfigWrapper}. */
@RunWith(JUnit4.class)
public class ConfigWrapperTest {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigWrapperTest.class);

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
  private static final String TEST_SALESFORCE_PARAMS_JSON_STRING =
      "{\n"
          + "\"sObjectName\": \"sObject\",\n"
          + "\"datetimeAfter\": \"datetime\",\n"
          + "\"consumerKey\": \"key\",\n"
          + "\"consumerSecret\": \"secret\",\n"
          + "\"username\": \"user\",\n"
          + "\"password\": \"password\",\n"
          + "\"loginUrl\": \"https://www.google.com\",\n"
          + "\"referenceName\": \"reference\"\n"
          + "}";
  private static final String SALESFORCE_TEST_PARAMS_JSON =
      "src/test/resources/salesforce_test_params.json";
  public static final String REFERENCE_NAME_PARAM_NAME = "referenceName";

  @Test
  public void testBuildingPluginConfigFromParamsMap() {
    try {
      String newReferenceName = "new reference name";
      SalesforceSourceConfig config =
          new ConfigWrapper<>(SalesforceSourceConfig.class)
              .withParams(TEST_SALESFORCE_PARAMS_MAP)
              .setParam("referenceName", newReferenceName)
              .build();
      assertNotNull(config);
      validateSalesforceConfigObject(TEST_SALESFORCE_PARAMS_MAP, config);
      assertEquals(newReferenceName, config.referenceName);
    } catch (Exception e) {
      LOG.error("Error occurred while building the config object", e);
      fail();
    }
  }

  @Test
  public void testBuildingPluginConfigFromJsonFile() {
    try {
      String newReferenceName = "new reference name";
      SalesforceSourceConfig config =
          new ConfigWrapper<>(SalesforceSourceConfig.class)
              .fromJsonFile(new File(SALESFORCE_TEST_PARAMS_JSON))
              .setParam(REFERENCE_NAME_PARAM_NAME, newReferenceName)
              .build();
      assertNotNull(config);
      validateSalesforceConfigObject(TEST_SALESFORCE_PARAMS_MAP, config);
      assertEquals(newReferenceName, config.referenceName);
    } catch (Exception e) {
      LOG.error("Error occurred while building the config object", e);
      fail();
    }
  }

  @Test
  public void testBuildingPluginConfigFromJsonString() {
    try {
      String newReferenceName = "new reference name";
      SalesforceSourceConfig config =
          new ConfigWrapper<>(SalesforceSourceConfig.class)
              .fromJsonString(TEST_SALESFORCE_PARAMS_JSON_STRING)
              .setParam(REFERENCE_NAME_PARAM_NAME, newReferenceName)
              .build();
      assertNotNull(config);
      validateSalesforceConfigObject(TEST_SALESFORCE_PARAMS_MAP, config);
      assertEquals(newReferenceName, config.referenceName);
    } catch (Exception e) {
      LOG.error("Error occurred while building the config object", e);
      fail();
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
