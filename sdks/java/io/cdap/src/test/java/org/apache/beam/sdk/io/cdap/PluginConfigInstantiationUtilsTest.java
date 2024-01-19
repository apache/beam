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

import io.cdap.plugin.servicenow.source.ServiceNowSourceConfig;
import io.cdap.plugin.servicenow.source.util.ServiceNowConstants;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
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

  private static final ImmutableMap<String, Object> TEST_SERVICE_NOW_PARAMS_MAP =
      ImmutableMap.<String, java.lang.Object>builder()
          .put(ServiceNowConstants.PROPERTY_CLIENT_ID, "clientId")
          .put(ServiceNowConstants.PROPERTY_CLIENT_SECRET, "clientSecret")
          .put(ServiceNowConstants.PROPERTY_API_ENDPOINT, "https://www.google.com")
          .put(ServiceNowConstants.PROPERTY_QUERY_MODE, "Table")
          .put(ServiceNowConstants.PROPERTY_USER, "user")
          .put(ServiceNowConstants.PROPERTY_PASSWORD, "password")
          .put(ServiceNowConstants.PROPERTY_TABLE_NAME, "tableName")
          .put(ServiceNowConstants.PROPERTY_VALUE_TYPE, "Actual")
          .put("referenceName", "oldReference")
          .build();

  @Test
  public void testBuildingPluginConfigFromParamsMap() {
    try {
      ServiceNowSourceConfig config =
          PluginConfigInstantiationUtils.getPluginConfig(
              TEST_SERVICE_NOW_PARAMS_MAP, ServiceNowSourceConfig.class);
      assertNotNull(config);
      validateServiceNowConfigObject(TEST_SERVICE_NOW_PARAMS_MAP, config);
    } catch (Exception e) {
      LOG.error("Error occurred while building the config object", e);
      fail();
    }
  }

  @Test
  public void testBuildingPluginConfigFromEmptyParamsMap() {
    try {
      ServiceNowSourceConfig config =
          PluginConfigInstantiationUtils.getPluginConfig(
              new HashMap<>(), ServiceNowSourceConfig.class);
      assertNotNull(config);
    } catch (Exception e) {
      LOG.error("Error occurred while building the config object", e);
      fail();
    }
  }

  @Test
  public void testBuildingPluginConfigFromNullClassFail() {
    try {
      PluginConfigInstantiationUtils.getPluginConfig(TEST_SERVICE_NOW_PARAMS_MAP, null);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Config class must be not null!", e.getMessage());
    }
  }

  private static void validateServiceNowConfigObject(
      Map<String, Object> params, ServiceNowSourceConfig config) {
    assertEquals(params.get(ServiceNowConstants.PROPERTY_CLIENT_ID), config.getClientId());
    assertEquals(params.get(ServiceNowConstants.PROPERTY_CLIENT_SECRET), config.getClientSecret());
    assertEquals(
        params.get(ServiceNowConstants.PROPERTY_API_ENDPOINT), config.getRestApiEndpoint());
    assertEquals(
        params.get(ServiceNowConstants.PROPERTY_QUERY_MODE), config.getQueryMode().getValue());
    assertEquals(params.get(ServiceNowConstants.PROPERTY_USER), config.getUser());
    assertEquals(params.get(ServiceNowConstants.PROPERTY_PASSWORD), config.getPassword());
    assertNotNull(config.getValueType());
    assertEquals(
        params.get(ServiceNowConstants.PROPERTY_VALUE_TYPE), config.getValueType().getValueType());
    assertEquals(params.get(ServiceNowConstants.PROPERTY_TABLE_NAME), config.getTableName());
  }
}
