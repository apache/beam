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
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.servicenow.source.ServiceNowInputFormat;
import io.cdap.plugin.servicenow.source.ServiceNowSource;
import io.cdap.plugin.servicenow.source.ServiceNowSourceConfig;
import io.cdap.plugin.servicenow.source.util.ServiceNowConstants;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.io.MapWritable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class PluginTest {

  private static final Logger LOG = LoggerFactory.getLogger(PluginTest.class);

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

  private static final String REFERENCE_NAME_PARAM_NAME = "referenceName";

  /** Config for ServiceNow Batch Source plugin. */
  public ServiceNowSourceConfig serviceNowSourceConfig =
      new ConfigWrapper<>(ServiceNowSourceConfig.class)
          .withParams(TEST_SERVICE_NOW_PARAMS_MAP)
          .setParam(REFERENCE_NAME_PARAM_NAME, "some reference name")
          .build();

  @Test
  public void testBuildingSourcePluginWithCDAPClasses() {
    try {
      Plugin<Schema, MapWritable> serviceNowSourcePlugin =
          Plugin.<Schema, MapWritable>createBatch(
                  ServiceNowSource.class,
                  ServiceNowInputFormat.class,
                  SourceInputFormatProvider.class)
              .withConfig(serviceNowSourceConfig)
              .withHadoopConfiguration(Schema.class, MapWritable.class);

      assertEquals(ServiceNowSource.class, serviceNowSourcePlugin.getPluginClass());
      assertEquals(ServiceNowInputFormat.class, serviceNowSourcePlugin.getFormatClass());
      assertEquals(
          SourceInputFormatProvider.class, serviceNowSourcePlugin.getFormatProviderClass());
      assertEquals(serviceNowSourceConfig, serviceNowSourcePlugin.getPluginConfig());
      assertEquals(
          ServiceNowInputFormat.class,
          serviceNowSourcePlugin
              .getHadoopConfiguration()
              .getClass(
                  PluginConstants.Hadoop.SOURCE.getFormatClass(),
                  PluginConstants.Format.INPUT.getFormatClass()));
    } catch (Exception e) {
      LOG.error("Error occurred while building the ServiceNow Source Plugin", e);
      fail();
    }
  }

  @Test
  public void testSettingPluginType() {
    Plugin<Schema, MapWritable> serviceNowSourcePlugin =
        Plugin.<Schema, MapWritable>createBatch(
                ServiceNowSource.class,
                ServiceNowInputFormat.class,
                SourceInputFormatProvider.class)
            .withConfig(serviceNowSourceConfig)
            .withHadoopConfiguration(Schema.class, MapWritable.class);

    assertEquals(PluginConstants.PluginType.SOURCE, serviceNowSourcePlugin.getPluginType());
  }

  @Test
  @SuppressWarnings("UnusedVariable")
  public void testSettingPluginTypeFailed() {
    try {
      Plugin<Schema, MapWritable> serviceNowSourcePlugin =
          Plugin.<Schema, MapWritable>createBatch(Object.class, Object.class, Object.class)
              .withConfig(serviceNowSourceConfig)
              .withHadoopConfiguration(Schema.class, MapWritable.class);
      fail("This should have thrown an exception");
    } catch (Exception e) {
      assertEquals("Provided class should be source or sink plugin", e.getMessage());
    }
  }
}
