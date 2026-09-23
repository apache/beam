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
package org.apache.beam.sdk.io.iceberg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class IcebergCatalogConfigTest {

  @Test
  public void testGetHadoopConfigurationWhenPropertiesNull() {
    IcebergCatalogConfig config =
        IcebergCatalogConfig.builder().setCatalogName("test_catalog").build();

    Configuration hadoopConf = config.getHadoopConfiguration();
    assertNotNull(hadoopConf);
    assertNull(hadoopConf.get("non.existent.key"));
  }

  @Test
  public void testGetHadoopConfigurationPopulatesProperties() {
    IcebergCatalogConfig config =
        IcebergCatalogConfig.builder()
            .setCatalogName("test_catalog")
            .setConfigProperties(
                ImmutableMap.of(
                    "fs.defaultFS", "file:///test/path",
                    "custom.hadoop.key", "custom-hadoop-val"))
            .build();

    Configuration hadoopConf = config.getHadoopConfiguration();
    assertNotNull(hadoopConf);
    assertEquals("file:///test/path", hadoopConf.get("fs.defaultFS"));
    assertEquals("custom-hadoop-val", hadoopConf.get("custom.hadoop.key"));
  }
}
