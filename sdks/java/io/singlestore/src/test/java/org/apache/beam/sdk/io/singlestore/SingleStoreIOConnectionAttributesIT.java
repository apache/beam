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
package org.apache.beam.sdk.io.singlestore;

import static org.apache.beam.sdk.io.common.IOITHelper.readIOTestPipelineOptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SingleStoreIOConnectionAttributesIT {
  private static String serverName;

  private static String username;

  private static String password;

  private static Integer port;

  @BeforeClass
  public static void setup() throws Exception {
    SingleStoreIOTestPipelineOptions options;
    try {
      options = readIOTestPipelineOptions(SingleStoreIOTestPipelineOptions.class);
    } catch (IllegalArgumentException e) {
      options = null;
    }
    org.junit.Assume.assumeNotNull(options);

    serverName = options.getSingleStoreServerName();
    username = options.getSingleStoreUsername();
    password = options.getSingleStorePassword();
    port = options.getSingleStorePort();
  }

  @Test
  public void connectionAttributes() throws Exception {
    Map<String, String> attributes = new HashMap<String, String>();
    attributes.put("_connector_name", "Apache Beam SingleStoreDB I/O");
    attributes.put("_connector_version", ReleaseInfo.getReleaseInfo().getVersion());
    attributes.put("_product_version", ReleaseInfo.getReleaseInfo().getVersion());

    SingleStoreIO.DataSourceConfiguration dataSourceConfiguration =
        SingleStoreIO.DataSourceConfiguration.create(serverName + ":" + port)
            .withPassword(password)
            .withUsername(username);

    DataSource dataSource = dataSourceConfiguration.getDataSource();

    try (Connection conn = dataSource.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs =
            stmt.executeQuery("select * from information_schema.mv_connection_attributes"); ) {
      while (rs.next()) {
        String attribute = rs.getString(3);
        String value = rs.getString(4);
        if (attributes.containsKey(attribute)) {
          assertEquals(attributes.get(attribute), value);
          attributes.remove(attribute);
        }
      }
    }

    assertTrue(attributes.isEmpty());
  }
}
