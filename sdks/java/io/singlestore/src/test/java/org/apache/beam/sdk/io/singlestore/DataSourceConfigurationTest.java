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

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test DataSourceConfiguration. */
@RunWith(JUnit4.class)
public class DataSourceConfigurationTest {
  @Test
  public void testGetDataSource() throws SQLException {
    SingleStoreIO.DataSourceConfiguration configuration =
        SingleStoreIO.DataSourceConfiguration.create("localhost")
            .withDatabase("db")
            .withConnectionProperties("a=b;c=d")
            .withPassword("password")
            .withUsername("admin");

    BasicDataSource dataSource = (BasicDataSource) configuration.getDataSource();
    assertEquals("jdbc:singlestore://localhost/db", dataSource.getUrl());
    assertEquals("admin", dataSource.getUsername());
    assertEquals("password", dataSource.getPassword());
  }
}
