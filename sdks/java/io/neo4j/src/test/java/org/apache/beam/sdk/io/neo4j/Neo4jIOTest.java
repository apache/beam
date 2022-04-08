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
package org.apache.beam.sdk.io.neo4j;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.neo4j.driver.Config;

@RunWith(JUnit4.class)
public class Neo4jIOTest {

  @Test
  public void testDriverConfigurationCreate() throws Exception {
    Neo4jIO.DriverConfiguration driverConfiguration =
        Neo4jIO.DriverConfiguration.create("someUrl", "username", "password");
    Assert.assertEquals("someUrl", driverConfiguration.getUrl().get());
    Assert.assertEquals("username", driverConfiguration.getUsername().get());
    Assert.assertEquals("password", driverConfiguration.getPassword().get());
  }

  @Test
  public void testDriverConfigurationWith() throws Exception {
    Neo4jIO.DriverConfiguration driverConfiguration = Neo4jIO.DriverConfiguration.create();

    Config config =
        Config.builder()
            .withEncryption()
            .withConnectionAcquisitionTimeout(54321L, TimeUnit.MILLISECONDS)
            .withConnectionTimeout(43210L, TimeUnit.MILLISECONDS)
            .withConnectionLivenessCheckTimeout(32109L, TimeUnit.MILLISECONDS)
            .withMaxConnectionLifetime(21098L, TimeUnit.MILLISECONDS)
            .withMaxConnectionPoolSize(101)
            .build();

    driverConfiguration = driverConfiguration.withConfig(config);

    Config configVerify = driverConfiguration.getConfig();
    Assert.assertNotNull(configVerify);
    Assert.assertEquals(true, configVerify.encrypted());

    Assert.assertEquals(54321L, configVerify.connectionAcquisitionTimeoutMillis());
    Assert.assertEquals(43210L, configVerify.connectionTimeoutMillis());
    Assert.assertEquals(32109L, configVerify.idleTimeBeforeConnectionTest());
    Assert.assertEquals(21098L, configVerify.maxConnectionLifetimeMillis());
    Assert.assertEquals(101, configVerify.maxConnectionPoolSize());

    driverConfiguration = driverConfiguration.withUrl("url1");
    Assert.assertEquals("url1", driverConfiguration.getUrl().get());

    // URL and URLs can be set independently but are both used
    driverConfiguration = driverConfiguration.withUrls(Arrays.asList("url2", "url3", "url4"));
    Assert.assertEquals(3, driverConfiguration.getUrls().get().size());

    driverConfiguration = driverConfiguration.withUsername("username");
    Assert.assertEquals("username", driverConfiguration.getUsername().get());

    driverConfiguration = driverConfiguration.withPassword("password");
    Assert.assertEquals("password", driverConfiguration.getPassword().get());
  }

  @Test
  public void testDriverConfigurationErrors() throws Exception {
    Neo4jIO.DriverConfiguration driverConfiguration = Neo4jIO.DriverConfiguration.create();

    try {
      driverConfiguration.withUrl((String) null);
      Assert.fail("Null URL is not reported");
    } catch (Exception e) {
      // OK, error was reported
    }

    try {
      driverConfiguration.withUsername((String) null);
      Assert.fail("Null user is not reported");
    } catch (Exception e) {
      // OK, error was reported
    }

    try {
      driverConfiguration.withUsername("");
    } catch (Exception e) {
      throw new AssertionError("Empty user string should not throw an error", e);
    }

    try {
      driverConfiguration.withPassword((String) null);
      Assert.fail("Null password is not reported");
    } catch (Exception e) {
      // OK, error was reported
    }

    try {
      driverConfiguration.withPassword("");
    } catch (Exception e) {
      throw new AssertionError("Empty password string should not throw an error", e);
    }
  }
}
