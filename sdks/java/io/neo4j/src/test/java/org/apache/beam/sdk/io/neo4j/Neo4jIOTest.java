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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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

    driverConfiguration = driverConfiguration.withEncryption();
    Assert.assertEquals(true, driverConfiguration.getEncryption().get());

    driverConfiguration = driverConfiguration.withoutEncryption();
    Assert.assertEquals(false, driverConfiguration.getEncryption().get());

    driverConfiguration = driverConfiguration.withUrl("url1");
    Assert.assertEquals("url1", driverConfiguration.getUrl().get());

    // URL and URLs can be set independently but are both used
    driverConfiguration = driverConfiguration.withUrls(Arrays.asList("url2", "url3", "url4"));
    Assert.assertEquals(3, driverConfiguration.getUrls().get().size());

    driverConfiguration = driverConfiguration.withUsername("username");
    Assert.assertEquals("username", driverConfiguration.getUsername().get());

    driverConfiguration = driverConfiguration.withPassword("password");
    Assert.assertEquals("password", driverConfiguration.getPassword().get());

    driverConfiguration = driverConfiguration.withRouting();
    Assert.assertEquals(true, driverConfiguration.getRouting().get());

    driverConfiguration = driverConfiguration.withoutRouting();
    Assert.assertEquals(false, driverConfiguration.getRouting().get());

    driverConfiguration = driverConfiguration.withConnectionAcquisitionTimeoutMs(54321L);
    Assert.assertEquals(
        54321L, (long) driverConfiguration.getConnectionAcquisitionTimeoutMs().get());

    driverConfiguration = driverConfiguration.withConnectionTimeoutMs(43210L);
    Assert.assertEquals(43210L, (long) driverConfiguration.getConnectionTimeoutMs().get());

    driverConfiguration = driverConfiguration.withConnectionLivenessCheckTimeoutMs(32109L);
    Assert.assertEquals(
        32109L, (long) driverConfiguration.getConnectionLivenessCheckTimeoutMs().get());

    driverConfiguration = driverConfiguration.withMaxConnectionLifetimeMs(21098L);
    Assert.assertEquals(21098L, (long) driverConfiguration.getMaxConnectionLifetimeMs().get());

    driverConfiguration = driverConfiguration.withMaxTransactionRetryTimeMs(10987L);
    Assert.assertEquals(10987L, (long) driverConfiguration.getMaxTransactionRetryTimeMs().get());

    driverConfiguration = driverConfiguration.withMaxConnectionPoolSize(101);
    Assert.assertEquals(101, (int) driverConfiguration.getMaxConnectionPoolSize().get());
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
