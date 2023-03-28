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
package org.apache.beam.sdk.io.hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HBaseSharedConnectionTest {
  static Configuration configuration;
  static Configuration configuration2;

  @BeforeClass
  public static void setup() {
    configuration = HBaseConfiguration.create();
    // Create a slightly different configuration
    configuration2 = HBaseConfiguration.create();
    configuration2.set("hbase.zookeeper.quorum", "random-server");
  }

  @After
  public void resetConnectionPool() throws IOException {
    HBaseSharedConnection.closeAll();

    Assert.assertEquals(0, HBaseSharedConnection.getConnectionPoolSize());
  }

  @Test
  public void testOpenThenCloseConnection() throws IOException {
    Assert.assertEquals(0, HBaseSharedConnection.getConnectionCount(configuration));

    HBaseSharedConnection.getOrCreate(configuration);
    Assert.assertEquals(1, HBaseSharedConnection.getConnectionCount(configuration));
    HBaseSharedConnection.getOrCreate(configuration);
    Assert.assertEquals(2, HBaseSharedConnection.getConnectionCount(configuration));

    HBaseSharedConnection.close(configuration);
    Assert.assertEquals(1, HBaseSharedConnection.getConnectionCount(configuration));
    HBaseSharedConnection.close(configuration);
    Assert.assertEquals(0, HBaseSharedConnection.getConnectionCount(configuration));
  }

  @Test
  public void testCloseNonExistentConnection() throws IOException {
    HBaseSharedConnection.close(configuration);
    Assert.assertEquals(0, HBaseSharedConnection.getConnectionPoolSize());
  }

  @Test
  public void testCloseThenOpenConnection() throws IOException {
    HBaseSharedConnection.close(configuration);
    HBaseSharedConnection.getOrCreate(configuration);

    Assert.assertEquals(1, HBaseSharedConnection.getConnectionPoolSize());
    Assert.assertEquals(1, HBaseSharedConnection.getConnectionCount(configuration));
  }

  @Test
  public void testCloseConnectionMultipleTimes() throws IOException {
    HBaseSharedConnection.getOrCreate(configuration);

    HBaseSharedConnection.close(configuration);
    HBaseSharedConnection.close(configuration);

    Assert.assertEquals(0, HBaseSharedConnection.getConnectionPoolSize());
  }

  @Test
  public void testOpenMultipleConnections() throws IOException {
    HBaseSharedConnection.getOrCreate(configuration);
    HBaseSharedConnection.getOrCreate(configuration2);
    HBaseSharedConnection.getOrCreate(configuration2);

    Assert.assertEquals(2, HBaseSharedConnection.getConnectionPoolSize());
    Assert.assertEquals(1, HBaseSharedConnection.getConnectionCount(configuration));
    Assert.assertEquals(2, HBaseSharedConnection.getConnectionCount(configuration2));
  }
}
