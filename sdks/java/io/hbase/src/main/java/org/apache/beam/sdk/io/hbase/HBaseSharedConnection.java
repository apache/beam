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
import java.io.Serializable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static connection shared between all threads of a worker, i.e. connectors are transient within
 * single worker machine. Connectors are not persisted between worker machines as Connection
 * serialization is not implemented. Each worker will create its own connection and share it between
 * all its threads.
 */
class HBaseSharedConnection implements Serializable {
  private static final long serialVersionUID = 5252999807656940415L;
  private static final Logger LOG = LoggerFactory.getLogger(HBaseSharedConnection.class);

  // Transient connection to be initialized per worker
  // Wrap Connection in array because static Connection cannot be non-null in beam repo
  private static @MonotonicNonNull Connection connection = null;
  // Number of threads using the shared connection, close connection if connectionCount goes to 0
  private static int connectionCount;

  /**
   * Create or return existing Hbase connection.
   *
   * @param configuration Hbase configuration
   * @return Hbase connection
   * @throws IOException
   */
  public static synchronized Connection getOrCreate(Configuration configuration)
      throws IOException {
    if (connection == null || connection.isClosed()) {
      connection = ConnectionFactory.createConnection(configuration);
      connectionCount = 0;
    }
    connectionCount++;
    return connection;
  }

  /**
   * Decrement connector count and close connection if no more connector is using it.
   *
   * @throws IOException
   */
  public static synchronized void close() throws IOException {
    connectionCount--;
    if (connectionCount == 0) {
      if (connection != null) {
        connection.close();
      }
    }
    if (connectionCount < 0) {
      LOG.warn("Connection count at " + connectionCount + ", should not be possible");
      connectionCount = 0;
    }
  }

  public String getDebugString() {
    return String.format(
        "Connection down: %s%n" + "Connectors: %s%n",
        (connection == null || connection.isClosed()), connectionCount);
  }

  public int getConnectionCount() {
    return connectionCount;
  }
}
