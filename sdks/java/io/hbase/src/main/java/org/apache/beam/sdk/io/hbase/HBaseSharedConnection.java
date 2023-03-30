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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.shaded.com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Pair;
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

  // Transient connection pool to be initialized per worker
  // Integer represents number of threads connected, close connection if connectionCount goes to 0
  private static HashMap<String, Pair<Connection, Integer>> connectionPool = new HashMap<>();

  /**
   * Hash configuration to a string.
   *
   * <p>Zookeeper quorum is guaranteed to be unique per hbase cluster.
   *
   * @param configuration
   * @return
   */
  private static String hash(Configuration configuration) {
    Preconditions.checkNotNull(configuration);

    return configuration.get("hbase.zookeeper.quorum");
  }

  /**
   * Create or return existing Hbase connection.
   *
   * @param configuration Hbase configuration
   * @return Hbase connection
   * @throws IOException
   */
  public static synchronized Connection getOrCreate(Configuration configuration)
      throws IOException {
    String confString = hash(configuration);

    // Initialize connection if it didn't exist before
    if (!connectionPool.containsKey(confString)) {
      connectionPool.put(
          confString, new Pair<>(ConnectionFactory.createConnection(configuration), 0));
    }

    // Increment connection count
    connectionPool.get(confString).setSecond(connectionPool.get(confString).getSecond() + 1);

    return connectionPool.get(confString).getFirst();
  }

  /**
   * Closes all open connections in pool.
   *
   * @throws IOException
   */
  @Internal
  static synchronized void closeAll() throws IOException {
    Set<String> set = new HashSet<>(connectionPool.keySet());
    for (String confString : set) {
      closeAll(confString);
    }
  }

  /**
   * Decrement connector count and close connection if no more connector is using it.
   *
   * @throws IOException
   */
  public static synchronized void close(Configuration configuration) throws IOException {
    String confString = hash(configuration);
    close(confString);
  }

  private static synchronized void closeAll(String confString) throws IOException {
    while (connectionPool.containsKey(confString)) {
      close(confString);
    }
  }

  private static synchronized void close(String confString) throws IOException {
    if (!connectionPool.containsKey(confString)) {
      return;
    }
    // Decrement connection count
    connectionPool.get(confString).setSecond(connectionPool.get(confString).getSecond() - 1);

    // Warn if connection count is not 0 and reset connection count
    if (connectionPool.get(confString).getSecond() < 0) {
      LOG.warn("Connection count for + " + confString + " at below 0, " + getDebugString());
      connectionPool.get(confString).setSecond(0);
    }

    // Close connection and remove entry from connection pool and count
    if (connectionPool.get(confString).getSecond() == 0) {
      connectionPool.get(confString).getFirst().close();

      // Remove entry from pool
      connectionPool.remove(confString);
    }
  }

  public static String getDebugString() {
    return String.format("Connection pool status: %s%n", connectionPool);
  }

  public static int getConnectionPoolSize() {
    return connectionPool.size();
  }

  public static int getConnectionCount(Configuration configuration) {
    String confString = hash(configuration);

    if (!connectionPool.containsKey(confString)) {
      return 0;
    }

    return connectionPool.get(confString).getSecond();
  }
}
