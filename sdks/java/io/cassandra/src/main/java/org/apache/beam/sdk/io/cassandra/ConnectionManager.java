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
package org.apache.beam.sdk.io.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.io.cassandra.CassandraIO.Read;
import org.apache.beam.sdk.options.ValueProvider;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class ConnectionManager {

  private static final ConcurrentHashMap<String, Cluster> clusterMap =
      new ConcurrentHashMap<String, Cluster>();
  private static final ConcurrentHashMap<String, Session> sessionMap =
      new ConcurrentHashMap<String, Session>();

  static {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  for (Session session : sessionMap.values()) {
                    if (!session.isClosed()) {
                      session.close();
                    }
                  }
                }));
  }

  private static String readToClusterHash(Read<?> read) {
    return Objects.requireNonNull(read.hosts()).get().stream().reduce(",", (a, b) -> a + b)
        + Objects.requireNonNull(read.port()).get()
        + safeVPGet(read.localDc())
        + safeVPGet(read.consistencyLevel());
  }

  private static String readToSessionHash(Read<?> read) {
    return readToClusterHash(read) + read.keyspace().get();
  }

  static Session getSession(Read<?> read) {
    Cluster cluster =
        clusterMap.computeIfAbsent(
            readToClusterHash(read),
            k ->
                CassandraIO.getCluster(
                    Objects.requireNonNull(read.hosts()),
                    Objects.requireNonNull(read.port()),
                    read.username(),
                    read.password(),
                    read.localDc(),
                    read.consistencyLevel(),
                    read.connectTimeout(),
                    read.readTimeout(),
                    read.sslOptions()));
    return sessionMap.computeIfAbsent(
        readToSessionHash(read),
        k -> cluster.connect(Objects.requireNonNull(read.keyspace()).get()));
  }

  private static String safeVPGet(ValueProvider<String> s) {
    return s != null ? s.get() : "";
  }
}
