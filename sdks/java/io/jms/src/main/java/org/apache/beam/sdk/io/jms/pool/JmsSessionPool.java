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
package org.apache.beam.sdk.io.jms.pool;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import javax.jms.Session;
import org.apache.beam.sdk.io.jms.JmsIO;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class JmsSessionPool<T> extends SerializableSessionPool<Session> {

  private final String id = UUID.randomUUID().toString();

  private final JmsIO.Write<T> spec;

  public JmsSessionPool(JmsIO.Write<T> spec) {
    super(spec.getJmsPoolConfiguration().getMaxActiveConnections());
    this.spec = spec;
  }

  @Override
  public ObjectPool<Session> createDelegate() {
    JmsPoolConfiguration configuration = spec.getJmsPoolConfiguration();
    JmsSessionFactory<T> factory = new JmsSessionFactory<>(spec);
    GenericObjectPoolConfig<Session> config = new GenericObjectPoolConfig<>();
    config.setLifo(false);
    config.setFairness(true);
    config.setJmxEnabled(false);
    config.setTestOnCreate(true);
    config.setTestOnBorrow(true);
    config.setTestWhileIdle(true);
    config.setTestOnReturn(true);
    config.setNumTestsPerEvictionRun(-1);
    config.setMinEvictableIdleTime(Duration.ofSeconds(60));
    config.setTimeBetweenEvictionRuns(Duration.ofSeconds(15));
    config.setSoftMinEvictableIdleTime(Duration.ofSeconds(60));
    config.setMaxWait(
        Duration.ofSeconds(
            Objects.requireNonNull(configuration.getMaxTimeout()).getStandardSeconds()));
    config.setMaxTotal(configuration.getMaxActiveConnections());
    return new GenericObjectPool<>(factory, config);
  }

  @Override
  public String toString() {
    return String.format("Session Pool id: %s", id);
  }
}
