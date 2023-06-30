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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

@AutoValue
public abstract class JmsPoolConfiguration implements Serializable {
  private static final Integer DEFAULT_MAX_ACTIVE_CONNECTIONS = 100;
  private static final Integer DEFAULT_INITIAL_ACTIVE_CONNECTIONS = 20;
  private static final Duration DEFAULT_MAX_TIMEOUT_DURATION = Duration.standardMinutes(5);

  abstract int getMaxActiveConnections();

  public abstract int getInitialActiveConnections();

  public abstract Duration getMaxTimeout();

  public static JmsPoolConfiguration create() {
    return create(DEFAULT_MAX_ACTIVE_CONNECTIONS, DEFAULT_INITIAL_ACTIVE_CONNECTIONS, null);
  }

  public static JmsPoolConfiguration create(
      int maxActiveConnections, int initialActiveConnections, @Nullable Duration maxTimeout) {
    checkArgument(maxActiveConnections > 0, "maxActiveConnections should be greater than 0");
    checkArgument(
        initialActiveConnections > 0, "initialActiveConnections should be greater than 0");

    if (maxTimeout == null || maxTimeout.equals(Duration.ZERO)) {
      maxTimeout = DEFAULT_MAX_TIMEOUT_DURATION;
    }

    return new AutoValue_JmsPoolConfiguration.Builder()
        .setMaxActiveConnections(maxActiveConnections)
        .setInitialActiveConnections(initialActiveConnections)
        .setMaxTimeout(maxTimeout)
        .build();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract JmsPoolConfiguration.Builder setMaxActiveConnections(int maxActiveConnections);

    abstract JmsPoolConfiguration.Builder setInitialActiveConnections(int initialActiveConnections);

    abstract JmsPoolConfiguration.Builder setMaxTimeout(Duration maxTimeout);

    abstract JmsPoolConfiguration build();
  }
}
