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
package org.apache.beam.integration.nexmark;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * A set of {@link NexmarkConfiguration}s.
 */
public enum NexmarkSuite {
  /**
   * The default.
   */
  DEFAULT(defaultConf()),

  /**
   * Sweep through all queries using the default configuration.
   * 100k/10k events (depending on query).
   */
  SMOKE(smoke()),

  /**
   * As for SMOKE, but with 10m/1m events.
   */
  STRESS(stress()),

  /**
   * As for SMOKE, but with 1b/100m events.
   */
  FULL_THROTTLE(fullThrottle());

  private static List<NexmarkConfiguration> defaultConf() {
    List<NexmarkConfiguration> configurations = new ArrayList<>();
    NexmarkConfiguration configuration = new NexmarkConfiguration();
    configurations.add(configuration);
    return configurations;
  }

  private static List<NexmarkConfiguration> smoke() {
    List<NexmarkConfiguration> configurations = new ArrayList<>();
    for (int query = 0; query <= 12; query++) {
      NexmarkConfiguration configuration = NexmarkConfiguration.DEFAULT.clone();
      configuration.query = query;
      configuration.numEvents = 100_000;
      if (query == 4 || query == 6 || query == 9) {
        // Scale back so overall runtimes are reasonably close across all queries.
        configuration.numEvents /= 10;
      }
      configurations.add(configuration);
    }
    return configurations;
  }

  private static List<NexmarkConfiguration> stress() {
    List<NexmarkConfiguration> configurations = smoke();
    for (NexmarkConfiguration configuration : configurations) {
      if (configuration.numEvents >= 0) {
        configuration.numEvents *= 1000;
      }
    }
    return configurations;
  }

  private static List<NexmarkConfiguration> fullThrottle() {
    List<NexmarkConfiguration> configurations = smoke();
    for (NexmarkConfiguration configuration : configurations) {
      if (configuration.numEvents >= 0) {
        configuration.numEvents *= 1000;
      }
    }
    return configurations;
  }

  private final List<NexmarkConfiguration> configurations;

  NexmarkSuite(List<NexmarkConfiguration> configurations) {
    this.configurations = configurations;
  }

  /**
   * Return the configurations corresponding to this suite. We'll override each configuration
   * with any set command line flags, except for --isStreaming which is only respected for
   * the {@link #DEFAULT} suite.
   */
  public Iterable<NexmarkConfiguration> getConfigurations(NexmarkOptions options) {
    Set<NexmarkConfiguration> results = new LinkedHashSet<>();
    for (NexmarkConfiguration configuration : configurations) {
      NexmarkConfiguration result = configuration.clone();
      result.overrideFromOptions(options);
      results.add(result);
    }
    return results;
  }
}
