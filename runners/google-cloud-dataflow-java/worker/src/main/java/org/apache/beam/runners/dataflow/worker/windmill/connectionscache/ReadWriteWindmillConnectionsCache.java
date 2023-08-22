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
package org.apache.beam.runners.dataflow.worker.windmill.connectionscache;

import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;

public interface ReadWriteWindmillConnectionsCache extends ReadOnlyWindmillConnectionsCache {
  void consumeWindmillDispatcherEndpoints(ImmutableSet<HostAndPort> dispatcherEndpoints);

  void consumeWindmillWorkerEndpoints(WindmillEndpoints windmillEndpoints);

  /**
   * To pass as {@link ReadOnlyWindmillConnectionsCache} to enforce read only interface. This allows
   * optimization for many concurrent readers. {@link
   * org.apache.beam.runners.dataflow.worker.windmill.WindmillStream}(s) will hold a reference to
   * the read-only interface, with updates coming from the consumer of the worker metadata produced
   * in {@link
   * org.apache.beam.runners.dataflow.worker.windmill.WindmillStream.GetWorkerMetadataStream}.
   */
  default ReadOnlyWindmillConnectionsCache asReadOnlyCache() {
    return this;
  }
}
