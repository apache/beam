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
package org.apache.beam.runners.dataflow.worker.windmill;

import java.util.Set;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemReceiver;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;

/** Client for WindmillService via Streaming Engine. */
@Internal
public interface StreamingEngineWindmillClient {
  /** Returns the windmill service endpoints set by setWindmillServiceEndpoints */
  ImmutableSet<HostAndPort> getWindmillServiceEndpoints();

  /**
   * Sets the new endpoints used to talk to windmill. Upon first call, the stubs are initialized. On
   * subsequent calls, if endpoints are different from previous values new stubs are created,
   * replacing the previous ones.
   */
  void setWindmillServiceEndpoints(Set<HostAndPort> endpoints);

  /**
   * Gets work to process, returned as a stream.
   *
   * <p>Each time a WorkItem is received, it will be passed to the given receiver. The returned
   * GetWorkStream object can be used to control the lifetime of the stream.
   */
  WindmillStream.GetWorkStream getWorkStream(
      Windmill.GetWorkRequest request, WorkItemReceiver receiver);

  /** Get additional data such as state needed to process work, returned as a stream. */
  WindmillStream.GetDataStream getDataStream();

  /** Returns a stream allowing individual WorkItemCommitRequests to be streamed to Windmill. */
  WindmillStream.CommitWorkStream commitWorkStream();
}
