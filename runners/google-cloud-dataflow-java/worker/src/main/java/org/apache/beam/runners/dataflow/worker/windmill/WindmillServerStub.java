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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;
import org.apache.beam.runners.dataflow.worker.status.StatusDataProvider;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemReceiver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;

/** Stub for communicating with a Windmill server. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public abstract class WindmillServerStub implements StatusDataProvider {

  /**
   * Sets the new endpoints used to talk to windmill. Upon first call, the stubs are initialized. On
   * subsequent calls, if endpoints are different from previous values new stubs are created,
   * replacing the previous ones.
   */
  public abstract void setWindmillServiceEndpoints(Set<HostAndPort> endpoints) throws IOException;

  /** Returns true iff this WindmillServerStub is ready for making API calls. */
  public abstract boolean isReady();

  /** Get a batch of work to process. */
  public abstract Windmill.GetWorkResponse getWork(Windmill.GetWorkRequest request);

  /** Get additional data such as state needed to process work. */
  public abstract Windmill.GetDataResponse getData(Windmill.GetDataRequest request);

  /** Commit the work, issuing any output productions, state modifications etc. */
  public abstract Windmill.CommitWorkResponse commitWork(Windmill.CommitWorkRequest request);

  /** Get configuration data from the server. */
  public abstract Windmill.GetConfigResponse getConfig(Windmill.GetConfigRequest request);

  /** Report execution information to the server. */
  public abstract Windmill.ReportStatsResponse reportStats(Windmill.ReportStatsRequest request);

  /**
   * Gets work to process, returned as a stream.
   *
   * <p>Each time a WorkItem is received, it will be passed to the given receiver. The returned
   * GetWorkStream object can be used to control the lifetime of the stream.
   */
  public abstract GetWorkStream getWorkStream(
      Windmill.GetWorkRequest request, WorkItemReceiver receiver);

  /** Get additional data such as state needed to process work, returned as a stream. */
  public abstract GetDataStream getDataStream();

  /** Returns a stream allowing individual WorkItemCommitRequests to be streamed to Windmill. */
  public abstract CommitWorkStream commitWorkStream();

  /** Returns the amount of time the server has been throttled and resets the time to 0. */
  public abstract long getAndResetThrottleTime();

  @Override
  public void appendSummaryHtml(PrintWriter writer) {}

  /** Generic Exception type for implementors to use to represent errors while making RPCs. */
  public static final class RpcException extends RuntimeException {
    public RpcException(Throwable cause) {
      super(cause);
    }
  }
}
