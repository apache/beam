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
package org.apache.beam.runners.dataflow.worker.windmill.client.getdata;

import java.io.PrintWriter;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.CloseableStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamPool;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.Heartbeats;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;

/**
 * StreamingEngine implementation of {@link GetDataClient}.
 *
 * @implNote Uses {@link WindmillStreamPool} to send/receive requests. Depending on options, may use
 *     a dedicated stream pool for heartbeats.
 */
@Internal
@ThreadSafe
public final class StreamingEngineGetDataClient implements GetDataClient, WorkRefreshClient {

  private final WindmillStreamPool<GetDataStream> getDataStreamPool;
  private final ThrottlingGetDataMetricTracker getDataMetricTracker;

  public StreamingEngineGetDataClient(
      ThrottlingGetDataMetricTracker getDataMetricTracker,
      WindmillStreamPool<GetDataStream> getDataStreamPool) {
    this.getDataMetricTracker = getDataMetricTracker;
    this.getDataStreamPool = getDataStreamPool;
  }

  @Override
  public Windmill.KeyedGetDataResponse getStateData(
      String computation, KeyedGetDataRequest request) {
    try (AutoCloseable ignored =
            getDataMetricTracker.trackSingleCallWithThrottling(
                ThrottlingGetDataMetricTracker.Type.STATE);
        CloseableStream<GetDataStream> closeableStream = getDataStreamPool.getCloseableStream()) {
      return closeableStream.stream().requestKeyedData(computation, request);
    } catch (Exception e) {
      throw new GetDataException(
          "Error occurred fetching state for computation="
              + computation
              + ", key="
              + request.getShardingKey(),
          e);
    }
  }

  @Override
  public Windmill.GlobalData getSideInputData(GlobalDataRequest request) {
    try (AutoCloseable ignored =
            getDataMetricTracker.trackSingleCallWithThrottling(
                ThrottlingGetDataMetricTracker.Type.SIDE_INPUT);
        CloseableStream<GetDataStream> closeableStream = getDataStreamPool.getCloseableStream()) {
      return closeableStream.stream().requestGlobalData(request);
    } catch (Exception e) {
      throw new GetDataException(
          "Error occurred fetching side input for tag=" + request.getDataId(), e);
    }
  }

  @Override
  public void refreshActiveWork(Map<HeartbeatSender, Heartbeats> heartbeats) {
    Map.Entry<HeartbeatSender, Heartbeats> heartbeat =
        Iterables.getOnlyElement(heartbeats.entrySet());
    HeartbeatSender heartbeatSender = heartbeat.getKey();
    Heartbeats heartbeatToSend = heartbeat.getValue();

    if (heartbeatToSend.heartbeatRequests().isEmpty()) {
      return;
    }

    try (AutoCloseable ignored = getDataMetricTracker.trackHeartbeats(heartbeatToSend.size())) {
      heartbeatSender.sendHeartbeats(heartbeatToSend);
    } catch (Exception e) {
      throw new GetDataException("Error occurred refreshing heartbeats=" + heartbeatToSend, e);
    }
  }

  @Override
  public void printHtml(PrintWriter writer) {
    getDataMetricTracker.printHtml(writer);
  }
}
