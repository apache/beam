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
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.CloseableStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamPool;
import org.apache.beam.sdk.annotations.Internal;

/**
 * StreamingEngine implementation of {@link GetDataClient}.
 *
 * @implNote Uses {@link WindmillStreamPool} to send requests.
 */
@Internal
@ThreadSafe
public final class StreamPoolGetDataClient implements GetDataClient {

  private final WindmillStreamPool<GetDataStream> getDataStreamPool;
  private final ThrottlingGetDataMetricTracker getDataMetricTracker;

  public StreamPoolGetDataClient(
      ThrottlingGetDataMetricTracker getDataMetricTracker,
      WindmillStreamPool<GetDataStream> getDataStreamPool) {
    this.getDataMetricTracker = getDataMetricTracker;
    this.getDataStreamPool = getDataStreamPool;
  }

  @Override
  public Windmill.KeyedGetDataResponse getStateData(
      String computationId, KeyedGetDataRequest request) {
    try (AutoCloseable ignored = getDataMetricTracker.trackStateDataFetchWithThrottling();
        CloseableStream<GetDataStream> closeableStream = getDataStreamPool.getCloseableStream()) {
      return closeableStream.stream().requestKeyedData(computationId, request);
    } catch (Exception e) {
      throw new GetDataException(
          "Error occurred fetching state for computation="
              + computationId
              + ", key="
              + request.getShardingKey(),
          e);
    }
  }

  @Override
  public Windmill.GlobalData getSideInputData(GlobalDataRequest request) {
    try (AutoCloseable ignored = getDataMetricTracker.trackSideInputFetchWithThrottling();
        CloseableStream<GetDataStream> closeableStream = getDataStreamPool.getCloseableStream()) {
      return closeableStream.stream().requestGlobalData(request);
    } catch (Exception e) {
      throw new GetDataException(
          "Error occurred fetching side input for tag=" + request.getDataId(), e);
    }
  }

  @Override
  public void printHtml(PrintWriter writer) {
    getDataMetricTracker.printHtml(writer);
  }
}
