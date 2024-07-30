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
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.WorkItemCancelledException;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.AbstractWindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.sdk.annotations.Internal;

/** {@link GetDataClient} that fetches data directly from a specific {@link GetDataStream}. */
@Internal
public final class StreamGetDataClient implements GetDataClient {

  private final GetDataStream getDataStream;
  private final Function<String, GetDataStream> sideInputGetDataStreamFactory;
  private final ThrottlingGetDataMetricTracker getDataMetricTracker;

  private StreamGetDataClient(
      GetDataStream getDataStream,
      Function<String, GetDataStream> sideInputGetDataStreamFactory,
      ThrottlingGetDataMetricTracker getDataMetricTracker) {
    this.getDataStream = getDataStream;
    this.sideInputGetDataStreamFactory = sideInputGetDataStreamFactory;
    this.getDataMetricTracker = getDataMetricTracker;
  }

  public static GetDataClient create(
      GetDataStream getDataStream,
      Function<String, GetDataStream> sideInputGetDataStreamFactory,
      ThrottlingGetDataMetricTracker getDataMetricTracker) {
    return new StreamGetDataClient(
        getDataStream, sideInputGetDataStreamFactory, getDataMetricTracker);
  }

  /**
   * @throws WorkItemCancelledException when the fetch fails due to the stream being shutdown,
   *     indicating that the {@link
   *     org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem} that triggered the
   *     fetch has been cancelled.
   */
  @Override
  public Windmill.KeyedGetDataResponse getStateData(
      String computationId, Windmill.KeyedGetDataRequest request) throws GetDataException {
    try (AutoCloseable ignored = getDataMetricTracker.trackStateDataFetchWithThrottling()) {
      return getDataStream.requestKeyedData(computationId, request);
    } catch (AbstractWindmillStream.WindmillStreamShutdownException e) {
      throw new WorkItemCancelledException(request.getShardingKey());
    } catch (Exception e) {
      throw new GetDataException(
          "Error occurred fetching state for computation="
              + computationId
              + ", key="
              + request.getShardingKey(),
          e);
    }
  }

  /**
   * @throws WorkItemCancelledException when the fetch fails due to the stream being shutdown,
   *     indicating that the {@link
   *     org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem} that triggered the
   *     fetch has been cancelled.
   */
  @Override
  public Windmill.GlobalData getSideInputData(Windmill.GlobalDataRequest request)
      throws GetDataException {
    GetDataStream sideInputGetDataStream =
        sideInputGetDataStreamFactory.apply(request.getDataId().getTag());
    try (AutoCloseable ignored = getDataMetricTracker.trackSideInputFetchWithThrottling()) {
      return sideInputGetDataStream.requestGlobalData(request);
    } catch (AbstractWindmillStream.WindmillStreamShutdownException e) {
      throw new WorkItemCancelledException(e);
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
