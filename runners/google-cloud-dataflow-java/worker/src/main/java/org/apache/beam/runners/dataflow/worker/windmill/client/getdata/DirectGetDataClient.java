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

import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.WorkItemCancelledException;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.sdk.annotations.Internal;

/** {@link GetDataClient} that fetches data directly from a specific {@link GetDataStream}. */
@Internal
public final class DirectGetDataClient implements GetDataClient {

  private final GetDataStream directGetDataStream;
  private final Supplier<GetDataStream> sideInputGetDataStream;
  private final ThrottlingGetDataMetricTracker getDataMetricTracker;

  private DirectGetDataClient(
      GetDataStream directGetDataStream,
      Supplier<GetDataStream> sideInputGetDataStream,
      ThrottlingGetDataMetricTracker getDataMetricTracker) {
    this.directGetDataStream = directGetDataStream;
    this.sideInputGetDataStream = sideInputGetDataStream;
    this.getDataMetricTracker = getDataMetricTracker;
  }

  public static GetDataClient create(
      GetDataStream getDataStream,
      Supplier<GetDataStream> sideInputGetDataStream,
      ThrottlingGetDataMetricTracker getDataMetricTracker) {
    return new DirectGetDataClient(getDataStream, sideInputGetDataStream, getDataMetricTracker);
  }

  @Override
  public Windmill.KeyedGetDataResponse getStateData(
      String computation, Windmill.KeyedGetDataRequest request) {
    if (directGetDataStream.isShutdown()) {
      throw new WorkItemCancelledException(request.getShardingKey());
    }

    try (AutoCloseable ignored =
        getDataMetricTracker.trackSingleCallWithThrottling(
            ThrottlingGetDataMetricTracker.Type.STATE)) {
      return directGetDataStream.requestKeyedData(computation, request);
    } catch (Exception e) {
      if (directGetDataStream.isShutdown()) {
        throw new WorkItemCancelledException(request.getShardingKey());
      }

      throw new GetDataException(
          "Error occurred fetching state for computation="
              + computation
              + ", key="
              + request.getShardingKey(),
          e);
    }
  }

  @Override
  public Windmill.GlobalData getSideInputData(Windmill.GlobalDataRequest request) {
    GetDataStream sideInputGetDataStream = this.sideInputGetDataStream.get();
    if (sideInputGetDataStream.isShutdown()) {
      throw new GetDataException(
          "Error occurred fetching side input for tag=" + request.getDataId());
    }

    try (AutoCloseable ignored =
        getDataMetricTracker.trackSingleCallWithThrottling(
            ThrottlingGetDataMetricTracker.Type.SIDE_INPUT)) {
      return sideInputGetDataStream.requestGlobalData(request);
    } catch (Exception e) {
      throw new GetDataException(
          "Error occurred fetching side input for tag=" + request.getDataId(), e);
    }
  }
}
