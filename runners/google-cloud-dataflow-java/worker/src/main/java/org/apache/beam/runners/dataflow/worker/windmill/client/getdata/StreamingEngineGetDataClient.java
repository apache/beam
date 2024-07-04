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

import com.google.auto.value.AutoBuilder;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.StreamingEngineWindmillClient;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.CloseableStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamPool;
import org.apache.beam.sdk.annotations.Internal;
import org.joda.time.Duration;

/**
 * StreamingEngine implementation of {@link GetDataClient}.
 *
 * @implNote Uses {@link WindmillStreamPool} to send/receive requests. Depending on options, may use
 *     a dedicated stream pool for heartbeats.
 */
@Internal
@ThreadSafe
public final class StreamingEngineGetDataClient implements GetDataClient {
  private static final Duration STREAM_TIMEOUT = Duration.standardSeconds(30);

  private final WindmillStreamPool<GetDataStream> getDataStreamPool;
  private final WindmillStreamPool<GetDataStream> heartbeatStreamPool;
  private final ThrottlingGetDataMetricTracker getDataMetricTracker;

  StreamingEngineGetDataClient(
      StreamingEngineWindmillClient windmillClient,
      ThrottlingGetDataMetricTracker getDataMetricTracker,
      boolean useSeparateHeartbeatStreams,
      int numGetDataStreams) {
    this.getDataMetricTracker = getDataMetricTracker;
    this.getDataStreamPool =
        WindmillStreamPool.create(
            Math.max(1, numGetDataStreams), STREAM_TIMEOUT, windmillClient::getDataStream);
    if (useSeparateHeartbeatStreams) {
      this.heartbeatStreamPool =
          WindmillStreamPool.create(1, STREAM_TIMEOUT, windmillClient::getDataStream);
    } else {
      this.heartbeatStreamPool = this.getDataStreamPool;
    }
  }

  public static Builder builder(
      StreamingEngineWindmillClient windmillClient,
      ThrottlingGetDataMetricTracker getDataMetricTracker) {
    return new AutoBuilder_StreamingEngineGetDataClient_Builder()
        .setWindmillClient(windmillClient)
        .setGetDataMetricTracker(getDataMetricTracker)
        .setUseSeparateHeartbeatStreams(false)
        .setNumGetDataStreams(1);
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
                ThrottlingGetDataMetricTracker.Type.STATE);
        CloseableStream<GetDataStream> closeableStream = getDataStreamPool.getCloseableStream()) {
      return closeableStream.stream().requestGlobalData(request);
    } catch (Exception e) {
      throw new GetDataException(
          "Error occurred fetching side input for tag=" + request.getDataId(), e);
    }
  }

  @Override
  public void refreshActiveWork(Map<String, List<Windmill.HeartbeatRequest>> heartbeats) {
    if (heartbeats.isEmpty()) {
      return;
    }

    try (AutoCloseable ignored =
            getDataMetricTracker.trackSingleCallWithThrottling(
                ThrottlingGetDataMetricTracker.Type.STATE);
        CloseableStream<GetDataStream> closeableStream = heartbeatStreamPool.getCloseableStream()) {
      closeableStream.stream().refreshActiveWork(heartbeats);
    } catch (Exception e) {
      throw new GetDataException("Error occurred refreshing heartbeats=" + heartbeats, e);
    }
  }

  @Override
  public void printHtml(PrintWriter writer) {
    getDataMetricTracker.printHtml(writer);
  }

  @Internal
  @AutoBuilder
  public abstract static class Builder {
    abstract Builder setWindmillClient(StreamingEngineWindmillClient windmillClient);

    abstract Builder setGetDataMetricTracker(ThrottlingGetDataMetricTracker getDataMetricTracker);

    public abstract Builder setUseSeparateHeartbeatStreams(boolean useSeparateHeartbeatStreams);

    public abstract Builder setNumGetDataStreams(int numGetDataStreams);

    abstract StreamingEngineGetDataClient autoBuild();

    public final GetDataClient build() {
      return autoBuild();
    }
  }
}
