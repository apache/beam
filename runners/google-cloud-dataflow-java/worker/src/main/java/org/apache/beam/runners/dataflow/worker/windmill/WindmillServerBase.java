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
import java.util.Set;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemReceiver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;

/**
 * Implementation of a WindmillServerStub which communicates with a Windmill appliance server.
 *
 * @implNote This is only for use in Streaming Appliance. Please do not change the name or path of
 *     this class as this will break JNI.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class WindmillServerBase extends WindmillServerStub {

  /** Pointer to the underlying native windmill client object. */
  private final long nativePointer;

  protected WindmillServerBase(String host) {
    this.nativePointer = create(host);
  }

  @Override
  protected void finalize() {
    destroy(nativePointer);
  }

  @Override
  public void setWindmillServiceEndpoints(Set<HostAndPort> endpoints) {
    // This class is used for windmill appliance and local runner tests.
  }

  @Override
  public ImmutableSet<HostAndPort> getWindmillServiceEndpoints() {
    return ImmutableSet.of();
  }

  @Override
  public Windmill.GetWorkResponse getWork(Windmill.GetWorkRequest workRequest) {
    try {
      byte[] requestBytes = workRequest.toByteArray();
      return Windmill.GetWorkResponse.newBuilder()
          .mergeFrom(getWorkImpl(nativePointer, requestBytes, requestBytes.length))
          .build();
    } catch (IOException e) {
      throw new RuntimeException("Proto deserialization failed: " + e);
    }
  }

  @Override
  public Windmill.GetDataResponse getData(Windmill.GetDataRequest dataRequest) {
    try {
      byte[] requestBytes = dataRequest.toByteArray();
      return Windmill.GetDataResponse.newBuilder()
          .mergeFrom(getDataImpl(nativePointer, requestBytes, requestBytes.length))
          .build();
    } catch (IOException e) {
      throw new RuntimeException("Proto deserialization failed: " + e);
    }
  }

  @Override
  public Windmill.CommitWorkResponse commitWork(Windmill.CommitWorkRequest commitRequest) {
    try {
      byte[] requestBytes = commitRequest.toByteArray();
      return Windmill.CommitWorkResponse.newBuilder()
          .mergeFrom(commitWorkImpl(nativePointer, requestBytes, requestBytes.length))
          .build();
    } catch (IOException e) {
      throw new RuntimeException("Proto deserialization failed: " + e);
    }
  }

  @Override
  public Windmill.GetConfigResponse getConfig(Windmill.GetConfigRequest configRequest) {
    try {
      byte[] requestBytes = configRequest.toByteArray();
      return Windmill.GetConfigResponse.newBuilder()
          .mergeFrom(getConfigImpl(nativePointer, requestBytes, requestBytes.length))
          .build();
    } catch (IOException e) {
      throw new RuntimeException("Proto deserialization failed: " + e);
    }
  }

  @Override
  public Windmill.ReportStatsResponse reportStats(Windmill.ReportStatsRequest statsRequest) {
    try {
      byte[] requestBytes = statsRequest.toByteArray();
      return Windmill.ReportStatsResponse.newBuilder()
          .mergeFrom(reportStatsImpl(nativePointer, requestBytes, requestBytes.length))
          .build();
    } catch (IOException e) {
      throw new RuntimeException("Proto deserialization failed: " + e);
    }
  }

  @Override
  public long getAndResetThrottleTime() {
    // Windmill appliance does not have throttling.
    return 0;
  }

  @Override
  public GetWorkStream getWorkStream(Windmill.GetWorkRequest request, WorkItemReceiver receiver) {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetDataStream getDataStream() {
    throw new UnsupportedOperationException();
  }

  @Override
  public CommitWorkStream commitWorkStream() {
    throw new UnsupportedOperationException();
  }

  /** Native methods for interacting with the underlying native windmill client code. */
  private native long create(String host);

  private native void destroy(long nativePointer);

  private native byte[] getWorkImpl(long nativePointer, byte[] workRequest, int requestSize);

  private native byte[] getDataImpl(long nativePointer, byte[] dataRequest, int requestSize);

  private native byte[] commitWorkImpl(long nativePointer, byte[] commitRequest, int requestSize);

  private native byte[] getConfigImpl(long nativePointer, byte[] configRequest, int requestSize);

  private native byte[] reportStatsImpl(
      long nativePointer, byte[] reportStatsRequest, int requestSize);
}
