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
package org.apache.beam.fn.harness.control;

import org.apache.beam.model.pipeline.v1.MetricsApi;
import org.apache.beam.runners.core.metrics.MonitoringInfoMetricName;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.InvalidProtocolBufferException;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

// TODO rename to MonitoringInfoShortIdCache
public class ShortIdCache {

  private int lastShortId = 0;

  private HashMap<MonitoringInfoMetricName, String> infoKeyToShortId = new HashMap<>();

  // TODO use some sort of concurrent map?
  // OR shorter synchronized blocks?
  private HashMap<String, MetricsApi.MonitoringInfo> shortIdToInfo = new HashMap<>();

  private static final AtomicReference<ShortIdCache> SHORT_ID_CACHE =
      new AtomicReference<>(new ShortIdCache());

  private ShortIdCache() {}

  /** Return the {@link MetricsContainer} for the current process. */
  public static ShortIdCache getShortIdCache() {
    return SHORT_ID_CACHE.get();
  }

  /**
   * Initialize the {@link ShortIdCache} for the current process.
   * Instead of setting this statically, we require that it be initialized.
   * This allows tests to set and clear the ShortIdCache appropriately.
   *
   * @return The ShortIdCache for the current process.
   */
  public static ShortIdCache initializeShortIdCache() {
    return SHORT_ID_CACHE.getAndSet(new ShortIdCache());
  }

  public String getShortId(MetricsApi.MonitoringInfo monitoring_info) {
    MonitoringInfoMetricName infoKey = MonitoringInfoMetricName.of(monitoring_info);
    synchronized (this) {
      String shortId = infoKeyToShortId.get(infoKey);
      if (shortId == null) {
        MetricsApi.MonitoringInfo payloadCleared;
        try {
          payloadCleared = MetricsApi.MonitoringInfo.parseFrom(
            monitoring_info.toByteArray());
          payloadCleared = payloadCleared.toBuilder().clearPayload().build();
        } catch (InvalidProtocolBufferException e) {
          // TODO use logger here.
          e.printStackTrace();
          return "";
        }
        lastShortId += 1;
        shortId = Integer.toHexString(lastShortId);
        infoKeyToShortId.put(infoKey, shortId);
        shortIdToInfo.put(shortId, payloadCleared);
      }
      return shortId;
    }
  }

  public synchronized HashMap<String, MetricsApi.MonitoringInfo> getInfos(
      Iterable<String> shortIds) {
    HashMap<String, MetricsApi.MonitoringInfo> infos = new HashMap<>();
    for (String shortId : shortIds) {
      MetricsApi.MonitoringInfo info = shortIdToInfo.get(shortId);
      if (info != null) {
        infos.put(shortId, info);
      }
    }
    return infos;
  }

}
