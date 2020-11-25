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

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.model.pipeline.v1.MetricsApi;
import org.apache.beam.runners.core.metrics.MonitoringInfoMetricName;
import org.apache.beam.sdk.metrics.MetricsContainer;

public class MonitoringInfoShortIdCache implements AutoCloseable {

  private int lastShortId = 0;

  private HashMap<MonitoringInfoMetricName, String> infoKeyToShortId = new HashMap<>();

  private HashMap<String, MetricsApi.MonitoringInfo> shortIdToInfo = new HashMap<>();

  private static final AtomicReference<MonitoringInfoShortIdCache> SHORT_ID_CACHE =
      new AtomicReference<>(new MonitoringInfoShortIdCache());

  private MonitoringInfoShortIdCache() {}

  /** Return the {@link MetricsContainer} for the current process. */
  public static MonitoringInfoShortIdCache getShortIdCache() {
    return SHORT_ID_CACHE.get();
  }

  /**
   * Initialize and assign the {@link MonitoringInfoShortIdCache} to the current process. Usage: try
   * (MonitoringInfoShortIdCache cache = MonitoringInfoShortIdCache.newShortIdCache()) { ; } catch
   * (Exception e) { // Handle error when AutoClosure calls close() }
   *
   * <p>Instead of setting this statically, we require that it be initialized. This allows tests to
   * set and clear the ShortIdCache appropriately.
   *
   * @return The ShortIdCache for the current process.
   */
  public static MonitoringInfoShortIdCache newShortIdCache() {
    return SHORT_ID_CACHE.getAndSet(new MonitoringInfoShortIdCache());
  }

  @Override
  public void close() throws Exception {
    SHORT_ID_CACHE.getAndSet(null);
  }

  public String getShortId(MetricsApi.MonitoringInfo monitoringInfo) {
    MonitoringInfoMetricName infoKey = MonitoringInfoMetricName.of(monitoringInfo);
    synchronized (this) {
      String shortId = infoKeyToShortId.get(infoKey);
      if (shortId == null) {
        MetricsApi.MonitoringInfo.Builder builder =
            MetricsApi.MonitoringInfo.newBuilder(monitoringInfo);
        builder.clearPayload();
        lastShortId += 1;
        shortId = Integer.toHexString(lastShortId);
        infoKeyToShortId.put(infoKey, shortId);
        shortIdToInfo.put(shortId, builder.build());
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
