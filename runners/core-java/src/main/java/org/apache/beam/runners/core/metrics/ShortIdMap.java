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
package org.apache.beam.runners.core.metrics;

import java.util.HashMap;
import java.util.NoSuchElementException;
import org.apache.beam.model.pipeline.v1.MetricsApi;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashBiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A Class for registering short ids for MonitoringInfos. */
public class ShortIdMap {
  private static final Logger LOG = LoggerFactory.getLogger(ShortIdMap.class);

  private int counter = 0;
  // Bidirectional map.
  private BiMap<String, MonitoringInfo> monitoringInfoMap = HashBiMap.create();

  public synchronized String getOrCreateShortId(MonitoringInfo info) {
    Preconditions.checkNotNull(info);
    // Remove the payload and startTime before using the MonitoringInfo as a key.
    // AS only the URN+labels uniquely identify the MonitoringInfo
    MonitoringInfo.Builder cleaner = MonitoringInfo.newBuilder(info);
    cleaner.clearPayload();
    cleaner.clearStartTime();
    MonitoringInfo cleaned = cleaner.build();
    LOG.info("original MontitoringInfo " + info);
    LOG.info("cleaned MontitoringInfo " + cleaned);
    String shortId = monitoringInfoMap.inverse().get(cleaned);
    if (shortId == null) {
      shortId = "metric" + counter++;
      LOG.info("Assign new short ID: " + shortId + " cleaned MonitoringInfo " + cleaned);
      monitoringInfoMap.put(shortId, info);
    }
    return shortId;
  }

  public synchronized MonitoringInfo get(String shortId) {
    MonitoringInfo monitoringInfo = monitoringInfoMap.get(shortId);
    if (monitoringInfo == null) {
      throw new NoSuchElementException(shortId);
    }
    return monitoringInfo;
  }
}
