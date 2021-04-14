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

import java.util.NoSuchElementException;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashBiMap;

/** A Class for registering short ids for MonitoringInfos. */
public class ShortIdMap {
  private int counter = 0;
  private BiMap<String, MonitoringInfo> monitoringInfoMap = HashBiMap.create();

  public synchronized String getOrCreateShortId(MonitoringInfo info) {
    Preconditions.checkNotNull(info);
    String shortId = monitoringInfoMap.inverse().get(info);
    if (shortId == null) {
      shortId = "metric" + counter++;
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
