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
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

/** A Class for registering short ids for MonitoringInfos. */
public class ShortIdMap {
  private int counter = 0;
  // We must use two maps instead of a BiMap because the two maps store different MonitoringInfos.
  // The values in shortIdToInfo contain the MonitoringInfo with only the payload cleared.
  private Map<String, MonitoringInfo> shortIdToInfo = new HashMap<>();
  // The keys in cleanedInfoToShortId contain a cleaned MonitoringInfo after clearing
  // the payload, startTime and type.
  private Map<MonitoringInfo, String> cleanedInfoToShortId = new HashMap<>();

  public synchronized String getOrCreateShortId(MonitoringInfo info) {
    Preconditions.checkNotNull(info);
    // Remove the payload, startTime and typeUrn before using the MonitoringInfo as a key.
    // As only the URN+labels uniquely identify the MonitoringInfo
    MonitoringInfo.Builder cleaner = MonitoringInfo.newBuilder(info);
    cleaner.clearPayload();
    cleaner.clearStartTime();
    cleaner.clearType();
    MonitoringInfo cleaned = cleaner.build();
    String shortId = cleanedInfoToShortId.get(cleaned);
    if (shortId == null) {
      shortId = "metric" + counter++;
      // Make sure we don't clean the type or startTime when we store it
      // in the shortIdToInfo map because we still need to be able to look this information up.
      MonitoringInfo.Builder noPayloadCleaner = MonitoringInfo.newBuilder(info);
      noPayloadCleaner.clearPayload();
      shortIdToInfo.put(shortId, noPayloadCleaner.build());
      cleanedInfoToShortId.put(cleaned, shortId);
    }
    return shortId;
  }

  public synchronized MonitoringInfo get(String shortId) {
    MonitoringInfo monitoringInfo = shortIdToInfo.get(shortId);
    if (monitoringInfo == null) {
      throw new NoSuchElementException(shortId);
    }
    return monitoringInfo;
  }
}
