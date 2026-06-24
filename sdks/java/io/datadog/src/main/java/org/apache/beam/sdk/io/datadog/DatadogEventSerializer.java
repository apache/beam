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
package org.apache.beam.sdk.io.datadog;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class DatadogEventSerializer {
  private static final Gson GSON =
      new GsonBuilder().setFieldNamingStrategy(f -> f.getName().toLowerCase()).create();

  private DatadogEventSerializer() {}

  /** Utility method to get payload string from a list of {@link DatadogEvent}s. */
  public static String getPayloadString(List<DatadogEvent> events) {
    return GSON.toJson(events);
  }

  /** Utility method to get payload string from a {@link DatadogEvent}. */
  public static String getPayloadString(DatadogEvent event) {
    return GSON.toJson(event);
  }

  /** Utility method to get payload size from a string. */
  public static long getPayloadSize(String payload) {
    return payload.getBytes(StandardCharsets.UTF_8).length;
  }
}
