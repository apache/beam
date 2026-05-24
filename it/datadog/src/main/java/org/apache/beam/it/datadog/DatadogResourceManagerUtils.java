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
package org.apache.beam.it.datadog;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Utilities for {@link DatadogResourceManager} implementations. */
public final class DatadogResourceManagerUtils {

  // Datadog event metadata keys
  private static final String DD_SOURCE_KEY = "ddsource";
  private static final String DD_TAGS_KEY = "ddtags";
  private static final String DD_HOSTNAME_KEY = "hostname";
  private static final String DD_SERVICE_KEY = "service";
  private static final String DD_MESSAGE_KEY = "message";

  private DatadogResourceManagerUtils() {}

  public static Map<String, Object> datadogEntryToMap(DatadogLogEntry entry) {
    Map<String, Object> eventMap = new HashMap<>();
    Optional.ofNullable(entry.ddsource()).ifPresent(v -> eventMap.put(DD_SOURCE_KEY, v));
    Optional.ofNullable(entry.ddtags()).ifPresent(v -> eventMap.put(DD_TAGS_KEY, v));
    Optional.ofNullable(entry.hostname()).ifPresent(v -> eventMap.put(DD_HOSTNAME_KEY, v));
    Optional.ofNullable(entry.service()).ifPresent(v -> eventMap.put(DD_SERVICE_KEY, v));
    Optional.ofNullable(entry.message()).ifPresent(v -> eventMap.put(DD_MESSAGE_KEY, v));
    return eventMap;
  }

  /**
   * Generates a secure, valid Datadog API key.
   *
   * @return The generated password.
   */
  static String generateApiKey() {
    String uuid = UUID.randomUUID().toString();
    char[] chars = uuid.toCharArray();
    for (int i = 0; i < chars.length; i++) {
      if (Character.isLetter(chars[i])) {
        chars[i] = Character.toUpperCase(chars[i]);
        break;
      }
    }
    String result = new String(chars);

    // In the rare case a UUID has only one letter, it will now be uppercase.
    // The test requires a lowercase letter, so we add one if missing.
    if (!result.matches(".*[a-z].*")) {
      return result + "a";
    }
    return result;
  }
}
