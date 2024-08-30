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
package org.apache.beam.fn.harness.logging;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.slf4j.MDC;

/**
 * Adds MDC key:value pairs for a quota event. The contents of the MDC are added to each log entry.
 *
 * <p>Suggested usage: try (QuotaEventCloseable qec = new
 * QuotaEvent.Builder().withOperation(op).withFullResourceName(name).create()) { LOG.info(...); }
 *
 * <p>MDC data is thread-local. After calling QuotaEventClosable.close, any overwritten
 * quota_event.* keys will not be restored.
 */
public class QuotaEvent {
  public static class QuotaEventCloseable implements AutoCloseable {
    public QuotaEventCloseable(Map<String, String> data) {
      this.data = data;
    }

    @Override
    public void close() {
      if (!enabled) {
        return;
      }
      for (String key : data.keySet()) {
        MDC.remove(key);
      }
    }

    private final Map<String, String> data;
  }

  public static class Builder {
    /** @param messageText Any additional description for the event. */
    public Builder withMessageText(String messageText) {
      data.put("quota_event.message_text", messageText);
      return this;
    }

    /**
     * @param fullResourceName The full path for the resource involved. For GCP, this would follow
     *     https://google.aip.dev/122#full-resource-names.
     */
    public Builder withFullResourceName(String fullResourceName) {
      data.put("quota_event.full_resource_name", fullResourceName);
      return this;
    }

    /**
     * @param quotaName The full quota name. For GCP, this would look like:
     *     example.googleapis.com/quota/name
     */
    public Builder withQuotaName(String quotaName) {
      data.put("quota_event.quota_name", quotaName);
      return this;
    }

    /** @param operation Name of the operation that generated this quota event, in snake-case. */
    public Builder withOperation(String operation) {
      data.put("quota_event.operation", operation);
      return this;
    }

    /**
     * Puts the given values in the MDC as a quota event.
     *
     * @return An AutoCloseable that removes the added values from the MDC, or null if the feature
     *     is disabled.
     */
    public @Nullable QuotaEventCloseable create() {
      if (!enabled) {
        return null;
      }

      data.forEach(MDC::put);
      return new QuotaEventCloseable(data);
    }

    private final Map<String, String> data = new HashMap<>();
  }

  /** Set whether this feature is enabled. Affects Builder.create(). */
  public static void setEnabled(boolean enabled) {
    QuotaEvent.enabled = enabled;
  }

  private static boolean enabled = false;
}
