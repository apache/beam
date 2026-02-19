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
package org.apache.beam.it.datadog.conditions;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.datadog.DatadogResourceManager;

/** ConditionCheck to validate if Datadog has received a certain amount of events. */
@AutoValue
public abstract class DatadogLogEntriesCheck extends ConditionCheck {

  abstract DatadogResourceManager resourceManager();

  abstract Integer minEntries();

  @Nullable
  abstract Integer maxEntries();

  @Override
  public String getDescription() {
    if (maxEntries() != null) {
      return String.format(
          "Datadog check if logs have between %d and %d events", minEntries(), maxEntries());
    }
    return String.format("Datadog check if logs have %d events", minEntries());
  }

  @Override
  public CheckResult check() {
    long totalEvents = resourceManager().getEntries().size();

    if (totalEvents < minEntries()) {
      return new CheckResult(
          false, String.format("Expected %d but has only %d", minEntries(), totalEvents));
    }
    Integer max = maxEntries();
    if (max != null && totalEvents > max) {
      return new CheckResult(
          false, String.format("Expected up to %d but found %d events", max, totalEvents));
    }

    if (max != null) {
      return new CheckResult(
          true,
          String.format(
              "Expected between %d and %d events and found %d",
              minEntries(), maxEntries(), totalEvents));
    }

    return new CheckResult(
        true, String.format("Expected at least %d events and found %d", minEntries(), totalEvents));
  }

  public static Builder builder(DatadogResourceManager resourceManager) {
    return new AutoValue_DatadogLogEntriesCheck.Builder().setResourceManager(resourceManager);
  }

  /** Builder for {@link DatadogLogEntriesCheck}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setResourceManager(DatadogResourceManager resourceManager);

    public abstract Builder setMinEntries(Integer minEvents);

    public abstract Builder setMaxEntries(Integer maxEvents);

    abstract DatadogLogEntriesCheck autoBuild();

    public DatadogLogEntriesCheck build() {
      return autoBuild();
    }
  }
}
