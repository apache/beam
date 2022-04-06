/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.beam.sdk.io.cdap.hubspot.source.streaming;

import java.util.Arrays;

/**
 * Frequency of API polling by streaming receiver.
 */
public enum PullFrequency {
  MINUTES_15("15 min", 15),
  MINUTES_30("30 min", 30),
  HOUR_1("1 hour", 60),
  HOUR_4("4 hour", 4 * 60),
  HOUR_8("8 hour", 8 * 60);

  private final String name;
  private final Integer minutesValue;

  PullFrequency(String name, Integer minutesValue) {
    this.name = name;
    this.minutesValue = minutesValue;
  }

  public String getName() {
    return name;
  }

  public Integer getMinutesValue() {
    return minutesValue;
  }

  /**
   * Returns the PullFrequency.
   * @param name the name is string type
   * @return the PullFrequency
   */
  public static PullFrequency fromValue(String name) {
    return Arrays.stream(PullFrequency.values())
      .filter(pullFrequency -> pullFrequency.getName().equals(name))
      .findAny()
      .orElseThrow(() -> new RuntimeException(String.format("Unexpected pull frequency value '%s'.", name)));
  }
}
