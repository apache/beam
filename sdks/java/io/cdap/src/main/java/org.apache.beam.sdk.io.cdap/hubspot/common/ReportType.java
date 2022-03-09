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
package org.apache.beam.sdk.io.cdap.hubspot.common;

import java.util.Arrays;

/**
 * Convenience enum to map Report Type UI selections to meaningful values. Must be one of: The
 * analytics report type of content that you want to get data for. The analytics report category
 * used to break down the analytics data. The analytics report type of object that you want the
 * analytics data for.
 */
@SuppressWarnings("ImmutableEnumChecker")
public enum ReportType {
  REPORT_CONTENT("Content"),
  REPORT_CATEGORY("Category"),
  REPORT_OBJECT("Object");

  private String stringValue;

  ReportType(String stringValue) {
    this.stringValue = stringValue;
  }

  /**
   * Returns the ReportType.
   *
   * @param value the value is string type
   * @return the ReportType
   */
  public static ReportType fromString(String value) {
    return Arrays.stream(ReportType.values())
        .filter(type -> type.stringValue.equals(value))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(String.format("'%s' is invalid ObjectType.", value)));
  }

  public String getStringValue() {
    return stringValue;
  }
}
