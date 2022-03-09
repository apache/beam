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

import io.cdap.cdap.etl.api.FailureCollector;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/** Helper class to incorporate Hubspot Config Validation. */
@SuppressWarnings("MissingCasesInEnumSwitch")
public class ConfigValidator {
  /**
   * Verifies if sink hubspot config only contains time period.
   *
   * @param sourceHubspotConfig the source hubspot config
   * @param failureCollector the failure collector
   */
  public static void validateTimePeriod(
      SourceHubspotConfig sourceHubspotConfig, FailureCollector failureCollector) {
    if (sourceHubspotConfig.containsMacro(SourceHubspotConfig.TIME_PERIOD)) {
      return;
    }
    try {
      TimePeriod period = sourceHubspotConfig.getTimePeriod();
      if (!sourceHubspotConfig.containsMacro(SourceHubspotConfig.REPORT_TYPE)
          && sourceHubspotConfig.getReportEndpoint().equals(ReportEndpoint.TOTALS)) {
        switch (period) {
          case MONTHLY:
          case WEEKLY:
          case DAILY:
            failureCollector
                .addFailure(
                    String.format(
                        "Time period '%s' is not valid for '%s'.",
                        sourceHubspotConfig.timePeriod, sourceHubspotConfig.reportType),
                    "Use summarized Time Periods for totals.")
                .withConfigProperty(SourceHubspotConfig.TIME_PERIOD);
        }
      }
    } catch (IllegalArgumentException e) {
      failureCollector
          .addFailure(
              String.format("Time period '%s' is not valid.", sourceHubspotConfig.timePeriod),
              "Select one of: total, daily, weekly, monthly, summarize/daily, "
                  + "summarize/weekly, summarize/monthly")
          .withConfigProperty(SourceHubspotConfig.TIME_PERIOD);
    }
  }

  static void validateFilters(
      SourceHubspotConfig sourceHubspotConfig, FailureCollector failureCollector) {
    if (sourceHubspotConfig.containsMacro(SourceHubspotConfig.FILTERS)
        || sourceHubspotConfig.containsMacro(SourceHubspotConfig.TIME_PERIOD)) {
      return;
    }
    List<String> filters = sourceHubspotConfig.getFilters();
    switch (sourceHubspotConfig.getTimePeriod()) {
      case DAILY:
      case WEEKLY:
      case MONTHLY:
        if (filters == null || filters.isEmpty()) {
          failureCollector
              .addFailure(
                  "NO filters defined.",
                  "When using daily, weekly, or monthly for the time_period,"
                      + " you must include at least one filter.")
              .withConfigProperty(SourceHubspotConfig.FILTERS);
        }
        for (String filter : filters) {
          if (filters == null || filters.isEmpty()) {
            failureCollector
                .addFailure("Filter must not be empty.", null)
                .withConfigProperty(SourceHubspotConfig.FILTERS);
          } else {
            if (!filter.matches("\\w+")) {
              failureCollector
                  .addFailure(
                      String.format("Filter '%s' is not a valid filter", filter),
                      "Filter must one word without special symbols")
                  .withConfigProperty(SourceHubspotConfig.FILTERS);
            }
          }
        }
    }
  }

  static void validateReportType(
      SourceHubspotConfig sourceHubspotConfig, FailureCollector failureCollector) {
    if (sourceHubspotConfig.containsMacro(SourceHubspotConfig.REPORT_TYPE)) {
      return;
    }
    try {
      switch (sourceHubspotConfig.getReportType()) {
        case REPORT_CATEGORY:
          if (sourceHubspotConfig.containsMacro(SourceHubspotConfig.REPORT_CATEGORY)) {
            return;
          }
          try {
            sourceHubspotConfig.getReportEndpoint(sourceHubspotConfig.reportCategory);
          } catch (IllegalArgumentException e) {
            failureCollector
                .addFailure(
                    String.format(
                        "Report Category '%s' is not valid.", sourceHubspotConfig.reportCategory),
                    null)
                .withConfigProperty(SourceHubspotConfig.REPORT_CATEGORY);
          }
          break;
        case REPORT_OBJECT:
          if (sourceHubspotConfig.containsMacro(SourceHubspotConfig.REPORT_OBJECT)) {
            return;
          }
          try {
            sourceHubspotConfig.getReportEndpoint(sourceHubspotConfig.reportObject);
          } catch (IllegalArgumentException e) {
            failureCollector
                .addFailure(
                    String.format(
                        "Report Object '%s' is not valid.", sourceHubspotConfig.reportObject),
                    null)
                .withConfigProperty(SourceHubspotConfig.REPORT_OBJECT);
          }
          break;
        case REPORT_CONTENT:
          if (sourceHubspotConfig.containsMacro(SourceHubspotConfig.REPORT_CONTENT)) {
            return;
          }
          try {
            sourceHubspotConfig.getReportEndpoint(sourceHubspotConfig.reportContent);
          } catch (IllegalArgumentException e) {
            failureCollector
                .addFailure(
                    String.format(
                        "Report Content '%s' is not valid.", sourceHubspotConfig.reportContent),
                    null)
                .withConfigProperty(SourceHubspotConfig.REPORT_CONTENT);
          }
          break;
      }
    } catch (IllegalArgumentException e) {
      failureCollector
          .addFailure(
              String.format("Report Type '%s' is not valid.", sourceHubspotConfig.reportType), null)
          .withConfigProperty(SourceHubspotConfig.REPORT_TYPE);
    }
  }

  protected static void validateObjectType(
      SourceHubspotConfig sourceHubspotConfig, FailureCollector failureCollector) {
    if (sourceHubspotConfig.containsMacro(BaseHubspotConfig.OBJECT_TYPE)) {
      return;
    }
    try {
      sourceHubspotConfig.getObjectType();
    } catch (IllegalArgumentException e) {
      failureCollector
          .addFailure(
              String.format("Object Type '%s' is not valid.", sourceHubspotConfig.objectType), null)
          .withConfigProperty(BaseHubspotConfig.OBJECT_TYPE);
    }
  }

  protected static void validateAuthorization(
      SourceHubspotConfig sourceHubspotConfig, FailureCollector failureCollector) {
    if (sourceHubspotConfig.containsMacro(SourceHubspotConfig.TIME_PERIOD)
        || sourceHubspotConfig.containsMacro(SourceHubspotConfig.FILTERS)
        || sourceHubspotConfig.containsMacro(SourceHubspotConfig.REPORT_TYPE)
        || sourceHubspotConfig.containsMacro(BaseHubspotConfig.OBJECT_TYPE)
        || sourceHubspotConfig.containsMacro(BaseHubspotConfig.API_KEY)
        || sourceHubspotConfig.containsMacro(SourceHubspotConfig.START_DATE)
        || sourceHubspotConfig.containsMacro(SourceHubspotConfig.END_DATE)) {
      return;
    }
    try {
      new HubspotHelper().getHubspotPage(sourceHubspotConfig, null);
    } catch (IOException e) {
      if (e.getMessage().toLowerCase().contains("forbidden")) {
        failureCollector
            .addFailure("Api endpoint not accessible with provided Api Key.", null)
            .withConfigProperty(BaseHubspotConfig.API_KEY);
      } else {
        failureCollector.addFailure(
            "Api endpoint not accessible with provided configuration.", null);
      }
    }
  }

  protected static void validateDateRange(
      SourceHubspotConfig sourceHubspotConfig, FailureCollector failureCollector) {
    if (sourceHubspotConfig.containsMacro(SourceHubspotConfig.START_DATE)
        || sourceHubspotConfig.containsMacro(SourceHubspotConfig.END_DATE)) {
      return;
    }
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyDDmm");
    Date startDate = null;
    Date endDate = null;
    if (sourceHubspotConfig.startDate == null || sourceHubspotConfig.startDate.isEmpty()) {
      failureCollector
          .addFailure(
              "Start Date not defined for ANALYTICS object selected.", "Use YYYYMMDD date format.")
          .withConfigProperty(SourceHubspotConfig.START_DATE);
    }
    if (sourceHubspotConfig.endDate == null || sourceHubspotConfig.endDate.isEmpty()) {
      failureCollector
          .addFailure(
              "End Date not defined for ANALYTICS object selected.", "Use YYYYMMDD date format.")
          .withConfigProperty(SourceHubspotConfig.END_DATE);
    }
    if (sourceHubspotConfig.startDate != null && sourceHubspotConfig.endDate != null) {
      try {
        startDate = simpleDateFormat.parse(sourceHubspotConfig.startDate);
      } catch (ParseException e) {
        failureCollector
            .addFailure("Invalid startDate format.", "Use YYYYMMDD date format.")
            .withConfigProperty(SourceHubspotConfig.START_DATE);
      }
      try {
        endDate = simpleDateFormat.parse(sourceHubspotConfig.endDate);
      } catch (ParseException e) {
        failureCollector
            .addFailure("Invalid endDate format.", "Use YYYYMMDD date format.")
            .withConfigProperty(SourceHubspotConfig.END_DATE);
      }
      if (startDate != null && endDate != null && startDate.after(endDate)) {
        failureCollector.addFailure("startDate must be earlier than endDate.", "Enter valid date.");
      }
    }
  }
}
