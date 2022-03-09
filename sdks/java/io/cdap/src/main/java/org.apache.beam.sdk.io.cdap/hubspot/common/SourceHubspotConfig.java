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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/** Provides source configuration for accessing Hubspot API. */
@SuppressWarnings("rawtypes")
public class SourceHubspotConfig extends BaseHubspotConfig {

  public static final String TIME_PERIOD = "timePeriod";
  public static final String REPORT_TYPE = "reportType";
  public static final String REPORT_CONTENT = "reportContent";
  public static final String REPORT_CATEGORY = "reportCategory";
  public static final String START_DATE = "startDate";
  public static final String END_DATE = "endDate";
  public static final String FILTERS = "filters";
  public static final String REPORT_OBJECT = "reportObject";

  @Name(FILTERS)
  @Description(
      "Keyword to filter the analytics report data to include only the specified breakdowns.")
  @Macro
  @Nullable
  public String filters;

  @Name(START_DATE)
  @Description("Start date for the analytics report data. YYYYMMDD format.")
  @Macro
  @Nullable
  public String startDate;

  @Name(END_DATE)
  @Description("End date for the analytics report data. YYYYMMDD format.")
  @Macro
  @Nullable
  public String endDate;

  @Name(REPORT_TYPE)
  @Description("Analytics report target to get data for.")
  @Macro
  @Nullable
  public String reportType;

  @Name(REPORT_CONTENT)
  @Description("Analytics report type of content that you want to get data for.")
  @Macro
  @Nullable
  public String reportContent;

  @Name(REPORT_CATEGORY)
  @Description("Analytics report category used to break down the analytics data.")
  @Macro
  @Nullable
  public String reportCategory;

  @Name(REPORT_OBJECT)
  @Description("Analytics report type of object that you want the analytics data for.")
  @Macro
  @Nullable
  public String reportObject;

  @Name(TIME_PERIOD)
  @Description("Time period used to group the data.")
  @Macro
  @Nullable
  public String timePeriod;

  public SourceHubspotConfig(String referenceName) {
    super(referenceName);
  }

  /**
   * Constructor for SourceHubspotConfig object.
   *
   * @param referenceName the reference name
   * @param apiServerUrl the api server url of hubspot
   * @param objectType the object type of hubspot
   * @param apiKey the api key of hubspot
   * @param filters the filters for Analytics object type
   * @param startDate the start date for Analytics object type
   * @param endDate the end date for Analytics object type
   * @param reportType the report type for Analytics object type
   * @param reportContent the report content
   * @param reportCategory the report category
   * @param reportObject the report object
   * @param timePeriod the time period for Analytics object type
   */
  public SourceHubspotConfig(
      String referenceName,
      String apiServerUrl,
      String objectType,
      String apiKey,
      String filters,
      String startDate,
      String endDate,
      String reportType,
      String reportContent,
      String reportCategory,
      String reportObject,
      String timePeriod) {
    super(referenceName, apiServerUrl, objectType, apiKey);
    this.filters = filters;
    this.startDate = startDate;
    this.endDate = endDate;
    this.reportType = reportType;
    this.reportContent = reportContent;
    this.reportCategory = reportCategory;
    this.reportObject = reportObject;
    this.timePeriod = timePeriod;
  }

  /** Validates {@link SourceHubspotConfig} instance. */
  public void validate(FailureCollector failureCollector) {
    ConfigValidator.validateObjectType(this, failureCollector);
    if (!containsMacro(OBJECT_TYPE) && getObjectType().equals(ObjectType.ANALYTICS)) {
      ConfigValidator.validateReportType(this, failureCollector);
      ConfigValidator.validateTimePeriod(this, failureCollector);
      ConfigValidator.validateFilters(this, failureCollector);
      ConfigValidator.validateDateRange(this, failureCollector);
    }
    ConfigValidator.validateAuthorization(this, failureCollector);
  }

  /**
   * Returns the Schema.
   *
   * @return the Schema
   */
  public Schema getSchema() {
    Set<Schema.Field> schemaFields = new HashSet<>();
    schemaFields.add(
        Schema.Field.of("objectType", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    schemaFields.add(Schema.Field.of("object", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    return Schema.recordOf("etlSchemaBody", schemaFields);
  }

  /**
   * Returns the ReportEndpoint.
   *
   * @return the ReportEndpoint
   */
  @Nullable
  public ReportEndpoint getReportEndpoint() {
    switch (getReportType()) {
      case REPORT_CATEGORY:
        return getReportEndpoint(reportCategory);
      case REPORT_OBJECT:
        return getReportEndpoint(reportObject);
      case REPORT_CONTENT:
        return getReportEndpoint(reportContent);
      default:
        throw new IllegalArgumentException(
            String.format("'%s' is invalid ObjectType.", reportType));
    }
  }

  @Nullable
  public ReportEndpoint getReportEndpoint(String reportCategory) {
    return ReportEndpoint.fromString(reportCategory);
  }

  @Nullable
  public ReportType getReportType() {
    return ReportType.fromString(reportType);
  }

  @Nullable
  public TimePeriod getTimePeriod() {
    return TimePeriod.fromString(timePeriod);
  }

  /**
   * Returns the list of string.
   *
   * @return the list of string
   */
  @Nullable
  public List<String> getFilters() {
    List<String> list = new ArrayList();
    if (filters != null && !filters.isEmpty()) {
      list.addAll(Arrays.asList(filters.split(",")));
    }
    return list;
  }
}
