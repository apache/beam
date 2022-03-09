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
package org.apache.beam.sdk.io.cdap.zendesk.batch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.cdap.zendesk.batch.http.ConnectionTimeoutException;
import org.apache.beam.sdk.io.cdap.zendesk.batch.http.PagedIterator;
import org.apache.beam.sdk.io.cdap.zendesk.common.ObjectType;
import org.apache.beam.sdk.io.cdap.zendesk.common.config.BaseZendeskSourceConfig;

/**
 * This class {@link ZendeskBatchSourceConfig} provides all the configuration required for
 * configuring the {@link ZendeskBatchSource} plugin.
 */
public class ZendeskBatchSourceConfig extends BaseZendeskSourceConfig {

  public static final String PROPERTY_START_DATE = "startDate";
  public static final String PROPERTY_END_DATE = "endDate";
  public static final String PROPERTY_SATISFACTION_RATINGS_SCORE = "satisfactionRatingsScore";
  public static final String PROPERTY_MAX_RETRY_COUNT = "maxRetryCount";
  public static final String PROPERTY_MAX_RETRY_WAIT = "maxRetryWait";
  public static final String PROPERTY_MAX_RETRY_JITTER_WAIT = "maxRetryJitterWait";
  public static final String PROPERTY_CONNECT_TIMEOUT = "connectTimeout";
  public static final String PROPERTY_READ_TIMEOUT = "readTimeout";
  public static final String PROPERTY_URL = "zendeskBaseUrl";
  public static final String PROPERTY_SCHEMA = "schema";

  @Name(PROPERTY_START_DATE)
  @Description(
      "Filter data to include only records which have Zendesk modified date "
          + "is greater than or equal to the specified date.")
  @Nullable
  @Macro
  private String startDate;

  @Name(PROPERTY_END_DATE)
  @Description(
      "Filter data to include only records which have Zendesk modified date "
          + "is less than the specified date.")
  @Nullable
  @Macro
  private String endDate;

  @Name(PROPERTY_SATISFACTION_RATINGS_SCORE)
  @Description(
      "Filter Satisfaction Ratings object to include only records "
          + "which have Zendesk score equal to the specified score.")
  @Nullable
  @Macro
  private String satisfactionRatingsScore;

  @Name(PROPERTY_MAX_RETRY_COUNT)
  @Description("Maximum number of retry attempts.")
  @Macro
  private Integer maxRetryCount;

  @Name(PROPERTY_MAX_RETRY_WAIT)
  @Description("Maximum time in seconds retries can take.")
  @Macro
  private Integer maxRetryWait;

  @Name(PROPERTY_MAX_RETRY_JITTER_WAIT)
  @Description("Maximum time in milliseconds added to retries.")
  @Macro
  private Integer maxRetryJitterWait;

  @Name(PROPERTY_CONNECT_TIMEOUT)
  @Description("Maximum time in seconds connection initialization can take.")
  @Macro
  private Integer connectTimeout;

  @Name(PROPERTY_READ_TIMEOUT)
  @Description("Maximum time in seconds fetching data from the server can take.")
  @Macro
  private Integer readTimeout;

  @Name(PROPERTY_URL)
  @Description("Zendesk base url.")
  @Macro
  private String zendeskBaseUrl;

  @Name(PROPERTY_SCHEMA)
  @Nullable
  @Description("Output schema for the source.")
  private String schema;

  /**
   * Constructor for ZendeskBatchSourceConfig object.
   *
   * @param referenceName The reference name
   * @param adminEmail Zendesk admin email
   * @param apiToken Zendesk API token
   * @param subdomains The list of sub-domains
   * @param objectsToPull The list of objects to pull
   * @param objectsToSkip The list of objects to skip
   * @param startDate The start date
   * @param endDate The end date
   * @param satisfactionRatingsScore The satisfaction ratings score
   * @param maxRetryCount The max retry count
   * @param maxRetryWait The max retry wait time
   * @param maxRetryJitterWait The max retry jitter wait time
   * @param connectTimeout The connection timeout
   * @param readTimeout The read time out
   * @param zendeskBaseUrl Zendesk base url
   * @param schema the schema
   */
  public ZendeskBatchSourceConfig(
      String referenceName,
      String adminEmail,
      String apiToken,
      String subdomains,
      @Nullable String objectsToPull,
      @Nullable String objectsToSkip,
      @Nullable String startDate,
      @Nullable String endDate,
      @Nullable String satisfactionRatingsScore,
      Integer maxRetryCount,
      Integer maxRetryWait,
      Integer maxRetryJitterWait,
      Integer connectTimeout,
      Integer readTimeout,
      String zendeskBaseUrl,
      @Nullable String schema) {
    super(referenceName, adminEmail, apiToken, subdomains, objectsToPull, objectsToSkip);
    this.startDate = startDate;
    this.endDate = endDate;
    this.satisfactionRatingsScore = satisfactionRatingsScore;
    this.maxRetryCount = maxRetryCount;
    this.maxRetryWait = maxRetryWait;
    this.maxRetryJitterWait = maxRetryJitterWait;
    this.connectTimeout = connectTimeout;
    this.readTimeout = readTimeout;
    this.zendeskBaseUrl = zendeskBaseUrl;
    this.schema = schema;
  }

  @Nullable
  public String getStartDate() {
    return startDate;
  }

  @Nullable
  public String getEndDate() {
    return endDate;
  }

  @Nullable
  public String getSatisfactionRatingsScore() {
    return satisfactionRatingsScore;
  }

  public Integer getMaxRetryCount() {
    return maxRetryCount;
  }

  public Integer getMaxRetryWait() {
    return maxRetryWait;
  }

  public Integer getMaxRetryJitterWait() {
    return maxRetryJitterWait;
  }

  public Integer getConnectTimeout() {
    return connectTimeout;
  }

  public Integer getReadTimeout() {
    return readTimeout;
  }

  public String getZendeskBaseUrl() {
    return zendeskBaseUrl;
  }

  /**
   * Fetches the schema for the selected single object to pull.
   *
   * @param collector The failure collector to collect the errors
   * @return
   */
  public Schema getSchema(FailureCollector collector) {
    if (Strings.isNullOrEmpty(schema)) {
      String object = getObjectsToPull().iterator().next();
      return ObjectType.fromString(object, collector).getObjectSchema();
    }
    try {
      return Schema.parseJson(schema);
    } catch (IOException | IllegalStateException e) {
      collector
          .addFailure(String.format("Unable to parse output schema: '%s'.", schema), null)
          .withConfigProperty(PROPERTY_SCHEMA);
      throw collector.getOrThrowException();
    }
  }

  @Override
  public void validate(FailureCollector collector) {
    super.validate(collector);
    validateConnection(collector);
    if (!containsMacro(PROPERTY_START_DATE) && !containsMacro(PROPERTY_OBJECTS_TO_PULL)) {
      try {
        boolean batchObjectSelected =
            getObjects().stream().map(ObjectType::fromString).anyMatch(ObjectType::isBatch);
        if (batchObjectSelected && Strings.isNullOrEmpty(startDate)) {
          collector
              .addFailure(
                  "Property 'Start Date' must not be empty.",
                  "Ensure 'Start Date' is specified for objects: "
                      + "Ticket Comments, Organizations, Ticket Metrics, Ticket Metric Events, "
                      + "Tickets, Users.")
              .withConfigProperty(PROPERTY_START_DATE);
        }
      } catch (IllegalStateException e) {
        collector
            .addFailure(e.getMessage(), null)
            .withConfigProperty(BaseZendeskSourceConfig.PROPERTY_OBJECTS_TO_PULL);
      }
    }
    validateIntervalFilterProperty(PROPERTY_START_DATE, getStartDate(), collector);
    validateIntervalFilterProperty(PROPERTY_END_DATE, getEndDate(), collector);
  }

  @VisibleForTesting
  void validateConnection(FailureCollector collector) {
    if (containsMacro(BaseZendeskSourceConfig.PROPERTY_ADMIN_EMAIL)
        || containsMacro(BaseZendeskSourceConfig.PROPERTY_API_TOKEN)
        || containsMacro(BaseZendeskSourceConfig.PROPERTY_SUBDOMAINS)) {
      return;
    }

    getSubdomains()
        .forEach(
            subdomain -> {
              try (PagedIterator pagedIterator =
                  new PagedIterator(this, ObjectType.GROUPS, subdomain)) {
                pagedIterator.hasNext();
              } catch (IOException | ConnectionTimeoutException e) {
                collector
                    .addFailure(
                        String.format(
                            "There was issue communicating with Zendesk subdomain '%s'.",
                            subdomain),
                        null)
                    .withConfigProperty(BaseZendeskSourceConfig.PROPERTY_SUBDOMAINS);
              }
            });
  }

  private void validateIntervalFilterProperty(
      String propertyName, String datetime, FailureCollector collector) {
    if (containsMacro(propertyName)) {
      return;
    }
    if (Strings.isNullOrEmpty(datetime)) {
      return;
    }
    try {
      ZonedDateTime.parse(datetime, DateTimeFormatter.ISO_DATE_TIME);
    } catch (DateTimeParseException e) {
      collector
          .addFailure(
              String.format("Invalid '%s' value: '%s'.", propertyName, datetime),
              "Value must be in Zendesk Formats. For example, 2019-01-01T23:01:01Z")
          .withConfigProperty(propertyName);
    }
  }
}
