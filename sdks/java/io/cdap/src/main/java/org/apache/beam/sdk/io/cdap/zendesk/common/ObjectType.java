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
package org.apache.beam.sdk.io.cdap.zendesk.common;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import java.util.Arrays;
import org.apache.beam.sdk.io.cdap.zendesk.common.config.BaseZendeskSourceConfig;

/** Supported Zendesk objects with schema. */
@SuppressWarnings("ImmutableEnumChecker")
public enum ObjectType {
  USERS_SIMPLE("Users Simple", "users", "users.json", false, null),
  REQUESTS("Requests", "requests", "requests.json", false, null),
  ARTICLE_COMMENTS(
      "Article Comments",
      "comments",
      "help_center/users/%s/comments.json",
      false,
      ObjectTypeSchemaConstants.SCHEMA_ARTICLE_COMMENTS),
  POST_COMMENTS(
      "Post Comments",
      "comments",
      "community/users/%s/comments.json",
      false,
      ObjectTypeSchemaConstants.SCHEMA_POST_COMMENTS),
  REQUESTS_COMMENTS(
      "Requests Comments",
      "comments",
      "requests/%s/comments.json",
      false,
      ObjectTypeSchemaConstants.SCHEMA_REQUESTS_COMMENTS),
  TICKET_COMMENTS(
      "Ticket Comments",
      "ticket_events",
      "child_events",
      "incremental/ticket_events.json?include=comment_events",
      true,
      ObjectTypeSchemaConstants.SCHEMA_TICKET_COMMENTS),
  GROUPS("Groups", "groups", "groups.json", false, ObjectTypeSchemaConstants.SCHEMA_GROUPS),
  ORGANIZATIONS(
      "Organizations",
      "organizations",
      "incremental/organizations.json",
      true,
      ObjectTypeSchemaConstants.SCHEMA_ORGANIZATIONS),
  SATISFACTION_RATINGS(
      "Satisfaction Ratings",
      "satisfaction_ratings",
      "satisfaction_ratings.json",
      false,
      ObjectTypeSchemaConstants.SCHEMA_SATISFACTION_RATINGS),
  TAGS("Tags", "tags", "tags.json", false, ObjectTypeSchemaConstants.SCHEMA_TAGS),
  TICKET_FIELDS(
      "Ticket Fields",
      "ticket_fields",
      "ticket_fields.json",
      false,
      ObjectTypeSchemaConstants.SCHEMA_TICKET_FIELDS),
  TICKET_METRICS(
      "Ticket Metrics",
      "ticket_metrics",
      "ticket_metrics.json",
      false,
      ObjectTypeSchemaConstants.SCHEMA_TICKET_METRICS),
  TICKET_METRIC_EVENTS(
      "Ticket Metric Events",
      "ticket_metric_events",
      "incremental/ticket_metric_events.json",
      true,
      ObjectTypeSchemaConstants.SCHEMA_TICKET_METRIC_EVENTS),
  TICKETS(
      "Tickets",
      "tickets",
      "incremental/tickets.json",
      true,
      ObjectTypeSchemaConstants.SCHEMA_TICKETS),
  USERS("Users", "users", "incremental/users.json", true, ObjectTypeSchemaConstants.SCHEMA_USERS);

  private static final String CLASS_NAME = ObjectType.class.getName();

  private final String objectName;
  private final String responseKey;
  private final String childKey;
  private final String apiEndpoint;
  private final boolean batch;
  private final Schema objectSchema;

  ObjectType(
      String objectName,
      String responseKey,
      String apiEndpoint,
      boolean batch,
      Schema objectSchema) {
    this(objectName, responseKey, null, apiEndpoint, batch, objectSchema);
  }

  ObjectType(
      String objectName,
      String responseKey,
      String childKey,
      String apiEndpoint,
      boolean batch,
      Schema objectSchema) {
    this.objectName = objectName;
    this.responseKey = responseKey;
    this.childKey = childKey;
    this.apiEndpoint = apiEndpoint;
    this.batch = batch;
    this.objectSchema = objectSchema;
  }

  public String getObjectName() {
    return objectName;
  }

  public String getResponseKey() {
    return responseKey;
  }

  public String getChildKey() {
    return childKey;
  }

  public String getApiEndpoint() {
    return apiEndpoint;
  }

  public boolean isBatch() {
    return batch;
  }

  public Schema getObjectSchema() {
    return objectSchema;
  }

  /**
   * Converts object type string value into {@link ObjectType} enum.
   *
   * @param value object type string value
   * @return {@link ObjectType} enum
   */
  public static ObjectType fromString(String value) {
    return Arrays.stream(ObjectType.values())
        .filter(fields -> fields.getObjectName().equals(value))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(String.format("'%s' is invalid %s", value, CLASS_NAME)));
  }

  /**
   * Converts object type string value into {@link ObjectType} enum.
   *
   * @param value object type string value
   * @param collector the failure collector to collect the errors
   * @return {@link ObjectType} enum
   */
  public static ObjectType fromString(String value, FailureCollector collector) {
    return Arrays.stream(ObjectType.values())
        .filter(objectType -> objectType.getObjectName().equals(value))
        .findFirst()
        .orElseThrow(
            () -> {
              collector
                  .addFailure(
                      String.format("Unsupported schema for object %s. ", value),
                      "Ensure 'Object to Pull' is selected.")
                  .withConfigProperty(BaseZendeskSourceConfig.PROPERTY_OBJECTS_TO_PULL);
              return collector.getOrThrowException();
            });
  }
}
