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

/** Holds schemas for Zendesk objects. */
public class ObjectTypeSchemaConstants {

  public static final Schema SCHEMA_ARTICLE_COMMENTS =
      Schema.recordOf(
          "articleComments",
          Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
          Schema.Field.of("object", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("url", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("authorId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("sourceId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("sourceType", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("locale", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("htmlUrl", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("createdAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("updatedAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("voteSum", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("voteCount", Schema.nullableOf(Schema.of(Schema.Type.LONG))));

  public static final Schema SCHEMA_POST_COMMENTS =
      Schema.recordOf(
          "postComments",
          Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
          Schema.Field.of("object", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("url", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("authorId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("postId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("official", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("htmlUrl", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("createdAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("updatedAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("voteSum", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("voteCount", Schema.nullableOf(Schema.of(Schema.Type.LONG))));

  public static final Schema SCHEMA_REQUESTS_COMMENTS =
      Schema.recordOf(
          "requestsComments",
          Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
          Schema.Field.of("object", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("type", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("requestId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("htmlBody", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("plainBody", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("public", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("authorId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of(
              "attachments",
              Schema.nullableOf(
                  Schema.arrayOf(
                      Schema.nullableOf(
                          Schema.recordOf(
                              "attachmentObject",
                              Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                              Schema.Field.of(
                                  "fileName", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                              Schema.Field.of(
                                  "contentUrl", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                              Schema.Field.of(
                                  "url", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                              Schema.Field.of(
                                  "mappedContentUrl",
                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                              Schema.Field.of(
                                  "contentType", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                              Schema.Field.of(
                                  "width", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                              Schema.Field.of(
                                  "height", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                              Schema.Field.of(
                                  "size", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                              Schema.Field.of(
                                  "thumbnails",
                                  Schema.nullableOf(
                                      Schema.arrayOf(
                                          Schema.nullableOf(
                                              Schema.recordOf(
                                                  "photoObject",
                                                  Schema.Field.of(
                                                      "id",
                                                      Schema.nullableOf(
                                                          Schema.of(Schema.Type.LONG))),
                                                  Schema.Field.of(
                                                      "fileName",
                                                      Schema.nullableOf(
                                                          Schema.of(Schema.Type.STRING))),
                                                  Schema.Field.of(
                                                      "contentUrl",
                                                      Schema.nullableOf(
                                                          Schema.of(Schema.Type.STRING))),
                                                  Schema.Field.of(
                                                      "url",
                                                      Schema.nullableOf(
                                                          Schema.of(Schema.Type.STRING))),
                                                  Schema.Field.of(
                                                      "mappedContentUrl",
                                                      Schema.nullableOf(
                                                          Schema.of(Schema.Type.STRING))),
                                                  Schema.Field.of(
                                                      "contentType",
                                                      Schema.nullableOf(
                                                          Schema.of(Schema.Type.STRING))),
                                                  Schema.Field.of(
                                                      "width",
                                                      Schema.nullableOf(
                                                          Schema.of(Schema.Type.LONG))),
                                                  Schema.Field.of(
                                                      "height",
                                                      Schema.nullableOf(
                                                          Schema.of(Schema.Type.LONG))),
                                                  Schema.Field.of(
                                                      "size",
                                                      Schema.nullableOf(
                                                          Schema.of(Schema.Type.LONG))),
                                                  Schema.Field.of(
                                                      "inline",
                                                      Schema.nullableOf(
                                                          Schema.of(Schema.Type.BOOLEAN))),
                                                  Schema.Field.of(
                                                      "deleted",
                                                      Schema.nullableOf(
                                                          Schema.of(Schema.Type.BOOLEAN)))))))),
                              Schema.Field.of(
                                  "inline", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                              Schema.Field.of(
                                  "deleted",
                                  Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)))))))),
          Schema.Field.of("createdAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

  public static final Schema SCHEMA_TICKET_COMMENTS =
      Schema.recordOf(
          "ticketComments",
          Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
          Schema.Field.of("object", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("type", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("htmlBody", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("plainBody", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("public", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("authorId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of(
              "attachments",
              Schema.nullableOf(
                  Schema.arrayOf(
                      Schema.nullableOf(
                          Schema.recordOf(
                              "attachmentObject",
                              Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                              Schema.Field.of(
                                  "fileName", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                              Schema.Field.of(
                                  "contentUrl", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                              Schema.Field.of(
                                  "url", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                              Schema.Field.of(
                                  "mappedContentUrl",
                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                              Schema.Field.of(
                                  "contentType", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                              Schema.Field.of(
                                  "width", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                              Schema.Field.of(
                                  "height", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                              Schema.Field.of(
                                  "size", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                              Schema.Field.of(
                                  "thumbnails",
                                  Schema.nullableOf(
                                      Schema.arrayOf(
                                          Schema.nullableOf(
                                              Schema.recordOf(
                                                  "photoObject",
                                                  Schema.Field.of(
                                                      "id",
                                                      Schema.nullableOf(
                                                          Schema.of(Schema.Type.LONG))),
                                                  Schema.Field.of(
                                                      "fileName",
                                                      Schema.nullableOf(
                                                          Schema.of(Schema.Type.STRING))),
                                                  Schema.Field.of(
                                                      "contentUrl",
                                                      Schema.nullableOf(
                                                          Schema.of(Schema.Type.STRING))),
                                                  Schema.Field.of(
                                                      "url",
                                                      Schema.nullableOf(
                                                          Schema.of(Schema.Type.STRING))),
                                                  Schema.Field.of(
                                                      "mappedContentUrl",
                                                      Schema.nullableOf(
                                                          Schema.of(Schema.Type.STRING))),
                                                  Schema.Field.of(
                                                      "contentType",
                                                      Schema.nullableOf(
                                                          Schema.of(Schema.Type.STRING))),
                                                  Schema.Field.of(
                                                      "width",
                                                      Schema.nullableOf(
                                                          Schema.of(Schema.Type.LONG))),
                                                  Schema.Field.of(
                                                      "height",
                                                      Schema.nullableOf(
                                                          Schema.of(Schema.Type.LONG))),
                                                  Schema.Field.of(
                                                      "size",
                                                      Schema.nullableOf(
                                                          Schema.of(Schema.Type.LONG))),
                                                  Schema.Field.of(
                                                      "inline",
                                                      Schema.nullableOf(
                                                          Schema.of(Schema.Type.BOOLEAN))),
                                                  Schema.Field.of(
                                                      "deleted",
                                                      Schema.nullableOf(
                                                          Schema.of(Schema.Type.BOOLEAN)))))))),
                              Schema.Field.of(
                                  "inline", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                              Schema.Field.of(
                                  "deleted",
                                  Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)))))))),
          Schema.Field.of(
              "via",
              Schema.nullableOf(
                  Schema.recordOf(
                      "viaObject",
                      Schema.Field.of("channel", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of(
                          "source",
                          Schema.nullableOf(
                              Schema.recordOf(
                                  "sourceObject",
                                  Schema.Field.of(
                                      "from",
                                      Schema.nullableOf(
                                          Schema.recordOf(
                                              "fromObject",
                                              Schema.Field.of(
                                                  "profileUrl",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "subject",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "serviceInfo",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "title",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "topicName",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "id",
                                                  Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                              Schema.Field.of(
                                                  "topicId",
                                                  Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                              Schema.Field.of(
                                                  "revisionId",
                                                  Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                              Schema.Field.of(
                                                  "supportsChannelback",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "address",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "supportsClickthrough",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "originalRecipients",
                                                  Schema.nullableOf(
                                                      Schema.arrayOf(
                                                          Schema.nullableOf(
                                                              Schema.of(Schema.Type.STRING))))),
                                              Schema.Field.of(
                                                  "formattedPhone",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "ticketId",
                                                  Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                              Schema.Field.of(
                                                  "facebookId",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "registeredIntegrationServiceName",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "deleted",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "phone",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "name",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "username",
                                                  Schema.nullableOf(
                                                      Schema.of(Schema.Type.STRING)))))),
                                  Schema.Field.of(
                                      "to",
                                      Schema.nullableOf(
                                          Schema.recordOf(
                                              "toObject",
                                              Schema.Field.of(
                                                  "address",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "profileUrl",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "phone",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "name",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "formattedPhone",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "username",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "facebookId",
                                                  Schema.nullableOf(
                                                      Schema.of(Schema.Type.STRING)))))),
                                  Schema.Field.of(
                                      "rel",
                                      Schema.nullableOf(Schema.of(Schema.Type.STRING))))))))),
          Schema.Field.of("createdAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

  public static final Schema SCHEMA_GROUPS =
      Schema.recordOf(
          "group",
          Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
          Schema.Field.of("object", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("url", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("deleted", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("createdAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("updatedAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

  public static final Schema SCHEMA_ORGANIZATIONS =
      Schema.recordOf(
          "organization",
          Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
          Schema.Field.of("object", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("url", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("externalId", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("createdAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("updatedAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of(
              "domainNames",
              Schema.nullableOf(Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING))))),
          Schema.Field.of("details", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("notes", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("groupId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("sharedTickets", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("sharedComments", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of(
              "tags",
              Schema.nullableOf(Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING))))),
          Schema.Field.of(
              "organizationFields",
              Schema.nullableOf(
                  Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING)))));

  public static final Schema SCHEMA_SATISFACTION_RATINGS =
      Schema.recordOf(
          "satisfactionRatings",
          Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
          Schema.Field.of("object", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("url", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("assigneeId", Schema.of(Schema.Type.LONG)),
          Schema.Field.of("groupId", Schema.of(Schema.Type.LONG)),
          Schema.Field.of("requesterId", Schema.of(Schema.Type.LONG)),
          Schema.Field.of("ticketId", Schema.of(Schema.Type.LONG)),
          Schema.Field.of("score", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("createdAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("updatedAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("comment", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("reason", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("reasonId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("reasonCode", Schema.nullableOf(Schema.of(Schema.Type.LONG))));

  public static final Schema SCHEMA_TAGS =
      Schema.recordOf(
          "tags",
          Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("object", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("count", Schema.nullableOf(Schema.of(Schema.Type.LONG))));

  public static final Schema SCHEMA_TICKET_FIELDS =
      Schema.recordOf(
          "ticketFields",
          Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
          Schema.Field.of("object", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("url", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("type", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("title", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("rawTitle", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("description", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("rawDescription", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("position", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("active", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("required", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("collapsedForAgents", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("regexpForValidation", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("titleInPortal", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("rawTitleInPortal", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("visibleInPortal", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("editableInPortal", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("requiredInPortal", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("tag", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("createdAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("updatedAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of(
              "systemFieldOptions",
              Schema.nullableOf(
                  Schema.arrayOf(
                      Schema.nullableOf(
                          Schema.recordOf(
                              "systemFieldOption",
                              Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                              Schema.Field.of(
                                  "name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                              Schema.Field.of(
                                  "position", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                              Schema.Field.of(
                                  "rawName", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                              Schema.Field.of(
                                  "url", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                              Schema.Field.of(
                                  "value", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                              Schema.Field.of(
                                  "default",
                                  Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)))))))),
          Schema.Field.of(
              "customFieldOptions",
              Schema.nullableOf(
                  Schema.arrayOf(
                      Schema.nullableOf(
                          Schema.recordOf(
                              "customFieldOption",
                              Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                              Schema.Field.of(
                                  "name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                              Schema.Field.of(
                                  "position", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                              Schema.Field.of(
                                  "rawName", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                              Schema.Field.of(
                                  "url", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                              Schema.Field.of(
                                  "value", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                              Schema.Field.of(
                                  "default",
                                  Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)))))))),
          Schema.Field.of("subTypeId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("removable", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("agentDescription", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

  public static final Schema SCHEMA_TICKET_METRICS =
      Schema.recordOf(
          "ticketMetrics",
          Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
          Schema.Field.of("object", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("ticketId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("url", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("groupStations", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("assigneeStations", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("reopens", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("replies", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("assigneeUpdatedAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("requesterUpdatedAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("statusUpdatedAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("initiallyAssignedAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("assignedAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("solvedAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("latestCommentAddedAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of(
              "firstResolutionTimeInMinutes",
              Schema.nullableOf(
                  Schema.recordOf(
                      "firstResolutionTimeInMinute",
                      Schema.Field.of("calendar", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                      Schema.Field.of(
                          "business", Schema.nullableOf(Schema.of(Schema.Type.LONG)))))),
          Schema.Field.of(
              "replyTimeInMinutes",
              Schema.nullableOf(
                  Schema.recordOf(
                      "replyTimeInMinute",
                      Schema.Field.of("calendar", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                      Schema.Field.of(
                          "business", Schema.nullableOf(Schema.of(Schema.Type.LONG)))))),
          Schema.Field.of(
              "fullResolutionTimeInMinutes",
              Schema.nullableOf(
                  Schema.recordOf(
                      "fullResolutionTimeInMinute",
                      Schema.Field.of("calendar", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                      Schema.Field.of(
                          "business", Schema.nullableOf(Schema.of(Schema.Type.LONG)))))),
          Schema.Field.of(
              "agentWaitTimeInMinutes",
              Schema.nullableOf(
                  Schema.recordOf(
                      "agentWaitTimeInMinute",
                      Schema.Field.of("calendar", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                      Schema.Field.of(
                          "business", Schema.nullableOf(Schema.of(Schema.Type.LONG)))))),
          Schema.Field.of(
              "requesterWaitTimeInMinutes",
              Schema.nullableOf(
                  Schema.recordOf(
                      "requesterWaitTimeInMinute",
                      Schema.Field.of("calendar", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                      Schema.Field.of(
                          "business", Schema.nullableOf(Schema.of(Schema.Type.LONG)))))),
          Schema.Field.of("createdAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("updatedAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

  public static final Schema SCHEMA_TICKET_METRIC_EVENTS =
      Schema.recordOf(
          "ticketMetricEvents",
          Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
          Schema.Field.of("object", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("ticketId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("metric", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("instanceId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("type", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("time", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of(
              "sla",
              Schema.nullableOf(
                  Schema.recordOf(
                      "slaObject",
                      Schema.Field.of("target", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                      Schema.Field.of(
                          "businessHours", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                      Schema.Field.of(
                          "policy",
                          Schema.nullableOf(
                              Schema.recordOf(
                                  "policyObject",
                                  Schema.Field.of(
                                      "id", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                  Schema.Field.of(
                                      "title", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                  Schema.Field.of(
                                      "description",
                                      Schema.nullableOf(Schema.of(Schema.Type.STRING))))))))),
          Schema.Field.of(
              "status",
              Schema.nullableOf(
                  Schema.recordOf(
                      "statusObject",
                      Schema.Field.of("calendar", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                      Schema.Field.of(
                          "business", Schema.nullableOf(Schema.of(Schema.Type.LONG)))))),
          Schema.Field.of("deleted", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));

  public static final Schema SCHEMA_TICKETS =
      Schema.recordOf(
          "tickets",
          Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
          Schema.Field.of("object", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("url", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("externalId", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("type", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("subject", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("rawSubject", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("description", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("priority", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("status", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("recipient", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("requesterId", Schema.of(Schema.Type.LONG)),
          Schema.Field.of("submitterId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("assigneeId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("organizationId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("groupId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of(
              "collaboratorIds",
              Schema.nullableOf(Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.LONG))))),
          Schema.Field.of(
              "collaborators",
              Schema.nullableOf(
                  Schema.arrayOf(
                      Schema.nullableOf(
                          Schema.recordOf(
                              "collaborator",
                              Schema.Field.of(
                                  "name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                              Schema.Field.of(
                                  "email", Schema.nullableOf(Schema.of(Schema.Type.STRING)))))))),
          Schema.Field.of(
              "emailCcIds",
              Schema.nullableOf(Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.LONG))))),
          Schema.Field.of(
              "followerIds",
              Schema.nullableOf(Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.LONG))))),
          Schema.Field.of("forumTopicId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("problemId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("hasIncidents", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("dueAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of(
              "tags",
              Schema.nullableOf(Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING))))),
          Schema.Field.of(
              "via",
              Schema.nullableOf(
                  Schema.recordOf(
                      "viaObject",
                      Schema.Field.of("channel", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of(
                          "source",
                          Schema.nullableOf(
                              Schema.recordOf(
                                  "sourceObject",
                                  Schema.Field.of(
                                      "from",
                                      Schema.nullableOf(
                                          Schema.recordOf(
                                              "fromObject",
                                              Schema.Field.of(
                                                  "profileUrl",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "subject",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "serviceInfo",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "title",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "topicName",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "id",
                                                  Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                              Schema.Field.of(
                                                  "topicId",
                                                  Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                              Schema.Field.of(
                                                  "revisionId",
                                                  Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                              Schema.Field.of(
                                                  "supportsChannelback",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "address",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "supportsClickthrough",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "originalRecipients",
                                                  Schema.nullableOf(
                                                      Schema.arrayOf(
                                                          Schema.nullableOf(
                                                              Schema.of(Schema.Type.STRING))))),
                                              Schema.Field.of(
                                                  "formattedPhone",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "ticketId",
                                                  Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                              Schema.Field.of(
                                                  "facebookId",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "registeredIntegrationServiceName",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "deleted",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "phone",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "name",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "username",
                                                  Schema.nullableOf(
                                                      Schema.of(Schema.Type.STRING)))))),
                                  Schema.Field.of(
                                      "to",
                                      Schema.nullableOf(
                                          Schema.recordOf(
                                              "toObject",
                                              Schema.Field.of(
                                                  "address",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "profileUrl",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "phone",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "name",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "formattedPhone",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "username",
                                                  Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                              Schema.Field.of(
                                                  "facebookId",
                                                  Schema.nullableOf(
                                                      Schema.of(Schema.Type.STRING)))))),
                                  Schema.Field.of(
                                      "rel",
                                      Schema.nullableOf(Schema.of(Schema.Type.STRING))))))))),
          Schema.Field.of(
              "customFields",
              Schema.nullableOf(
                  Schema.arrayOf(
                      Schema.nullableOf(
                          Schema.recordOf(
                              "customField",
                              Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                              Schema.Field.of(
                                  "value", Schema.nullableOf(Schema.of(Schema.Type.STRING)))))))),
          Schema.Field.of(
              "satisfactionRating",
              Schema.nullableOf(
                  Schema.recordOf(
                      "satisfactionRatingObject",
                      Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                      Schema.Field.of("url", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of("assigneeId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                      Schema.Field.of("groupId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                      Schema.Field.of(
                          "requesterId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                      Schema.Field.of("ticketId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                      Schema.Field.of("score", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of(
                          "createdAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of(
                          "updatedAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of("comment", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of("reason", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of("reasonId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                      Schema.Field.of(
                          "reasonCode", Schema.nullableOf(Schema.of(Schema.Type.LONG)))))),
          Schema.Field.of(
              "sharingAgreementIds",
              Schema.nullableOf(Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.LONG))))),
          Schema.Field.of(
              "followupIds",
              Schema.nullableOf(Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.LONG))))),
          Schema.Field.of("viaFollowupSourceId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of(
              "macroIds",
              Schema.nullableOf(Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.LONG))))),
          Schema.Field.of("ticketFormId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("brandId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("allowChannelback", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("allowAttachments", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("isPublic", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("createdAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("updatedAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

  public static final Schema SCHEMA_USERS =
      Schema.recordOf(
          "users",
          Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
          Schema.Field.of("object", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("email", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("active", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("alias", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("chatOnly", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("createdAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("customRoleId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("roleType", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("details", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("externalId", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("lastLoginAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("locale", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("localeId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("moderator", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("notes", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("onlyPublicComments", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("organizationId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("defaultGroupId", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
          Schema.Field.of("phone", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("sharedPhoneNumber", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of(
              "photo",
              Schema.nullableOf(
                  Schema.recordOf(
                      "attachmentObject",
                      Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                      Schema.Field.of("fileName", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of(
                          "contentUrl", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of("url", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of(
                          "mappedContentUrl", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of(
                          "contentType", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of("width", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                      Schema.Field.of("height", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                      Schema.Field.of("size", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                      Schema.Field.of(
                          "thumbnails",
                          Schema.nullableOf(
                              Schema.arrayOf(
                                  Schema.nullableOf(
                                      Schema.recordOf(
                                          "photoObject",
                                          Schema.Field.of(
                                              "id", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                          Schema.Field.of(
                                              "fileName",
                                              Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                          Schema.Field.of(
                                              "contentUrl",
                                              Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                          Schema.Field.of(
                                              "url",
                                              Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                          Schema.Field.of(
                                              "mappedContentUrl",
                                              Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                          Schema.Field.of(
                                              "contentType",
                                              Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                          Schema.Field.of(
                                              "width",
                                              Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                          Schema.Field.of(
                                              "height",
                                              Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                          Schema.Field.of(
                                              "size",
                                              Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                          Schema.Field.of(
                                              "inline",
                                              Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                                          Schema.Field.of(
                                              "deleted",
                                              Schema.nullableOf(
                                                  Schema.of(Schema.Type.BOOLEAN)))))))),
                      Schema.Field.of("inline", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                      Schema.Field.of(
                          "deleted", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)))))),
          Schema.Field.of("restrictedAgent", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("role", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("shared", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("sharedAgent", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("signature", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("suspended", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of(
              "tags",
              Schema.nullableOf(Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING))))),
          Schema.Field.of("ticketRestriction", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("timeZone", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of(
              "twoFactorAuthEnabled", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("updatedAt", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("url", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of(
              "userFields",
              Schema.nullableOf(
                  Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING)))),
          Schema.Field.of("verified", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
          Schema.Field.of("reportCsv", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
}
