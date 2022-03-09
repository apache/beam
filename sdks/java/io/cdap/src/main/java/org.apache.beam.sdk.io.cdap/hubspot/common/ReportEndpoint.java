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
 * Convenience enum to map ReportEndpoint UI selections to meaningful values. The analytics report
 * type of content that you want to get data for. Must be one of:
 *
 * <p>landing-pages - Pull data for landing pages. standard-pages - Pull data for website pages.
 * blog-posts - Pull data for individual blog posts. listing-pages - Pull data for blog listing
 * pages. knowledge-articles - Pull data for knowledge base articles.
 *
 * <p>The analytics report category used to break down the analytics data. Must be one of:
 *
 * <p>totals - Data will be the totals rolled up from sessions - Data broken down by session details
 * sources - Data broken down by traffic source geolocation - Data broken down by geographic
 * location utm-:utm_type - Data broken down by the standard UTM parameters. :utm_type must be one
 * of campaigns, contents, mediums, sources, or terms (i.e. utm-campaigns).
 *
 * <p>The analytics report type of object that you want the analytics data for. Must be one of:
 *
 * <p>event-completions - Get data for analytics events. The results are broken down by the event
 * ID. You can get the details for the events using this endpoint. forms - Get data for your HubSpot
 * forms. The results are broken down by form guids. You can get the details for the forms through
 * the Forms API. pages - Get data for all URLs with data collected by HubSpot tracking code. The
 * results are broken down by URL. social-assists - Get data for messages published through the
 * social publishing tools. The results are broken down by the broadcastGuid of the messages. You
 * can get the details of those messages through the Social Media API.
 */
@SuppressWarnings("ImmutableEnumChecker")
public enum ReportEndpoint {
  LANDING_PAGES("landing-pages"),
  STANDARD_PAGES("standard-pages"),
  BLOG_POSTS("blog-posts"),
  LISTING_PAGES("listing-pages"),
  KNOWLEDGE_ARTICLES("knowledge-articles"),
  TOTALS("totals"),
  SESSIONS("sessions"),
  SOURCES("sources"),
  GEOLOCATION("geolocation"),
  UTM_CAMPAIGNS("utm-campaigns"),
  UTM_CONTENTS("utm-contents"),
  UTM_MEDIUMS("utm-mediums"),
  UTM_SOURCES("utm-sources"),
  UTM_TERMS("utm-terms"),
  EVENT_COMPLETIONS("event-completions"),
  FORMS("forms"),
  PAGES("pages"),
  SOCIAL_ASSISTS("social-assists");

  private String stringValue;

  ReportEndpoint(String stringValue) {
    this.stringValue = stringValue;
  }

  /**
   * Returns the ReportEndpoint.
   *
   * @param value the value is string type
   * @return the ReportEndpoint
   */
  public static ReportEndpoint fromString(String value) {
    return Arrays.stream(ReportEndpoint.values())
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
