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
 * Convenience enum to map ObjectType UI selections to meaningful values.
 *
 * <p>Select from Contact Lists, Contacts, Email Events, Email Subscription, Recent Campaigns,
 * Analytics, Companies, Deals, Deal Pipelines, Marketing Email, Products, Tickets
 */
@SuppressWarnings("ImmutableEnumChecker")
public enum ObjectType {
  CONTACT_LISTS("Contact Lists"),
  CONTACTS("Contacts"),
  EMAIL_EVENTS("Email Events"),
  EMAIL_SUBSCRIPTION("Email Subscription"),
  RECENT_COMPANIES("Recent Companies"),
  ANALYTICS("Analytics"),
  COMPANIES("Companies"),
  DEALS("Deals"),
  DEAL_PIPELINES("Deal Pipelines"),
  MARKETING_EMAIL("Marketing Email"),
  PRODUCTS("Products"),
  TICKETS("Tickets");

  private String stringValue;

  ObjectType(String stringValue) {
    this.stringValue = stringValue;
  }

  /**
   * Returns the ObjectType.
   *
   * @param value the value is string type
   * @return the ObjectType
   */
  public static ObjectType fromString(String value) {
    return Arrays.stream(ObjectType.values())
        .filter(type -> type.stringValue.equals(value))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(String.format("'%s' is invalid ObjectType.", value)));
  }
}
