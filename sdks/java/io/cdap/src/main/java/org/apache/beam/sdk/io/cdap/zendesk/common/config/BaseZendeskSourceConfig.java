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
package org.apache.beam.sdk.io.cdap.zendesk.common.config;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.ReferencePluginConfig;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.cdap.zendesk.common.ObjectType;
import org.apache.commons.validator.routines.EmailValidator;

/** Base configuration for Zendesk Batch plugins. */
public class BaseZendeskSourceConfig extends ReferencePluginConfig {

  public static final String PROPERTY_ADMIN_EMAIL = "adminEmail";
  public static final String PROPERTY_API_TOKEN = "apiToken";
  public static final String PROPERTY_SUBDOMAINS = "subdomains";
  public static final String PROPERTY_OBJECTS_TO_PULL = "objectsToPull";
  public static final String PROPERTY_OBJECTS_TO_SKIP = "objectsToSkip";

  private static final String[] ALL_OBJECTS =
      new String[] {
        "Article Comments",
        "Post Comments",
        "Requests Comments",
        "Ticket Comments",
        "Groups",
        "Organizations",
        "Satisfaction Ratings",
        "Tags",
        "Ticket Fields",
        "Ticket Metrics",
        "Ticket Metric Events",
        "Tickets",
        "Users"
      };

  @Name(PROPERTY_ADMIN_EMAIL)
  @Description("Zendesk admin email.")
  @Macro
  private String adminEmail;

  @Name(PROPERTY_API_TOKEN)
  @Description("Zendesk API token.")
  @Macro
  private String apiToken;

  @Name(PROPERTY_SUBDOMAINS)
  @Description("Zendesk Subdomains to read objects from.")
  @Macro
  private String subdomains;

  @Name(PROPERTY_OBJECTS_TO_PULL)
  @Description("Objects to pull from Zendesk API.")
  @Macro
  @Nullable
  private String objectsToPull;

  @Name(PROPERTY_OBJECTS_TO_SKIP)
  @Description("Objects to skip from Zendesk API.")
  @Nullable
  private String objectsToSkip;

  /**
   * Constructor for BaseZendeskSourceConfig object.
   *
   * @param referenceName The reference name
   * @param adminEmail Zendesk admin email
   * @param apiToken Zendesk API token
   * @param subdomains The list of sub-domains
   * @param objectsToPull The list of objects to pull
   * @param objectsToSkip The list of objects to skip
   */
  public BaseZendeskSourceConfig(
      String referenceName,
      String adminEmail,
      String apiToken,
      String subdomains,
      @Nullable String objectsToPull,
      @Nullable String objectsToSkip) {
    super(referenceName);
    this.adminEmail = adminEmail;
    this.apiToken = apiToken;
    this.subdomains = subdomains;
    this.objectsToPull = objectsToPull;
    this.objectsToSkip = objectsToSkip;
  }

  public String getAdminEmail() {
    return adminEmail;
  }

  public String getApiToken() {
    return apiToken;
  }

  public Set<String> getSubdomains() {
    return getList(subdomains);
  }

  public Set<String> getObjectsToPull() {
    return getList(objectsToPull);
  }

  public Set<String> getObjectsToSkip() {
    return getList(objectsToSkip);
  }

  /**
   * Builds a final list of objects to be pulled by reading object to pull and objects to skip
   * lists.
   *
   * @return the list of objects to pull
   */
  public List<String> getObjects() {
    Set<String> objectsToPull = getObjectsToPull();
    Set<String> objectsToSkip = getObjectsToSkip();

    return Arrays.stream(ALL_OBJECTS)
        .filter(name -> objectsToPull.isEmpty() || objectsToPull.contains(name))
        .filter(name -> !objectsToSkip.contains(name))
        .collect(Collectors.toList());
  }

  /**
   * Validates {@link BaseZendeskSourceConfig} instance.
   *
   * @param collector The failure collector to collect the errors
   */
  public void validate(FailureCollector collector) {
    IdUtils.validateReferenceName(referenceName, collector);
    if (!containsMacro(PROPERTY_ADMIN_EMAIL) && !EmailValidator.getInstance().isValid(adminEmail)) {
      collector
          .addFailure(String.format("'%s' is not a valid email.", adminEmail), null)
          .withConfigProperty(PROPERTY_ADMIN_EMAIL);
    }
    if (!Strings.isNullOrEmpty(objectsToSkip) && getObjects().isEmpty()) {
      collector
          .addFailure(
              "All objects are skipped.",
              "Make sure 'Objects to Pull' and 'Objects to Skip' fields don't hold same values.")
          .withConfigProperty(PROPERTY_OBJECTS_TO_PULL)
          .withConfigProperty(PROPERTY_OBJECTS_TO_SKIP);
    }
  }

  protected Set<String> getList(String value) {
    return Strings.isNullOrEmpty(value)
        ? Collections.emptySet()
        : Stream.of(value.split(","))
            .map(String::trim)
            .filter(name -> !name.isEmpty())
            .collect(Collectors.toSet());
  }

  /**
   * Returns the map of schemas per object.
   *
   * @param collector The failure collector to collect the errors
   * @return map of schemas per object
   */
  public Map<String, Schema> getSchemas(FailureCollector collector) {
    return getObjects().stream()
        .map(object -> ObjectType.fromString(object, collector))
        .collect(Collectors.toMap(ObjectType::getObjectName, ObjectType::getObjectSchema));
  }
}
