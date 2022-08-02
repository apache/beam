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
package org.apache.beam.sdk.io.gcp.healthcare;

import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * FhirSearchParameter represents the query parameters for a FHIR search request, used as a
 * parameter for {@link FhirIO.Search}.
 */
@DefaultCoder(FhirSearchParameterCoder.class)
public class FhirSearchParameter<T> {

  /** FHIR resource type. */
  private final String resourceType;
  /**
   * The key is used as a key for the search query, if there is source information to propagate
   * through the pipeline.
   */
  private final String key;
  /**
   * The search query. For an OR search, put both query values in a single string. For an AND
   * search, use a list.
   */
  private final @Nullable Map<String, T> queries;

  private FhirSearchParameter(
      String resourceType, @Nullable String key, @Nullable Map<String, T> queries) {
    this.resourceType = resourceType;
    if (key != null) {
      this.key = key;
    } else {
      this.key = "";
    }
    this.queries = queries;
  }

  /**
   * Creates a FhirSearchParameter of type T.
   *
   * @param resourceType the FHIR resource type.
   * @param key Key for the search query to key the results with.
   * @param queries The search query.
   * @param <T> The type of the search query value.
   * @return The FhirSearchParameter of type T.
   */
  public static <T> FhirSearchParameter<T> of(
      String resourceType, @Nullable String key, @Nullable Map<String, T> queries) {
    return new FhirSearchParameter<>(resourceType, key, queries);
  }

  /**
   * Creates a FhirSearchParameter of type T, without a key.
   *
   * @param resourceType the FHIR resource type.
   * @param queries The search query.
   * @param <T> The type of the search query value.
   * @return The FhirSearchParameter of type T.
   */
  public static <T> FhirSearchParameter<T> of(
      String resourceType, @Nullable Map<String, T> queries) {
    return new FhirSearchParameter<>(resourceType, null, queries);
  }

  public String getResourceType() {
    return resourceType;
  }

  public String getKey() {
    return key;
  }

  public @Nullable Map<String, T> getQueries() {
    return queries;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FhirSearchParameter<?> that = (FhirSearchParameter<?>) o;
    return Objects.equals(resourceType, that.resourceType)
        && Objects.equals(key, that.key)
        && Objects.equals(queries, that.queries);
  }

  @Override
  public int hashCode() {
    return Objects.hash(resourceType, queries);
  }

  @Override
  public String toString() {
    return String.format(
        "FhirSearchParameter{resourceType='%s', key='%s', queries='%s'}'",
        resourceType, key, queries);
  }
}
