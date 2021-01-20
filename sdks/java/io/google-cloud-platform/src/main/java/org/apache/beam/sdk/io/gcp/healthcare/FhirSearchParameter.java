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

  private String resourceType;
  private @Nullable Map<String, T> queries;

  public FhirSearchParameter(String resourceType, @Nullable Map<String, T> queries) {
    this.resourceType = resourceType;
    this.queries = queries;
  }

  public static <T> FhirSearchParameter<T> of(String resourceType, Map<String, T> queries) {
    return new FhirSearchParameter<>(resourceType, queries);
  }

  public String getResourceType() {
    return resourceType;
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
    return Objects.equals(resourceType, that.resourceType) && Objects.equals(queries, that.queries);
  }

  @Override
  public int hashCode() {
    return Objects.hash(resourceType, queries);
  }
}
