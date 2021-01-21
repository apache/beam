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

@SuppressWarnings({
    "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
/**
 * SearchParameter represents the query parameters for a FHIR search request, used as a parameter
 * for {@link FhirIO.Search}.
 */
public class SearchParameter<T> {

  String resourceType;
  Map<String, T> queries;

  public SearchParameter(String resourceType, Map<String, T> queries) {
    this.resourceType = resourceType;
    this.queries = queries;
  }

  public static <T> SearchParameter<T> of(String resourceType, Map<String, T> queries) {
    return new SearchParameter<>(resourceType, queries);
  }

  public String getResourceType() {
    return resourceType;
  }

  public Map<String, T> getQueries() {
    return queries;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SearchParameter<?> that = (SearchParameter<?>) o;
    return Objects.equals(resourceType, that.resourceType) && Objects.equals(queries, that.queries);
  }

  @Override
  public int hashCode() {
    return Objects.hash(resourceType, queries);
  }
}
