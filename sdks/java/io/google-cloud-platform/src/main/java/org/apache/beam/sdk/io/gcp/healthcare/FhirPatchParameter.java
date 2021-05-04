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

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * FhirPatchParemeter represents the parameters for a FHIR patch request, used as a parameter for
 * {@link FhirIO.PatchResources}.
 */
@DefaultCoder(FhirPatchParameterCoder.class)
public class FhirPatchParameter implements Serializable {
  private final String resourceName;
  private final String patch;
  private final @Nullable Map<String, String> query;

  private FhirPatchParameter(
      String resourceName, String patch, @Nullable Map<String, String> query) {
    this.resourceName = resourceName;
    this.patch = patch;
    this.query = query;
  }

  /**
   * Creates a FhirPatchParameter to represent a conditional FHIR Patch request.
   *
   * @param resourcePath the resource path, in format
   *     projects/{p}/locations/{l}/datasets/{d}/fhirStores/{f}/fhir/{resourceType}
   * @param patch the patch operation
   * @param query query for conditional patch
   * @return FhirPatchParameter
   */
  public static FhirPatchParameter of(
      String resourcePath, String patch, @Nullable Map<String, String> query) {
    return new FhirPatchParameter(resourcePath, patch, query);
  }

  /**
   * Creates a FhirPatchParameter to represent a FHIR Patch request.
   *
   * @param resourceName the resource name, in format
   *     projects/{p}/locations/{l}/datasets/{d}/fhirStores/{f}/fhir/{resourceType}/{id}
   * @param patch the patch operation
   * @return FhirPatchParameter
   */
  public static FhirPatchParameter of(String resourceName, String patch) {
    return new FhirPatchParameter(resourceName, patch, null);
  }

  public String getResourceName() {
    return resourceName;
  }

  public String getPatch() {
    return patch;
  }

  public Map<String, String> getQuery() {
    return query;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FhirPatchParameter that = (FhirPatchParameter) o;
    return Objects.equals(resourceName, that.resourceName)
        && Objects.equals(patch, that.patch)
        && Objects.equals(query, that.query);
  }

  @Override
  public int hashCode() {
    return Objects.hash(resourceName, patch, query);
  }

  @Override
  public String toString() {
    return "{\"resourceName\":\""
        + resourceName
        + "\",\"patch\":\""
        + patch
        + "\""
        + (query == null ? "}" : "\"query\":\"" + query.toString())
        + "\"}";
  }
}
