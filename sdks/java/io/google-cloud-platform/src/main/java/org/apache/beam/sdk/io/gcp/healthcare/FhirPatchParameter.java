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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Map;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * FhirPatchParemeter represents the parameters for a FHIR patch request, used as a parameter for
 * {@link FhirIO.PatchResources}.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
abstract class FhirPatchParameter implements Serializable {
  abstract String resourceName();

  abstract String patch();

  abstract @Nullable Map<String, String> query();

  static Builder builder() {
    return new AutoValue_FhirPatchParameter.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setResourceName(String resourceName);

    abstract Builder setPatch(String patch);

    abstract Builder setQuery(Map<String, String> query);

    abstract FhirPatchParameter build();
  }
}
