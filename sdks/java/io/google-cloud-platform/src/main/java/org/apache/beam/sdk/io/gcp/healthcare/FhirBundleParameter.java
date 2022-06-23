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
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

/** FhirBundleParameter represents a FHIR bundle in JSON format to be executed on a FHIR store. */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class FhirBundleParameter implements Serializable {

  static Builder builder() {
    return new AutoValue_FhirBundleParameter.Builder();
  }

  /**
   * String representing the metadata of the Bundle to be written. Used to pass metadata through the
   * ExecuteBundles PTransform.
   */
  public abstract String getMetadata();

  /** FHIR R4 bundle resource object as a string. */
  public abstract String getBundle();

  @SchemaCreate
  public static FhirBundleParameter of(@Nullable String metadata, String bundle) {

    return FhirBundleParameter.builder()
        .setMetadata(Objects.toString(metadata, ""))
        .setBundle(bundle)
        .build();
  }

  public static FhirBundleParameter of(String bundle) {
    return FhirBundleParameter.of(null, bundle);
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setMetadata(String metadata);

    abstract Builder setBundle(String bundle);

    abstract FhirBundleParameter build();
  }
}
