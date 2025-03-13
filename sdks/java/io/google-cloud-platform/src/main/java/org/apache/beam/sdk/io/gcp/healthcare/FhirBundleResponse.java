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

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class FhirBundleResponse implements Serializable {

  static FhirBundleResponse.Builder builder() {
    return new AutoValue_FhirBundleResponse.Builder();
  }

  /** FhirBundleParameter represents a FHIR bundle in JSON format to be executed on a FHIR store. */
  public abstract FhirBundleParameter getFhirBundleParameter();

  /**
   * HTTP response from the FHIR store after attempting to write the Bundle method. The value varies
   * depending on BATCH vs TRANSACTION bundles.
   */
  public abstract String getResponse();

  @SchemaCreate
  public static FhirBundleResponse of(
      FhirBundleParameter fhirBundleParameter, @Nullable String response) {
    return FhirBundleResponse.builder()
        .setFhirBundleParameter(fhirBundleParameter)
        .setResponse(Objects.toString(response, ""))
        .build();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract FhirBundleResponse.Builder setFhirBundleParameter(
        FhirBundleParameter fhirBundleParameter);

    abstract FhirBundleResponse.Builder setResponse(String response);

    abstract FhirBundleResponse build();
  }
}
