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
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;

/**
 * FhirBundleWithMetadata represents a FHIR bundle, with it's metadata (eg. source ID like HL7
 * message path) in JSON format to be executed on any FHIR store. *
 */
@DefaultCoder(FhirBundleWithMetadataCoder.class)
@AutoValue
public abstract class FhirBundleWithMetadata {

  static Builder builder() {
    return new AutoValue_FhirBundleWithMetadata.Builder();
  }

  /**
   * String representing the source of the Bundle to be written. Used to pass source data through
   * the ExecuteBundles PTransform.
   */
  public abstract String getMetadata();

  /** FHIR R4 bundle resource object as a string. */
  public abstract String getBundle();

  /** HTTP response from the FHIR store after attempting to write the Bundle method. */
  public abstract String getResponse();

  public static FhirBundleWithMetadata of(
      @Nullable String metadata, String bundle, @Nullable String response) {

    return FhirBundleWithMetadata.builder()
        .setMetadata(Objects.toString(metadata, ""))
        .setBundle(bundle)
        .setResponse(Objects.toString(response, ""))
        .build();
  }

  public static FhirBundleWithMetadata of(String bundle) {
    return FhirBundleWithMetadata.of(null, bundle, null);
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setMetadata(String metadata);

    abstract Builder setBundle(String bundle);

    abstract Builder setResponse(String response);

    abstract FhirBundleWithMetadata build();
  }
}
