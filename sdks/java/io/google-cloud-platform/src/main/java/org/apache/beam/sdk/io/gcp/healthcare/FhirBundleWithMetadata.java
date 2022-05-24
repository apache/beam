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
 * FhirBundleWithMetadata represents a FHIR bundle, with it's metadata (eg. Hl7 messageId) in JSON
 * format to be executed on the intermediate FHIR store. *
 */
@DefaultCoder(FhirBundleWithMetadataCoder.class)
@AutoValue
public abstract class FhirBundleWithMetadata {

  static Builder builder() {
    return new AutoValue_FhirBundleWithMetadata.Builder();
  }

  public abstract String getMetadata();

  public abstract String getBundle();

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
