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
package org.apache.beam.sdk.io.components.ratelimiter;

import com.google.auto.value.AutoValue;
import java.util.Map;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Context for an Envoy Rate Limiter call.
 *
 * <p>Contains the domain and descriptors required to define a specific rate limit bucket.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class EnvoyRateLimiterContext implements RateLimiterContext {

  public abstract String getDomain();

  public abstract ImmutableMap<String, String> getDescriptors();

  public static Builder builder() {
    return new AutoValue_EnvoyRateLimiterContext.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setDomain(@NonNull String domain);

    public abstract ImmutableMap.Builder<String, String> descriptorsBuilder();

    public Builder addDescriptor(@NonNull String key, @NonNull String value) {
      descriptorsBuilder().put(key, value);
      return this;
    }

    public Builder setDescriptors(@NonNull Map<String, String> descriptors) {
      descriptorsBuilder().putAll(descriptors);
      return this;
    }

    public abstract EnvoyRateLimiterContext build();
  }
}
