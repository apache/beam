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
package org.apache.beam.sdk.io.splunk;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link SplunkEvent} describes a single payload sent to Splunk's Http Event Collector (HEC)
 * endpoint.
 *
 * <p>Each object represents a single event and related metadata elements such as:
 *
 * <ul>
 *   <li>time
 *   <li>host
 *   <li>source
 *   <li>sourceType
 *   <li>index
 * </ul>
 */
@DefaultCoder(SplunkEventCoder.class)
@AutoValue
public abstract class SplunkEvent {

  /** Provides a builder for creating {@link SplunkEvent} objects. */
  public static Builder newBuilder() {
    return new AutoValue_SplunkEvent.Builder();
  }

  public abstract @Nullable Long time();

  public abstract @Nullable String host();

  public abstract @Nullable String source();

  @SerializedName("sourcetype")
  public abstract @Nullable String sourceType();

  public abstract @Nullable String index();

  public abstract @Nullable JsonObject fields();

  public abstract @Nullable String event();

  /** A builder class for creating a {@link SplunkEvent}. */
  @AutoValue.Builder
  public abstract static class Builder {

    abstract Builder setTime(Long time);

    abstract Builder setHost(String host);

    abstract Builder setSource(String source);

    abstract Builder setSourceType(String sourceType);

    abstract Builder setIndex(String index);

    abstract Builder setFields(JsonObject fields);

    abstract Builder setEvent(String event);

    abstract String event();

    abstract SplunkEvent build();

    /**
     * Assigns time value to the event metadata.
     *
     * @param time time value to assign
     */
    public Builder withTime(Long time) {
      checkNotNull(time, "withTime(time) called with null input.");

      return setTime(time);
    }

    /**
     * Assigns host value to the event metadata.
     *
     * @param host host value to assign
     */
    public Builder withHost(String host) {
      checkNotNull(host, "withHost(host) called with null input.");

      return setHost(host);
    }

    /**
     * Assigns source value to the event metadata.
     *
     * @param source source value to assign
     */
    public Builder withSource(String source) {
      checkNotNull(source, "withSource(source) called with null input.");

      return setSource(source);
    }

    /**
     * Assigns sourceType value to the event metadata.
     *
     * @param sourceType sourceType value to assign
     */
    public Builder withSourceType(String sourceType) {
      checkNotNull(sourceType, "withSourceType(sourceType) called with null input.");

      return setSourceType(sourceType);
    }

    /**
     * Assigns index value to the event metadata.
     *
     * @param index index value to assign
     */
    public Builder withIndex(String index) {
      checkNotNull(index, "withIndex(index) called with null input.");

      return setIndex(index);
    }

    /**
     * Assigns fields value to the event metadata.
     *
     * @param fields fields value to assign
     */
    public Builder withFields(JsonObject fields) {
      checkNotNull(fields, "withFields(fields) called with null input.");

      return setFields(fields);
    }

    /**
     * Assigns the event payload to be sent to the HEC endpoint.
     *
     * @param event payload to be sent to HEC
     */
    public Builder withEvent(String event) {
      checkNotNull(event, "withEvent(event) called with null input.");

      return setEvent(event);
    }

    /** Creates a {@link SplunkEvent} object. */
    public SplunkEvent create() {
      checkNotNull(event(), "Event information is required.");
      return build();
    }
  }
}
