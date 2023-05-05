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

/**
 * HL7v2ReadParameter represents the read parameters for a HL7v2 read request, used as the input
 * type for {@link HL7v2IO.HL7v2Read}.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class HL7v2ReadParameter implements Serializable {

  static HL7v2ReadParameter.Builder builder() {
    return new AutoValue_HL7v2ReadParameter.Builder();
  }

  /**
   * String representing the metadata of the messageId to be read. Used to pass metadata through the
   * HL7v2Read PTransform.
   */
  public abstract String getMetadata();

  /** HL7v2MessageId string. */
  public abstract String getHl7v2MessageId();

  @SchemaCreate
  public static HL7v2ReadParameter of(@Nullable String metadata, String hl7v2MessageId) {

    return HL7v2ReadParameter.builder()
        .setMetadata(Objects.toString(metadata, ""))
        .setHl7v2MessageId(hl7v2MessageId)
        .build();
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract HL7v2ReadParameter.Builder setMetadata(String metadata);

    public abstract Builder setHl7v2MessageId(String value);

    abstract HL7v2ReadParameter build();
  }

  @Override
  public final boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof HL7v2ReadParameter)) {
      return false;
    }
    HL7v2ReadParameter other = (HL7v2ReadParameter) o;
    return (Objects.equals(getMetadata(), other.getMetadata())
        && Objects.equals(getHl7v2MessageId(), other.getHl7v2MessageId()));
  }

  @Override
  public final int hashCode() {
    return Objects.hash(getMetadata(), getHl7v2MessageId());
  }
}
