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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * HL7v2ReadResponse represents the response format for a HL7v2 read request, used as the output
 * type of {@link HL7v2IO.HL7v2Read}.
 */
public class HL7v2ReadResponse {

  @Nullable private final String metadata;
  private final HL7v2Message hl7v2Message;

  @Override
  public String toString() {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(this);
    } catch (IOException e) {
      return this.hl7v2Message.toString();
    }
  }

  /**
   * From metadata and hl7v2Message to {@link HL7v2ReadResponse}.
   *
   * @param metadata the metadata
   * @param hl7v2Message the hl7v2 message
   * @return the hl7v2 read response
   */
  public static HL7v2ReadResponse of(String metadata, HL7v2Message hl7v2Message) {
    return new HL7v2ReadResponse(metadata, hl7v2Message);
  }

  public HL7v2ReadResponse(@Nullable String metadata, HL7v2Message hl7v2Message) {
    this.metadata = metadata;
    this.hl7v2Message = hl7v2Message;
  }

  /**
   * Gets metadata.
   *
   * @return the metadata
   */
  @Nullable
  public String getMetadata() {
    return metadata;
  }

  /**
   * Gets hl7v2Message.
   *
   * @return the hl7v2Message
   */
  public HL7v2Message getHL7v2Message() {
    return hl7v2Message;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof HL7v2ReadResponse)) {
      return false;
    }
    HL7v2ReadResponse other = (HL7v2ReadResponse) o;
    return (Objects.equals(metadata, other.getMetadata())
        && Objects.equals(hl7v2Message, other.getHL7v2Message()));
  }

  @Override
  public int hashCode() {
    return Objects.hash(metadata, hl7v2Message);
  }
}
