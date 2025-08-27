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
package io.confluent.kafka.schemaregistry.client.rest.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Generic JSON error message.
 *
 * <p>This is an updated version of the original <code>
 * io.confluent.kafka.schemaregistry.client.rest.entities.ErrorMessage</code> class from the
 * Confluent Schema Registry Client, which has the same structure in Java and is still fully
 * compatible with the client code, but supports parsing the source JSON error message in more
 * formats via Jackson annotations (see {@link #unpackNestedError}).
 *
 * <p>In additional to the original error format:
 *
 * <pre>
 *   {
 *     "error_code": 400,
 *     "message": "..."
 *   }
 * </pre>
 *
 * It also supports this format:
 *
 * <pre>
 *   {
 *     "error: {
 *       "code": 400,
 *       "message": "..."
 *     }
 *   }
 * </pre>
 *
 * Since the class is in the same package <code>
 * io.confluent.kafka.schemaregistry.client.rest.entities</code>, pre-pending this library to the
 * classpath will override its implementation from the Schema Registry Client library and allow
 * parsing Managed Kafka style error messages.
 */
@Schema(description = "Error message")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ErrorMessage {

  private int errorCode = -1;
  private String message = "";

  public ErrorMessage() {}

  // Required for compatibility with the original class.
  // @JsonProperty("error_code") and @JsonProperty("message") annotations
  // have been removed to force Jackson always use the default constructor
  // and then set the values via either setErrorCode/setMessage or
  // unpackNestedError methods.
  public ErrorMessage(int errorCode, String message) {
    this.errorCode = errorCode;
    this.message = message;
  }

  @Schema(description = "Error code")
  @JsonProperty("error_code")
  public int getErrorCode() {
    return errorCode;
  }

  @JsonProperty("error_code")
  public void setErrorCode(int errorCode) {
    this.errorCode = errorCode;
  }

  @Schema(description = "Detailed error message")
  @JsonProperty
  public String getMessage() {
    return message;
  }

  @JsonProperty
  public void setMessage(String message) {
    this.message = message;
  }

  @JsonProperty("error")
  public void unpackNestedError(Map<String, Object> error) {
    Object code = error.get("code");

    if (code != null) {
      try {
        this.errorCode = ((Integer) code);
      } catch (RuntimeException e1) {
        try {
          this.errorCode = Integer.parseInt(code.toString());
        } catch (RuntimeException e2) {
          // ignore if can't parse the error code as an integer
        }
      }
    }

    String status = tryGetNonEmptyString(error.get("status"));
    String message = tryGetNonEmptyString(error.get("message"));

    if (message != null && status != null) {
      this.message = status + ": " + message;
    } else if (message != null) {
      this.message = message;
    } else if (status != null) {
      this.message = status;
    }
  }

  private static String tryGetNonEmptyString(@Nullable Object object) {
    if (object != null) {
      String s = object.toString();
      if (s.length() > 0) {
        return s;
      }
    }
    return "";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ErrorMessage that = (ErrorMessage) o;
    return errorCode == that.errorCode && Objects.equals(message, that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(errorCode, message);
  }
}
