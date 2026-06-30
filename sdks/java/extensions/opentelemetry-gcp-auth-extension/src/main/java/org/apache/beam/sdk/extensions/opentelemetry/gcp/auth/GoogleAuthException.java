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
package org.apache.beam.sdk.extensions.opentelemetry.gcp.auth;

/**
 * An unchecked exception indicating a failure during Google authentication. This exception is
 * thrown when there are issues with retrieving or refreshing Google Application Default Credentials
 * (ADC).
 *
 * <p>Copied from
 * https://github.com/open-telemetry/opentelemetry-java-contrib/blob/main/gcp-auth-extension/src/main/java/io/opentelemetry/contrib/gcp/auth/GoogleAuthException.java
 */
public class GoogleAuthException extends RuntimeException {

  private static final long serialVersionUID = 149908685226796448L;

  /**
   * Constructs a new {@code GoogleAuthException} with the specified reason and cause.
   *
   * @param reason the reason for the authentication failure.
   * @param cause the underlying cause of the exception (e.g., an IOException).
   */
  GoogleAuthException(Reason reason, Throwable cause) {
    super(reason.message, cause);
  }

  /** Enumerates the possible reasons for a Google authentication failure. */
  enum Reason {
    /** Indicates a failure to retrieve Google Application Default Credentials. */
    FAILED_ADC_RETRIEVAL("Unable to retrieve Google Application Default Credentials."),
    /** Indicates a failure to retrieve Google Application Default Credentials. */
    FAILED_ADC_REFRESH("Unable to refresh Google Application Default Credentials.");

    private final String message;

    /**
     * Constructs a new {@code Reason} with the specified message.
     *
     * @param message the message describing the reason.
     */
    Reason(String message) {
      this.message = message;
    }

    /**
     * Returns the message associated with this reason.
     *
     * @return the message describing the reason.
     */
    public String getMessage() {
      return message;
    }
  }
}
