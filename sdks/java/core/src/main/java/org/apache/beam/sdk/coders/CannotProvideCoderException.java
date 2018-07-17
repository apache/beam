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
package org.apache.beam.sdk.coders;

/**
 * The exception thrown when a {@link CoderRegistry} or {@link CoderProvider} cannot provide a
 * {@link Coder} that has been requested.
 */
public class CannotProvideCoderException extends Exception {
  private final ReasonCode reason;

  public CannotProvideCoderException(String message) {
    this(message, ReasonCode.UNKNOWN);
  }

  public CannotProvideCoderException(String message, ReasonCode reason) {
    super(message);
    this.reason = reason;
  }

  public CannotProvideCoderException(String message, Throwable cause) {
    this(message, cause, ReasonCode.UNKNOWN);
  }

  public CannotProvideCoderException(String message, Throwable cause, ReasonCode reason) {
    super(message, cause);
    this.reason = reason;
  }

  public CannotProvideCoderException(Throwable cause) {
    this(cause, ReasonCode.UNKNOWN);
  }

  public CannotProvideCoderException(Throwable cause, ReasonCode reason) {
    super(cause);
    this.reason = reason;
  }

  /** @return the reason that Coder inference failed. */
  public ReasonCode getReason() {
    return reason;
  }

  /**
   * Returns the inner-most {@link CannotProvideCoderException} when they are deeply nested.
   *
   * <p>For example, if a coder for {@code List<KV<Integer, Whatsit>>} cannot be provided because
   * there is no known coder for {@code Whatsit}, the root cause of the exception should be a
   * CannotProvideCoderException with details pertinent to {@code Whatsit}, suppressing the
   * intermediate layers.
   */
  public Throwable getRootCause() {
    Throwable cause = getCause();
    if (cause == null) {
      return this;
    } else if (!(cause instanceof CannotProvideCoderException)) {
      return cause;
    } else {
      return ((CannotProvideCoderException) cause).getRootCause();
    }
  }

  /** Indicates the reason that {@link Coder} inference failed. */
  public enum ReasonCode {
    /**
     * The reason a coder could not be provided is unknown or does not have an established {@link
     * ReasonCode}.
     */
    UNKNOWN,

    /**
     * The reason a coder could not be provided is type erasure, for example when requesting coder
     * inference for a {@code List<T>} where {@code T} is unknown.
     */
    TYPE_ERASURE,

    /**
     * The reason a coder could not be provided is because the type variable {@code T} is over
     * specified with multiple incompatible coders.
     */
    OVER_SPECIFIED
  }
}
