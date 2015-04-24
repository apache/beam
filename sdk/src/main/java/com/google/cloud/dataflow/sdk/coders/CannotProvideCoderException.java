/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.coders;

/**
 * The exception thrown when a {@link CoderProvider} cannot
 * provide a {@link Coder} that has been requested.
 */
public class CannotProvideCoderException extends Exception {
  private static final long serialVersionUID = 0;

  public CannotProvideCoderException(String message) {
    super(message);
  }

  public CannotProvideCoderException(String message, Throwable cause) {
    super(message, cause);
  }

  public CannotProvideCoderException(Throwable cause) {
    super(cause);
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
}
