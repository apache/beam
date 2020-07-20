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
package org.apache.beam.sdk.util;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings.lenientFormat;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.apache.beam.sdk.annotations.Internal;
import org.checkerframework.checker.nullness.qual.EnsuresNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Beam-specific variants of {@link
 * org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions} that throws more
 * appropriate exception classes while being static analysis friendly.
 */
@Internal
public class Preconditions {
  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * @param reference an object reference
   * @return the non-null reference that was validated
   * @throws IllegalArgumentException if {@code reference} is null
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("reference")
  public static <T extends @NonNull Object> T checkArgumentNotNull(@Nullable T reference) {
    if (reference == null) {
      throw new IllegalArgumentException();
    }
    return reference;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * @param reference an object reference
   * @param errorMessage the exception message to use if the check fails; will be converted to a
   *     string using {@link String#valueOf(Object)}
   * @return the non-null reference that was validated
   * @throws IllegalArgumentException if {@code reference} is null
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("reference")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      @Nullable T reference, @Nullable Object errorMessage) {
    if (reference == null) {
      throw new IllegalArgumentException(String.valueOf(errorMessage));
    }
    return reference;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * @param reference an object reference
   * @param errorMessageTemplate a template for the exception message should the check fail. The
   *     message is formed by replacing each {@code %s} placeholder in the template with an
   *     argument. These are matched by position - the first {@code %s} gets {@code
   *     errorMessageArgs[0]}, etc. Unmatched arguments will be appended to the formatted message in
   *     square braces. Unmatched placeholders will be left as-is.
   * @param errorMessageArgs the arguments to be substituted into the message template. Arguments
   *     are converted to strings using {@link String#valueOf(Object)}.
   * @return the non-null reference that was validated
   * @throws IllegalArgumentException if {@code reference} is null
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("reference")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      @Nullable T reference,
      @Nullable String errorMessageTemplate,
      @Nullable Object... errorMessageArgs) {
    if (reference == null) {
      throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, errorMessageArgs));
    }
    return reference;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * <p>See {@link #checkArgumentNotNull(Object, String, Object...)} for details.
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("obj")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      T obj, @Nullable String errorMessageTemplate, char p1) {
    if (obj == null) {
      throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1));
    }
    return obj;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * <p>See {@link #checkArgumentNotNull(Object, String, Object...)} for details.
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("obj")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      T obj, @Nullable String errorMessageTemplate, int p1) {
    if (obj == null) {
      throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1));
    }
    return obj;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * <p>See {@link #checkArgumentNotNull(Object, String, Object...)} for details.
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("obj")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      T obj, @Nullable String errorMessageTemplate, long p1) {
    if (obj == null) {
      throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1));
    }
    return obj;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * <p>See {@link #checkArgumentNotNull(Object, String, Object...)} for details.
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("obj")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      T obj, @Nullable String errorMessageTemplate, @Nullable Object p1) {
    if (obj == null) {
      throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1));
    }
    return obj;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * <p>See {@link #checkArgumentNotNull(Object, String, Object...)} for details.
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("obj")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      T obj, @Nullable String errorMessageTemplate, char p1, char p2) {
    if (obj == null) {
      throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
    }
    return obj;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * <p>See {@link #checkArgumentNotNull(Object, String, Object...)} for details.
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("obj")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      T obj, @Nullable String errorMessageTemplate, char p1, int p2) {
    if (obj == null) {
      throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
    }
    return obj;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * <p>See {@link #checkArgumentNotNull(Object, String, Object...)} for details.
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("obj")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      T obj, @Nullable String errorMessageTemplate, char p1, long p2) {
    if (obj == null) {
      throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
    }
    return obj;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * <p>See {@link #checkArgumentNotNull(Object, String, Object...)} for details.
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("obj")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      T obj, @Nullable String errorMessageTemplate, char p1, @Nullable Object p2) {
    if (obj == null) {
      throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
    }
    return obj;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * <p>See {@link #checkArgumentNotNull(Object, String, Object...)} for details.
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("obj")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      T obj, @Nullable String errorMessageTemplate, int p1, char p2) {
    if (obj == null) {
      throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
    }
    return obj;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * <p>See {@link #checkArgumentNotNull(Object, String, Object...)} for details.
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("obj")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      T obj, @Nullable String errorMessageTemplate, int p1, int p2) {
    if (obj == null) {
      throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
    }
    return obj;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * <p>See {@link #checkArgumentNotNull(Object, String, Object...)} for details.
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("obj")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      T obj, @Nullable String errorMessageTemplate, int p1, long p2) {
    if (obj == null) {
      throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
    }
    return obj;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * <p>See {@link #checkArgumentNotNull(Object, String, Object...)} for details.
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("obj")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      T obj, @Nullable String errorMessageTemplate, int p1, @Nullable Object p2) {
    if (obj == null) {
      throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
    }
    return obj;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * <p>See {@link #checkArgumentNotNull(Object, String, Object...)} for details.
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("obj")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      T obj, @Nullable String errorMessageTemplate, long p1, char p2) {
    if (obj == null) {
      throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
    }
    return obj;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * <p>See {@link #checkArgumentNotNull(Object, String, Object...)} for details.
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("obj")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      T obj, @Nullable String errorMessageTemplate, long p1, int p2) {
    if (obj == null) {
      throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
    }
    return obj;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * <p>See {@link #checkArgumentNotNull(Object, String, Object...)} for details.
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("obj")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      T obj, @Nullable String errorMessageTemplate, long p1, long p2) {
    if (obj == null) {
      throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
    }
    return obj;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * <p>See {@link #checkArgumentNotNull(Object, String, Object...)} for details.
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("obj")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      T obj, @Nullable String errorMessageTemplate, long p1, @Nullable Object p2) {
    if (obj == null) {
      throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
    }
    return obj;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * <p>See {@link #checkArgumentNotNull(Object, String, Object...)} for details.
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("obj")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      T obj, @Nullable String errorMessageTemplate, @Nullable Object p1, char p2) {
    if (obj == null) {
      throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
    }
    return obj;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * <p>See {@link #checkArgumentNotNull(Object, String, Object...)} for details.
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("obj")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      T obj, @Nullable String errorMessageTemplate, @Nullable Object p1, int p2) {
    if (obj == null) {
      throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
    }
    return obj;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * <p>See {@link #checkArgumentNotNull(Object, String, Object...)} for details.
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("obj")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      T obj, @Nullable String errorMessageTemplate, @Nullable Object p1, long p2) {
    if (obj == null) {
      throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
    }
    return obj;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * <p>See {@link #checkArgumentNotNull(Object, String, Object...)} for details.
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("obj")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      T obj, @Nullable String errorMessageTemplate, @Nullable Object p1, @Nullable Object p2) {
    if (obj == null) {
      throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2));
    }
    return obj;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * <p>See {@link #checkArgumentNotNull(Object, String, Object...)} for details.
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("obj")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      T obj,
      @Nullable String errorMessageTemplate,
      @Nullable Object p1,
      @Nullable Object p2,
      @Nullable Object p3) {
    if (obj == null) {
      throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2, p3));
    }
    return obj;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null.
   *
   * <p>See {@link #checkArgumentNotNull(Object, String, Object...)} for details.
   */
  @CanIgnoreReturnValue
  @EnsuresNonNull("obj")
  public static <T extends @NonNull Object> T checkArgumentNotNull(
      T obj,
      @Nullable String errorMessageTemplate,
      @Nullable Object p1,
      @Nullable Object p2,
      @Nullable Object p3,
      @Nullable Object p4) {
    if (obj == null) {
      throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, p1, p2, p3, p4));
    }
    return obj;
  }
}
