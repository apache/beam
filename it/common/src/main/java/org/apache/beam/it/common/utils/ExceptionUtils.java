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
package org.apache.beam.it.common.utils;

import org.checkerframework.checker.nullness.qual.Nullable;

/** Utility class for handling exceptions in tests or resource managers. */
public class ExceptionUtils {

  /**
   * Utility to check if the given exception or any of its causes contain a specific message.
   *
   * @param exception Exception to check.
   * @param message Message to search for.
   * @return true if the message is found in the exception or any of the causes, false otherwise.
   */
  public static boolean containsMessage(@Nullable Throwable exception, String message) {
    if (exception == null) {
      return false;
    }

    if (exception.getMessage() != null && exception.getMessage().contains(message)) {
      return true;
    }

    return containsMessage(exception.getCause(), message);
  }

  /**
   * Utility to check if the given exception or any of its causes have a specific type.
   *
   * @param exception Exception to check.
   * @param type Type to search for.
   * @return true if the type is found in the exception or any of the causes, false otherwise.
   */
  public static boolean containsType(
      @Nullable Throwable exception, Class<? extends Throwable> type) {
    if (exception == null) {
      return false;
    }

    if (type.isAssignableFrom(exception.getClass())) {
      return true;
    }

    return containsType(exception.getCause(), type);
  }
}
