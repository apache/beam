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

/** Thrown when a value appears to have been mutated, but that mutation is forbidden. */
public class IllegalMutationException extends RuntimeException {
  private Object savedValue;
  private Object newValue;

  public IllegalMutationException(String message, Object savedValue, Object newValue) {
    super(message);
    this.savedValue = savedValue;
    this.newValue = newValue;
  }

  public IllegalMutationException(
      String message, Object savedValue, Object newValue, Throwable cause) {
    super(message, cause);
    this.savedValue = savedValue;
    this.newValue = newValue;
  }

  /** The original value, before the illegal mutation. */
  public Object getSavedValue() {
    return savedValue;
  }

  /** The value after the illegal mutation. */
  public Object getNewValue() {
    return newValue;
  }
}
