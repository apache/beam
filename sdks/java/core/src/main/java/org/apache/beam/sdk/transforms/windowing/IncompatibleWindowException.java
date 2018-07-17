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
package org.apache.beam.sdk.transforms.windowing;

/**
 * Exception thrown by {@link WindowFn#verifyCompatibility(WindowFn)} if two compared WindowFns are
 * not compatible, including the explanation of incompatibility.
 */
public class IncompatibleWindowException extends Exception {
  private WindowFn<?, ?> givenWindowFn;
  private String reason;

  public IncompatibleWindowException(WindowFn<?, ?> windowFn, String reason) {
    this.givenWindowFn = windowFn;
    this.reason = reason;
  }

  @Override
  public String getMessage() {
    String windowFn = givenWindowFn.getClass().getSimpleName();
    return String.format("The given WindowFn is %s. %s", windowFn, reason);
  }
}
