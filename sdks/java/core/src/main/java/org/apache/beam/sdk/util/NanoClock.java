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

/**
 * Nano clock which can be used to measure elapsed time in nanoseconds.
 *
 * <p>The default system implementation can be accessed at {@link #SYSTEM}. Alternative
 * implementations may be used for testing.
 */
@FunctionalInterface
interface NanoClock {

  /**
   * Returns the current value of the most precise available system timer, in nanoseconds for use to
   * measure elapsed time, to match the behavior of {@link System#nanoTime()}.
   */
  long nanoTime();

  /**
   * Provides the default System implementation of a nano clock by using {@link System#nanoTime()}.
   */
  NanoClock SYSTEM = System::nanoTime;
}
