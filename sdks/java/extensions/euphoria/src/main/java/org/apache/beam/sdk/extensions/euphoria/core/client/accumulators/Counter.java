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
package org.apache.beam.sdk.extensions.euphoria.core.client.accumulators;

import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;

/** Counter is a type of accumulator making a sum from integral numbers. */
@Audience(Audience.Type.CLIENT)
public interface Counter extends Accumulator {

  /**
   * Increment counter by given value.
   *
   * @param value Value to be added to the counter.
   */
  void increment(long value);

  /** Increment counter by one. */
  void increment();
}
