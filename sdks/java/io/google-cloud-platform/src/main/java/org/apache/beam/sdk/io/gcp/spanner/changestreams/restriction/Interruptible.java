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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction;

import com.google.cloud.Timestamp;
import org.joda.time.Duration;

/** An interruptible interface for timestamp restriction tracker. */
public interface Interruptible {
  /** Sets a soft timeout from now for processing new timestamps. */
  public void setSoftTimeout(Duration duration);

  /**
   * Returns true if the timestamp tracker can process new timestamps or false if it should
   * interrupt processing.
   *
   * @return {@code true} if the position processing should continue, {@code false} if the soft
   *     deadline has been reached and we have fully processed the previous position.
   */
  public boolean shouldContinue(Timestamp position);
}
