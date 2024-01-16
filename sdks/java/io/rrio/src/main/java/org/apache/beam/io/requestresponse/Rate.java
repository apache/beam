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
package org.apache.beam.io.requestresponse;

import java.io.Serializable;
import org.joda.time.Duration;

/** Configures the number of elements at most emitted within a time interval. */
public class Rate implements Serializable {

  public static Rate of(int numElements, Duration interval) {
    return new Rate(numElements, interval);
  }

  private final int numElements;
  private final Duration interval;

  private Rate(int numElements, Duration interval) {
    this.numElements = numElements;
    this.interval = interval;
  }

  public int getNumElements() {
    return numElements;
  }

  public Duration getInterval() {
    return interval;
  }
}
