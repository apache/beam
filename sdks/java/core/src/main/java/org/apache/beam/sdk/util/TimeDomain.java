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
 * {@code TimeDomain} specifies whether an operation is based on
 * timestamps of elements or current "real-world" time as reported while processing.
 */
public enum TimeDomain {
  /**
   * The {@code EVENT_TIME} domain corresponds to the timestamps on the elements. Time advances
   * on the system watermark advances.
   */
  EVENT_TIME,

  /**
   * The {@code PROCESSING_TIME} domain corresponds to the current to the current (system) time.
   * This is advanced during execution of the pipeline.
   */
  PROCESSING_TIME,

  /**
   * Same as the {@code PROCESSING_TIME} domain, except it won't fire a timer set for time
   * {@code T} until all timers from earlier stages set for a time earlier than {@code T} have
   * fired.
   */
  SYNCHRONIZED_PROCESSING_TIME
}
