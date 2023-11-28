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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * A data class that expresses a quota. Web API providers typically define a quota as the number of
 * requests per time interval.
 */
public class Quota implements Serializable {
  private final long numRequests;
  private final @NonNull Duration interval;

  public Quota(long numRequests, @NonNull Duration interval) {
    this.numRequests = numRequests;
    this.interval = interval;
  }

  /** The number of allowed requests. */
  public long getNumRequests() {
    return numRequests;
  }

  /** The duration context within which to allow requests. */
  public @NonNull Duration getInterval() {
    return interval;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Quota quota = (Quota) o;
    return numRequests == quota.numRequests && Objects.equal(interval, quota.interval);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(numRequests, interval);
  }

  @Override
  public String toString() {
    return "Quota{" + "numRequests=" + numRequests + ", interval=" + interval + '}';
  }
}
