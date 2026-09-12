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
package org.apache.beam.runners.dataflow.worker.util;

/** A simple histogram to track byte sizes. */
public class SimpleByteHistogram {
  private final long[] buckets = new long[7];

  public void add(long weight) {
    buckets[getBucket(weight)]++;
  }

  private int getBucket(long weight) {
    if (weight < 128) return 0;
    if (weight < 256) return 1;
    if (weight < 512) return 2;
    if (weight < 1024) return 3;
    if (weight < 10 * 1024) return 4;
    if (weight < 1024 * 1024) return 5;
    return 6;
  }

  public String format() {
    return String.format(
        "[<128B:%d, <256B:%d, <512B:%d, <1KB:%d, <10KB:%d, <1MB:%d, >=1MB:%d]",
        buckets[0], buckets[1], buckets[2], buckets[3], buckets[4], buckets[5], buckets[6]);
  }
}
