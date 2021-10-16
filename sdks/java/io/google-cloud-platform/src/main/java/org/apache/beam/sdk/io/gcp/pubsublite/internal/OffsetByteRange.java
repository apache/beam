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
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.range.OffsetRange;

@AutoValue
@DefaultCoder(OffsetByteRangeCoder.class)
abstract class OffsetByteRange {
  abstract OffsetRange getRange();

  abstract long getByteCount();

  static OffsetByteRange of(OffsetRange range, long byteCount) {
    return new AutoValue_OffsetByteRange(range, byteCount);
  }

  static OffsetByteRange of(OffsetRange range) {
    return of(range, 0);
  }
}
