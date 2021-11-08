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
package org.apache.beam.sdk.io.redis;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;

public class RedisCursorRange
    implements Serializable, HasDefaultTracker<RedisCursorRange, RedisCursorRangeTracker> {

  private RedisCursor startPosition;
  private RedisCursor endPosition;

  private RedisCursorRange(RedisCursor startPosition, RedisCursor endPosition) {
    this.startPosition = checkNotNull(startPosition, "startPosition");
    this.endPosition = checkNotNull(endPosition, "endPosition");
  }

  public static RedisCursorRange of(RedisCursor startPosition, RedisCursor endPosition) {
    return new RedisCursorRange(startPosition, endPosition);
  }

  @Override
  public RedisCursorRangeTracker newTracker() {
    return RedisCursorRangeTracker.of(this);
  }

  public RedisCursor getStartPosition() {
    return startPosition;
  }

  public RedisCursor getEndPosition() {
    return endPosition;
  }

  public RedisCursorRange fromEndPosition(RedisCursor position) {
    return new RedisCursorRange(endPosition, position);
  }
}
