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
package org.apache.beam.sdk.fn;

import java.util.concurrent.atomic.AtomicLong;

/** Common {@link IdGenerator} implementations. */
public class IdGenerators {

  /** Returns an {@link IdGenerator} which provides successive incrementing longs. */
  public static IdGenerator incrementingLongs() {
    AtomicLong longs = new AtomicLong();
    return () -> Long.toString(longs.incrementAndGet());
  }

  /** Returns an {@link IdGenerators} that will provide successive decrementing longs. */
  public static IdGenerator decrementingLongs() {
    AtomicLong longs = new AtomicLong();
    return () -> Long.toString(longs.decrementAndGet());
  }
}
