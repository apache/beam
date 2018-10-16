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
package org.apache.beam.sdk.extensions.euphoria.core.client.util;

import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.CombinableReduceFunction;

/** Provides commonly used function objects around computing sums. */
@Audience(Audience.Type.CLIENT)
public class Sums {

  private static final CombinableReduceFunction<Long> SUMS_OF_LONG =
      (CombinableReduceFunction<Long>) s -> s.collect(Collectors.summingLong(e -> e));
  private static final CombinableReduceFunction<Integer> SUMS_OF_INT =
      (CombinableReduceFunction<Integer>) s -> s.collect(Collectors.summingInt(e -> e));

  private Sums() {}

  public static CombinableReduceFunction<Long> ofLongs() {
    return SUMS_OF_LONG;
  }

  public static CombinableReduceFunction<Integer> ofInts() {
    return SUMS_OF_INT;
  }
}
