/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.client.util;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import java.util.stream.Collectors;

/** Provides commonly used function objects around computing sums. */
@Audience(Audience.Type.CLIENT)
public class Sums {

  private static final CombinableReduceFunction<Long> SUMS_OF_LONG =
      (CombinableReduceFunction<Long>) s -> s.collect(Collectors.summingLong(e -> e));

  public static CombinableReduceFunction<Long> ofLongs() {
    return SUMS_OF_LONG;
  }

  private static final CombinableReduceFunction<Integer> SUMS_OF_INT =
      (CombinableReduceFunction<Integer>) s -> s.collect(Collectors.summingInt(e -> e));

  public static CombinableReduceFunction<Integer> ofInts() {
    return SUMS_OF_INT;
  }

  private Sums() {}
}
