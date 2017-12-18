/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.testing;

import org.junit.Assert;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DatasetAssert {

  /**
   * Compare two data sets, no matter how they are ordered.
   *
   * @param left first dataset to compare
   * @param right second dataset to compare
   * @param <T> type of data, that data sets contain
   */
  public static <T> void unorderedEquals(List<T> left, List<T> right) {
    final Map<T, Integer> leftCounted = left.stream()
        .collect(Collectors.toMap(e -> e, e -> 1, (a, b) -> a + b));
    final Map<T, Integer> rightCounted = right.stream()
        .collect(Collectors.toMap(e -> e, e -> 1, (a, b) -> a + b));
    Assert.assertEquals(leftCounted, rightCounted);
  }
}
