/**
 * Copyright 2017 Seznam.cz, a.s.
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

package cz.seznam.euphoria.core.client.dataset.asserts;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import static org.junit.Assert.assertEquals;

/**
 * Asserts related to dataset outputs.
 */
public class DatasetAssert {

  public static <T> void unorderedEquals(List<T> expected, T... values) {
    unorderedEquals(Arrays.asList(values), expected);
  }

  public static <T> void unorderedEquals(List<T> left, List<T> right) {
    Map<T, Integer> leftCounted = left.stream()
        .collect(Collectors.toMap(e -> e, e -> 1, (a, b) -> a + b));
    Map<T, Integer> rightCounted = right.stream()
        .collect(Collectors.toMap(e -> e, e -> 1, (a, b) -> a + b));
    assertEquals(leftCounted, rightCounted);
  }

}
