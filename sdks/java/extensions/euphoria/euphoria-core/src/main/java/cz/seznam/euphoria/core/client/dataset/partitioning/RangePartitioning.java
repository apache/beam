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
package cz.seznam.euphoria.core.client.dataset.partitioning;

import cz.seznam.euphoria.shaded.guava.com.google.common.base.Preconditions;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Comparators;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Partitioning based on ranges simulating total order. The ranges are expected to be provided
 * already sorted.
 * @param <T> type of ranges - must be a subtype {@link Comparable} as well as {@link Serializable}
 */
public class RangePartitioning<T extends Comparable<? super T> & Serializable> 
    implements Partitioning<T> {
  
  private final List<T> ranges;
  
  public RangePartitioning(List<T> ranges) {
    this.ranges = new ArrayList<>(ranges);
    Preconditions.checkArgument(
        Comparators.isInStrictOrder(ranges, Comparator.naturalOrder()),
        "Ranges are expected to be sorted!");
  }
  
  @SuppressWarnings("unchecked")
  public RangePartitioning(T ... ranges) {
    this(Arrays.asList(ranges));
  }

  @Override
  public Partitioner<T> getPartitioner() {
    return new RangePartitioner<>(ranges);
  }
  
  @Override
  public int getNumPartitions() {
    return ranges.size() + 1;
  }
}
