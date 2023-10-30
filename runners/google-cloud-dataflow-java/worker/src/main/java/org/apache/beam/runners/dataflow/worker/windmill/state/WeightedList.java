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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import java.util.List;
import org.apache.beam.sdk.util.Weighted;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ForwardingList;

@VisibleForTesting
public class WeightedList<T> extends ForwardingList<T> implements Weighted {
  private final List<T> delegate;
  long weight;

  WeightedList(List<T> delegate) {
    this.delegate = delegate;
    this.weight = 0;
  }

  @Override
  protected List<T> delegate() {
    return delegate;
  }

  @Override
  public boolean add(T elem) {
    throw new UnsupportedOperationException("Must use AddWeighted()");
  }

  @Override
  public long getWeight() {
    return weight;
  }

  public void addWeighted(T elem, long weight) {
    delegate.add(elem);
    this.weight += weight;
  }
}
