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
package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * A GroupByKey place holder {@link Operation} during pipeline translation.
 */
public class GroupByKeyOperation<K, V> extends Operation<KV<K, V>> {

  private final WindowingStrategy<?, ?> windowingStrategy;
  private final KvCoder<K, V> kvCoder;

  public GroupByKeyOperation(WindowingStrategy<?, ?> windowingStrategy, KvCoder<K, V> kvCoder) {
    super(1);
    this.windowingStrategy = checkNotNull(windowingStrategy, "windowingStrategy");
    this.kvCoder = checkNotNull(kvCoder, "kvCoder");
  }

  @Override
  public void process(WindowedValue elem) {
    throw new IllegalStateException(
        String.format("%s should not in execution graph.", this.getClass().getSimpleName()));
  }

  public WindowingStrategy<?, ?> getWindowingStrategy() {
    return windowingStrategy;
  }

  public KvCoder<K, V> getKvCoder() {
    return kvCoder;
  }
}
