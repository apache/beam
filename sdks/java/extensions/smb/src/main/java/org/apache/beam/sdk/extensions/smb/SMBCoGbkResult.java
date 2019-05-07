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
package org.apache.beam.sdk.extensions.smb;

import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TupleTag;

/**
 * A row result of a {@link SortedBucketSource}. This is a group of {@link Iterable}s produced for a
 * given key, one for each source, similar to {@link
 * org.apache.beam.sdk.transforms.join.CoGbkResult}.
 */
public class SMBCoGbkResult {
  private final Map<TupleTag, Iterable<?>> valueMap;

  SMBCoGbkResult(Map<TupleTag, Iterable<?>> valueMap) {
    this.valueMap = valueMap;
  }

  /**
   * Returns the values from the table represented by the given {@code TupleTag<V>} as an {@code
   * Iterable<V>}.
   */
  public <V> Iterable<V> getAll(TupleTag<V> tag) {
    @SuppressWarnings("unchecked")
    final Iterable<V> result = (Iterable<V>) valueMap.get(tag);
    return result != null ? result : Collections.emptyList();
  }

  /**
   * Function to expand a {@link SMBCoGbkResult} into desired a result type, e.g. cartesian product
   * for joins.
   */
  public abstract static class ToFinalResult<FinalResultT>
      implements SerializableFunction<SMBCoGbkResult, FinalResultT> {
    public abstract Coder<FinalResultT> resultCoder();
  }
}
