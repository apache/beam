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
package org.apache.beam.sdk.io.components.deadletterqueue;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.NonNull;

public class DLQRouter<T, K> extends PTransform<@NonNull PCollectionTuple, @NonNull PCollection<T>> {

  private final TupleTag<T> goodMessages;

  private final TupleTag<K> badMessages;

  private final PTransform<@NonNull PCollection<K>,?> errorSink;

  public DLQRouter (TupleTag<T> goodMessages, TupleTag<K> badMessages, PTransform<@NonNull PCollection<K>,?> errorSink){
    this.goodMessages = goodMessages;
    this.badMessages = badMessages;
    this.errorSink = errorSink;
  }
  @Override
  public PCollection<T> expand(@NonNull PCollectionTuple input) {
    //validate no extra messages are dropped
    Map<TupleTag<?>,PCollection<?>> pcollections = new HashMap<>(input.getAll());
    pcollections.remove(goodMessages);
    pcollections.remove(badMessages);
    if (pcollections.size() != 0){
      throw new IllegalArgumentException("DLQ Router only supports PCollectionTuples split between two message groupings");
    }

    input.get(badMessages).apply(errorSink);

    return input.get(goodMessages);
  }
}
