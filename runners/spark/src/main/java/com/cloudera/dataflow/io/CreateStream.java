/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.dataflow.io;

import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.common.base.Preconditions;

/**
 * Create an input stream from Queue.
 *
 * @param <T> stream type
 */
public final class CreateStream<T> {

  private CreateStream() {
  }

  /**
   * Define the input stream to create from queue.
   *
   * @param queuedValues  defines the input stream
   * @param <T>           stream type
   * @return the queue that defines the input stream
   */
  public static <T> QueuedValues<T> fromQueue(Iterable<Iterable<T>> queuedValues) {
    return new QueuedValues<>(queuedValues);
  }

  public static final class QueuedValues<T> extends PTransform<PInput, PCollection<T>> {

    private final Iterable<Iterable<T>> queuedValues;

    QueuedValues(Iterable<Iterable<T>> queuedValues) {
      Preconditions.checkNotNull(queuedValues,
              "need to set the queuedValues of an Create.QueuedValues transform");
      this.queuedValues = queuedValues;
    }

    public Iterable<Iterable<T>> getQueuedValues() {
      return queuedValues;
    }

    @Override
    public PCollection<T> apply(PInput input) {
      // Spark streaming micro batches are bounded by default
      return PCollection.createPrimitiveOutputInternal(input.getPipeline(),
          WindowingStrategy.globalDefault(), PCollection.IsBounded.UNBOUNDED);
    }
  }

}
