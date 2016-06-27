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
package org.apache.beam.runners.spark.io;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;


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

  /**
   * {@link PTransform} for queueing values.
   */
  public static final class QueuedValues<T> extends PTransform<PInput, PCollection<T>> {

    private final Iterable<Iterable<T>> queuedValues;

    QueuedValues(Iterable<Iterable<T>> queuedValues) {
      checkNotNull(
          queuedValues, "need to set the queuedValues of an Create.QueuedValues transform");
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
