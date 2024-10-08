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
package org.apache.beam.sdk.transforms;

import java.util.function.Consumer;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

/**
 * A PTransform that returns its input, but also applies its input to an auxiliary PTransform, akin
 * to the shell {@code tee} command.
 *
 * <p>This can be useful to write out or otherwise process an intermediate transform without
 * breaking the linear flow of a chain of transforms, e.g.
 *
 * <pre><code>
 * {@literal PCollection<T>} input = ... ;
 * {@literal PCollection<T>} result =
 *     {@literal input.apply(...)}
 *     ...
 *     {@literal input.apply(Tee.of(someSideTransform)}
 *     ...
 *     {@literal input.apply(...)};
 * </code></pre>
 *
 * @param <T> the element type of the input PCollection
 */
public class Tee<T> extends PTransform<PCollection<T>, PCollection<T>> {
  private final PTransform<PCollection<T>, ?> consumer;

  /**
   * Returns a new Tee PTransform that will apply an auxilary transform to the input as well as pass
   * it on.
   *
   * @param consumer An additional PTransform that should process the input PCollection. Its output
   *     will be ignored.
   * @param <T> the type of the elements in the input {@code PCollection}.
   */
  public static <T> Tee<T> of(PTransform<PCollection<T>, ?> consumer) {
    return new Tee<>(consumer);
  }

  /**
   * Returns a new Tee PTransform that will apply an auxilary transform to the input as well as pass
   * it on.
   *
   * @param consumer An arbitrary {@link Consumer} that will be wrapped in a PTransform and applied
   *     to the input. Its output will be ignored.
   * @param <T> the type of the elements in the input {@code PCollection}.
   */
  public static <T> Tee<T> of(Consumer<PCollection<T>> consumer) {
    return of(
        new PTransform<PCollection<T>, PCollectionTuple>() {
          @Override
          public PCollectionTuple expand(PCollection<T> input) {
            consumer.accept(input);
            return PCollectionTuple.empty(input.getPipeline());
          }
        });
  }

  private Tee(PTransform<PCollection<T>, ?> consumer) {
    this.consumer = consumer;
  }

  @Override
  public PCollection<T> expand(PCollection<T> input) {
    input.apply(consumer);
    return input;
  }

  @Override
  protected String getKindString() {
    return "Tee(" + consumer.getName() + ")";
  }
}
