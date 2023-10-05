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

import java.io.Serializable;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;

/** Pair of a bit of user code (a "closure") and the {@link Requirements} needed to run it. */
public final class Contextful<ClosureT> implements Serializable {
  private final ClosureT closure;
  private final Requirements requirements;

  private Contextful(ClosureT closure, Requirements requirements) {
    this.closure = closure;
    this.requirements = requirements;
  }

  /** Returns the closure. */
  public ClosureT getClosure() {
    return closure;
  }

  /** Returns the requirements needed to run the closure. */
  public Requirements getRequirements() {
    return requirements;
  }

  /** Constructs a pair of the given closure and its requirements. */
  public static <ClosureT> Contextful<ClosureT> of(ClosureT closure, Requirements requirements) {
    return new Contextful<>(closure, requirements);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("closure", closure)
        .add("requirements", requirements)
        .toString();
  }

  /**
   * A function from an input to an output that may additionally access {@link Context} when
   * computing the result.
   */
  public interface Fn<InputT, OutputT> extends Serializable {
    /**
     * Invokes the function on the given input with the given context. The function may use the
     * context only for the capabilities declared in the {@link Contextful#getRequirements} of the
     * enclosing {@link Contextful}.
     */
    OutputT apply(InputT element, Context c) throws Exception;

    /** An accessor for additional capabilities available in {@link #apply}. */
    abstract class Context {
      /**
       * Accesses the given side input. The window in which it is accessed is unspecified, depends
       * on usage by the enclosing {@link PTransform}, and must be documented by that transform.
       */
      public <T> T sideInput(PCollectionView<T> view) {
        throw new UnsupportedOperationException();
      }

      /**
       * Convenience wrapper for creating a {@link Context} from a {@link DoFn.ProcessContext}, to
       * support the common case when a {@link PTransform} is invoking the {@link
       * Contextful#getClosure() closure} from inside a {@link DoFn}.
       */
      public static <InputT> Context wrapProcessContext(final DoFn<InputT, ?>.ProcessContext c) {
        return new ContextFromProcessContext<>(c);
      }

      private static class ContextFromProcessContext<InputT> extends Context {
        private final DoFn<InputT, ?>.ProcessContext c;

        ContextFromProcessContext(DoFn<InputT, ?>.ProcessContext c) {
          this.c = c;
        }

        @Override
        public <T> T sideInput(PCollectionView<T> view) {
          return c.sideInput(view);
        }
      }
    }
  }

  /**
   * Wraps a {@link ProcessFunction} as a {@link Contextful} of {@link Fn} with empty {@link
   * Requirements}.
   */
  public static <InputT, OutputT> Contextful<Fn<InputT, OutputT>> fn(
      final ProcessFunction<InputT, OutputT> fn) {
    return new Contextful<>((element, c) -> fn.apply(element), Requirements.empty());
  }

  /** Binary compatibility adapter for {@link #fn(ProcessFunction)}. */
  public static <InputT, OutputT> Contextful<Fn<InputT, OutputT>> fn(
      final SerializableFunction<InputT, OutputT> fn) {
    return fn((ProcessFunction<InputT, OutputT>) fn);
  }

  /** Same with {@link #of} but with better type inference behavior for the case of {@link Fn}. */
  public static <InputT, OutputT> Contextful<Fn<InputT, OutputT>> fn(
      final Fn<InputT, OutputT> fn, Requirements requirements) {
    return of(fn, requirements);
  }
}
