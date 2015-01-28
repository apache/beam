/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.dataflow.BasicSerializableSourceFormat;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

/**
 * The {@code PTransform} for reading from a {@code Source}.
 * <p>
 * Usage example:
 * <pre>
 * Pipeline p = Pipeline.create();
 * p.apply(ReadSource.from(new MySource().withFoo("foo").withBar("bar"))
 *                   .named("foobar"));
 * </pre>
 */
public class ReadSource {
  /**
   * Returns a new {@code ReadSource.Bound} {@code PTransform} with the given name.
   */
  @SuppressWarnings("unchecked")
  public static Bound<?> named(String name) {
    return new Bound(name, null);
  }

  /**
   * Returns a new unnamed {@code ReadSource.Bound} {@code PTransform} reading from the given
   * {@code Source}.
   */
  public static <T> Bound<T> from(Source<T> source) {
    return new Bound<>("", source);
  }

  /**
   * Implementation of the {@code ReadSource} {@code PTransform} builder.
   */
  public static class Bound<T>
      extends PTransform<PInput, PCollection<T>> {
    @Nullable
    private Source<T> source;

    private Bound(@Nullable String name, @Nullable Source<T> source) {
      super(name);
      this.source = source;
    }

    /**
     * Returns a new {@code ReadSource} {@code PTransform} that's like this one but
     * reads from the given {@code Source}.
     *
     * <p> Does not modify this object.
     */
    public <T> Bound<T> from(Source<T> source) {
      return new Bound<T>(getName(), source);
    }

    /**
     * Returns a new {@code ReadSource} {@code PTransform} that's like this one but
     * has the given name.
     *
     * <p> Does not modify this object.
     */
    public Bound<T> named(String name) {
      return new Bound<T>(name, source);
    }

    @Override
    protected Coder<T> getDefaultOutputCoder() {
      Preconditions.checkNotNull(source, "source must be set");
      return source.getDefaultOutputCoder();
    }

    @Override
    public final PCollection<T> apply(PInput input) {
      Preconditions.checkNotNull(source, "source must be set");
      source.validate();
      return PCollection.<T>createPrimitiveOutputInternal(new GlobalWindows())
          .setCoder(getDefaultOutputCoder());
    }

    /**
     * Returns the {@code Source} used to create this {@code ReadSource} {@code PTransform}.
     */
    @Nullable
    public Source<T> getSource() {
      return source;
    }

    static {
      DirectPipelineRunner.registerDefaultTransformEvaluator(
          Bound.class, new DirectPipelineRunner.TransformEvaluator<Bound>() {
            @Override
            public void evaluate(Bound transform, DirectPipelineRunner.EvaluationContext context) {
              BasicSerializableSourceFormat.evaluateReadHelper(transform, context);
            }
          });
    }
  }

}
