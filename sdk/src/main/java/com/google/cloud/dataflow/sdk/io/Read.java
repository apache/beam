/*
 * Copyright (C) 2015 Google Inc.
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

import static com.google.cloud.dataflow.sdk.util.StringUtils.approximateSimpleName;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollection.IsBounded;
import com.google.cloud.dataflow.sdk.values.PInput;

import org.joda.time.Duration;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

/**
 * A {@link PTransform} for reading from a {@link Source}.
 *
 * <p>Usage example:
 * <pre>
 * Pipeline p = Pipeline.create();
 * p.apply(Read.from(new MySource().withFoo("foo").withBar("bar"))
 *             .named("foobar"));
 * </pre>
 */
public class Read {
  /**
   * Returns a new {@code Read} {@code PTransform} builder with the given name.
   */
  public static Builder named(String name) {
    return new Builder(name);
  }

  /**
   * Returns a new {@code Read.Bounded} {@code PTransform} reading from the given
   * {@code BoundedSource}.
   */
  public static <T> Bounded<T> from(BoundedSource<T> source) {
    return new Bounded<>(null, source);
  }

  /**
   * Returns a new {@code Read.Unbounded} {@code PTransform} reading from the given
   * {@code UnboundedSource}.
   */
  public static <T> Unbounded<T> from(UnboundedSource<T, ?> source) {
    return new Unbounded<>(null, source);
  }

  /**
   * Helper class for building {@code Read} transforms.
   */
  public static class Builder {
    private final String name;

    private Builder(String name) {
      this.name = name;
    }

    /**
     * Returns a new {@code Read.Bounded} {@code PTransform} reading from the given
     * {@code BoundedSource}.
     */
    public <T> Bounded<T> from(BoundedSource<T> source) {
      return new Bounded<>(name, source);
    }

    /**
     * Returns a new {@code Read.Unbounded} {@code PTransform} reading from the given
     * {@code UnboundedSource}.
     */
    public <T> Unbounded<T> from(UnboundedSource<T, ?> source) {
      return new Unbounded<>(name, source);
    }
  }

  /**
   * {@link PTransform} that reads from a {@link BoundedSource}.
   */
  public static class Bounded<T> extends PTransform<PInput, PCollection<T>> {
    private final BoundedSource<T> source;

    private Bounded(@Nullable String name, BoundedSource<T> source) {
      super(name);
      this.source = SerializableUtils.ensureSerializable(source);
    }

    /**
     * Returns a new {@code Bounded} {@code PTransform} that's like this one but
     * has the given name.
     *
     * <p>Does not modify this object.
     */
    public Bounded<T> named(String name) {
      return new Bounded<T>(name, source);
    }

    @Override
    protected Coder<T> getDefaultOutputCoder() {
      return source.getDefaultOutputCoder();
    }

    @Override
    public final PCollection<T> apply(PInput input) {
      source.validate();

      return PCollection.<T>createPrimitiveOutputInternal(input.getPipeline(),
          WindowingStrategy.globalDefault(), IsBounded.BOUNDED)
          .setCoder(getDefaultOutputCoder());
    }

    /**
     * Returns the {@code BoundedSource} used to create this {@code Read} {@code PTransform}.
     */
    public BoundedSource<T> getSource() {
      return source;
    }

    @Override
    public String getKindString() {
      return "Read(" + approximateSimpleName(source.getClass()) + ")";
    }

    static {
      registerDefaultTransformEvaluator();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void registerDefaultTransformEvaluator() {
      DirectPipelineRunner.registerDefaultTransformEvaluator(
          Bounded.class,
          new DirectPipelineRunner.TransformEvaluator<Bounded>() {
            @Override
            public void evaluate(
                Bounded transform, DirectPipelineRunner.EvaluationContext context) {
              evaluateReadHelper(transform, context);
            }

            private <T> void evaluateReadHelper(
                Read.Bounded<T> transform, DirectPipelineRunner.EvaluationContext context) {
              try {
                List<DirectPipelineRunner.ValueWithMetadata<T>> output = new ArrayList<>();
                BoundedSource<T> source = transform.getSource();
                try (BoundedSource.BoundedReader<T> reader =
                    source.createReader(context.getPipelineOptions())) {
                  for (boolean available = reader.start();
                      available;
                      available = reader.advance()) {
                    output.add(
                        DirectPipelineRunner.ValueWithMetadata.of(
                            WindowedValue.timestampedValueInGlobalWindow(
                                reader.getCurrent(), reader.getCurrentTimestamp())));
                  }
                }
                context.setPCollectionValuesWithMetadata(context.getOutput(transform), output);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
          });
    }
  }

  /**
   * {@link PTransform} that reads from a {@link UnboundedSource}.
   */
  public static class Unbounded<T> extends PTransform<PInput, PCollection<T>> {
    private final UnboundedSource<T, ?> source;

    private Unbounded(@Nullable String name, UnboundedSource<T, ?> source) {
      super(name);
      this.source = SerializableUtils.ensureSerializable(source);
    }

    /**
     * Returns a new {@code Unbounded} {@code PTransform} that's like this one but
     * has the given name.
     *
     * <p>Does not modify this object.
     */
    public Unbounded<T> named(String name) {
      return new Unbounded<T>(name, source);
    }

    /**
     * Returns a new {@link BoundedReadFromUnboundedSource} that reads a bounded amount
     * of data from the given {@link UnboundedSource}.  The bound is specified as a number
     * of records to read.
     *
     * <p>This may take a long time to execute if the splits of this source are slow to read
     * records.
     */
    public BoundedReadFromUnboundedSource<T> withMaxNumRecords(long maxNumRecords) {
      return new BoundedReadFromUnboundedSource<T>(source, maxNumRecords, null);
    }

    /**
     * Returns a new {@link BoundedReadFromUnboundedSource} that reads a bounded amount
     * of data from the given {@link UnboundedSource}.  The bound is specified as an amount
     * of time to read for.  Each split of the source will read for this much time.
     */
    public BoundedReadFromUnboundedSource<T> withMaxReadTime(Duration maxReadTime) {
      return new BoundedReadFromUnboundedSource<T>(source, Long.MAX_VALUE, maxReadTime);
    }

    @Override
    protected Coder<T> getDefaultOutputCoder() {
      return source.getDefaultOutputCoder();
    }

    @Override
    public final PCollection<T> apply(PInput input) {
      source.validate();

      return PCollection.<T>createPrimitiveOutputInternal(
          input.getPipeline(), WindowingStrategy.globalDefault(), IsBounded.UNBOUNDED);
    }

    /**
     * Returns the {@code UnboundedSource} used to create this {@code Read} {@code PTransform}.
     */
    public UnboundedSource<T, ?> getSource() {
      return source;
    }

    @Override
    public String getKindString() {
      return "Read(" + approximateSimpleName(source.getClass()) + ")";
    }
  }
}
