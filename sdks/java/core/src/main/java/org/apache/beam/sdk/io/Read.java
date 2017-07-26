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
package org.apache.beam.sdk.io;

import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.NameUtils;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;

/**
 * A {@link PTransform} for reading from a {@link Source}.
 *
 * <p>Usage example:
 * <pre>
 * Pipeline p = Pipeline.create();
 * p.apply(Read.from(new MySource().withFoo("foo").withBar("bar")));
 * </pre>
 */
public class Read {

  /**
   * Returns a new {@code Read.Bounded} {@code PTransform} reading from the given
   * {@code BoundedSource}.
   */
  public static <T> Bounded<T> from(BoundedSource<T> source) {
    return new Bounded<>(null, source);
  }

  /**
   * Returns a new {@link Read.Unbounded} {@link PTransform} reading from the given
   * {@link UnboundedSource}.
   */
  public static <T> Unbounded<T> from(UnboundedSource<T, ?> source) {
    return new Unbounded<>(null, source);
  }

  /**
   * Helper class for building {@link Read} transforms.
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
  public static class Bounded<T> extends PTransform<PBegin, PCollection<T>> {
    private final BoundedSource<T> source;

    private Bounded(@Nullable String name, BoundedSource<T> source) {
      super(name);
      this.source = SerializableUtils.ensureSerializable(source);
    }

    @Override
    protected Coder<T> getDefaultOutputCoder() {
      return source.getOutputCoder();
    }

    @Override
    public final PCollection<T> expand(PBegin input) {
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
      return String.format("Read(%s)", NameUtils.approximateSimpleName(source));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .add(DisplayData.item("source", source.getClass())
            .withLabel("Read Source"))
          .include("source", source);
    }
  }

  /**
   * {@link PTransform} that reads from a {@link UnboundedSource}.
   */
  public static class Unbounded<T> extends PTransform<PBegin, PCollection<T>> {
    private final UnboundedSource<T, ?> source;

    private Unbounded(@Nullable String name, UnboundedSource<T, ?> source) {
      super(name);
      this.source = SerializableUtils.ensureSerializable(source);
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
      return source.getOutputCoder();
    }

    @Override
    public final PCollection<T> expand(PBegin input) {
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
      return String.format("Read(%s)", NameUtils.approximateSimpleName(source));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .add(DisplayData.item("source", source.getClass())
            .withLabel("Read Source"))
          .include("source", source);
    }
  }
}
