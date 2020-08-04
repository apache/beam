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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CollectionCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.OffsetBasedSource;
import org.apache.beam.sdk.io.OffsetBasedSource.OffsetBasedReader;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TimestampedValue.TimestampedValueCoder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * {@code Create<T>} takes a collection of elements of type {@code T} known when the pipeline is
 * constructed and returns a {@code PCollection<T>} containing the elements.
 *
 * <p>Example of use:
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * PCollection<Integer> pc = p.apply(Create.of(3, 4, 5).withCoder(BigEndianIntegerCoder.of()));
 *
 * Map<String, Integer> map = ...;
 * PCollection<KV<String, Integer>> pt =
 *     p.apply(Create.of(map)
 *      .withCoder(KvCoder.of(StringUtf8Coder.of(),
 *                            BigEndianIntegerCoder.of())));
 * }</pre>
 *
 * <p>{@code Create} can automatically determine the {@code Coder} to use if all elements have the
 * same run-time class, and a default coder is registered for that class. See {@link CoderRegistry}
 * for details on how defaults are determined.
 *
 * <p>If a coder can not be inferred, {@link Create.Values#withCoder} must be called explicitly to
 * set the encoding of the resulting {@code PCollection}.
 *
 * <p>A good use for {@code Create} is when a {@code PCollection} needs to be created without
 * dependencies on files or other external entities. This is especially useful during testing.
 *
 * <p>Caveat: {@code Create} only supports small in-memory datasets.
 *
 * @param <T> the type of the elements of the resulting {@code PCollection}
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class Create<T> {
  /**
   * Returns a new {@code Create.Values} transform that produces a {@link PCollection} containing
   * elements of the provided {@code Iterable}.
   *
   * <p>The argument should not be modified after this is called.
   *
   * <p>The elements of the output {@link PCollection} will have a timestamp of negative infinity,
   * see {@link Create#timestamped} for a way of creating a {@code PCollection} with timestamped
   * elements.
   *
   * <p>By default, {@code Create.Values} can automatically determine the {@code Coder} to use if
   * all elements have the same non-parameterized run-time class, and a default coder is registered
   * for that class. See {@link CoderRegistry} for details on how defaults are determined.
   * Otherwise, use {@link Create.Values#withCoder} to set the coder explicitly.
   */
  public static <T> Values<T> of(Iterable<T> elems) {
    return new Values<>(elems, Optional.absent(), Optional.absent());
  }

  /**
   * Returns a new {@code Create.Values} transform that produces a {@link PCollection} containing
   * the specified elements.
   *
   * <p>The elements will have a timestamp of negative infinity, see {@link Create#timestamped} for
   * a way of creating a {@code PCollection} with timestamped elements.
   *
   * <p>The arguments should not be modified after this is called.
   *
   * <p>By default, {@code Create.Values} can automatically determine the {@code Coder} to use if
   * all elements have the same non-parameterized run-time class, and a default coder is registered
   * for that class. See {@link CoderRegistry} for details on how defaults are determined.
   * Otherwise, use {@link Create.Values#withCoder} to set the coder explicitly.
   */
  @SafeVarargs
  public static <T> Values<T> of(@Nullable T elem, @Nullable T... elems) {
    // This can't be an ImmutableList, as it may accept nulls
    List<T> input = new ArrayList<>(elems.length + 1);
    input.add(elem);
    input.addAll(Arrays.asList(elems));
    return of(input);
  }

  /**
   * Returns a new {@code Create.Values} transform that produces an empty {@link PCollection} of
   * rows.
   */
  @Experimental(Kind.SCHEMAS)
  public static Values<Row> empty(Schema schema) {
    return new Values<Row>(
        new ArrayList<>(), Optional.of(SchemaCoder.of(schema)), Optional.absent());
  }

  /**
   * Returns a new {@code Create.Values} transform that produces an empty {@link PCollection}.
   *
   * <p>The elements will have a timestamp of negative infinity, see {@link Create#timestamped} for
   * a way of creating a {@code PCollection} with timestamped elements.
   *
   * <p>Since there are no elements, the {@code Coder} cannot be automatically determined. Instead,
   * the {@code Coder} is provided via the {@code coder} argument.
   */
  public static <T> Values<T> empty(Coder<T> coder) {
    return new Values<>(new ArrayList<>(), Optional.of(coder), Optional.absent());
  }

  /**
   * Returns a new {@code Create.Values} transform that produces an empty {@link PCollection}.
   *
   * <p>The elements will have a timestamp of negative infinity, see {@link Create#timestamped} for
   * a way of creating a {@code PCollection} with timestamped elements.
   *
   * <p>Since there are no elements, the {@code Coder} cannot be automatically determined. Instead,
   * the {@code Coder} is determined from given {@code TypeDescriptor<T>}. Note that a default coder
   * must be registered for the class described in the {@code TypeDescriptor<T>}.
   */
  public static <T> Values<T> empty(TypeDescriptor<T> type) {
    return new Values<>(new ArrayList<>(), Optional.absent(), Optional.of(type));
  }

  /**
   * Returns a new {@code Create.Values} transform that produces a {@link PCollection} of {@link
   * KV}s corresponding to the keys and values of the specified {@code Map}.
   *
   * <p>The elements will have a timestamp of negative infinity, see {@link Create#timestamped} for
   * a way of creating a {@code PCollection} with timestamped elements.
   *
   * <p>By default, {@code Create.Values} can automatically determine the {@code Coder} to use if
   * all elements have the same non-parameterized run-time class, and a default coder is registered
   * for that class. See {@link CoderRegistry} for details on how defaults are determined.
   * Otherwise, use {@link Create.Values#withCoder} to set the coder explicitly.
   */
  public static <K, V> Values<KV<K, V>> of(Map<K, V> elems) {
    List<KV<K, V>> kvs = new ArrayList<>(elems.size());
    for (Map.Entry<K, V> entry : elems.entrySet()) {
      kvs.add(KV.of(entry.getKey(), entry.getValue()));
    }
    return of(kvs);
  }

  /**
   * Returns an {@link OfValueProvider} transform that produces a {@link PCollection} of a single
   * element provided by the given {@link ValueProvider}.
   */
  public static <T> OfValueProvider<T> ofProvider(ValueProvider<T> provider, Coder<T> coder) {
    return new OfValueProvider<>(provider, coder);
  }

  /**
   * Returns a new {@link Create.TimestampedValues} transform that produces a {@link PCollection}
   * containing the elements of the provided {@code Iterable} with the specified timestamps.
   *
   * <p>The argument should not be modified after this is called.
   *
   * <p>By default, {@code Create.TimestampedValues} can automatically determine the {@code Coder}
   * to use if all elements have the same non-parameterized run-time class, and a default coder is
   * registered for that class. See {@link CoderRegistry} for details on how defaults are
   * determined. Otherwise, use {@link Create.TimestampedValues#withCoder} to set the coder
   * explicitly.
   */
  public static <T> TimestampedValues<T> timestamped(Iterable<TimestampedValue<T>> elems) {
    return new TimestampedValues<>(elems, Optional.absent(), Optional.absent());
  }

  /**
   * Returns a new {@link Create.TimestampedValues} transform that produces a {@link PCollection}
   * containing the specified elements with the specified timestamps.
   *
   * <p>The arguments should not be modified after this is called.
   */
  @SafeVarargs
  public static <T> TimestampedValues<T> timestamped(
      TimestampedValue<T> elem, @SuppressWarnings("unchecked") TimestampedValue<T>... elems) {
    return timestamped(ImmutableList.<TimestampedValue<T>>builder().add(elem).add(elems).build());
  }

  /**
   * Returns a new root transform that produces a {@link PCollection} containing the specified
   * elements with the specified timestamps.
   *
   * <p>The arguments should not be modified after this is called.
   *
   * <p>By default, {@code Create.TimestampedValues} can automatically determine the {@code Coder}
   * to use if all elements have the same non-parameterized run-time class, and a default coder is
   * registered for that class. See {@link CoderRegistry} for details on how defaults are
   * determined. Otherwise, use {@link Create.TimestampedValues#withCoder} to set the coder
   * explicitly.
   *
   * @throws IllegalArgumentException if there are a different number of values and timestamps
   */
  public static <T> TimestampedValues<T> timestamped(
      Iterable<T> values, Iterable<Long> timestamps) {
    List<TimestampedValue<T>> elems = new ArrayList<>();
    Iterator<T> valueIter = values.iterator();
    Iterator<Long> timestampIter = timestamps.iterator();
    while (valueIter.hasNext() && timestampIter.hasNext()) {
      elems.add(TimestampedValue.of(valueIter.next(), new Instant(timestampIter.next())));
    }
    checkArgument(
        !valueIter.hasNext() && !timestampIter.hasNext(),
        "Expect sizes of values and timestamps are same.");
    return timestamped(elems);
  }

  /////////////////////////////////////////////////////////////////////////////

  /** A {@code PTransform} that creates a {@code PCollection} from a set of in-memory objects. */
  public static class Values<T> extends PTransform<PBegin, PCollection<T>> {
    /**
     * Returns a {@link Create.Values} PTransform like this one that uses the given {@code Coder<T>}
     * to decode each of the objects into a value of type {@code T}.
     *
     * <p>By default, {@code Create.Values} can automatically determine the {@code Coder} to use if
     * all elements have the same non-parameterized run-time class, and a default coder is
     * registered for that class. See {@link CoderRegistry} for details on how defaults are
     * determined.
     *
     * <p>Note that for {@link Create.Values} with no elements, the {@link VoidCoder} is used.
     */
    public Values<T> withCoder(Coder<T> coder) {
      return new Values<>(elems, Optional.of(coder), typeDescriptor);
    }

    /**
     * Returns a {@link Create.Values} PTransform like this one that uses the given {@code Schema}
     * to represent objects.
     */
    @Experimental(Kind.SCHEMAS)
    public Values<T> withSchema(
        Schema schema,
        TypeDescriptor<T> typeDescriptor,
        SerializableFunction<T, Row> toRowFunction,
        SerializableFunction<Row, T> fromRowFunction) {
      return withCoder(SchemaCoder.of(schema, typeDescriptor, toRowFunction, fromRowFunction));
    }

    /**
     * Returns a {@link Create.Values} PTransform like this one that uses the given {@code Schema}
     * to represent objects.
     */
    @Experimental(Kind.SCHEMAS)
    public Values<T> withRowSchema(Schema schema) {
      return withCoder((SchemaCoder<T>) SchemaCoder.of(schema));
    }

    /**
     * Returns a {@link Create.Values} PTransform like this one that uses the given {@code
     * TypeDescriptor<T>} to determine the {@code Coder} to use to decode each of the objects into a
     * value of type {@code T}. Note that a default coder must be registered for the class described
     * in the {@code TypeDescriptor<T>}.
     *
     * <p>By default, {@code Create.Values} can automatically determine the {@code Coder} to use if
     * all elements have the same non-parameterized run-time class, and a default coder is
     * registered for that class. See {@link CoderRegistry} for details on how defaults are
     * determined.
     *
     * <p>Note that for {@link Create.Values} with no elements, the {@link VoidCoder} is used.
     */
    public Values<T> withType(TypeDescriptor<T> type) {
      return new Values<>(elems, coder, Optional.of(type));
    }

    public Iterable<T> getElements() {
      return elems;
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      Coder<T> coder;
      try {
        CoderRegistry coderRegistry = input.getPipeline().getCoderRegistry();
        SchemaRegistry schemaRegistry = input.getPipeline().getSchemaRegistry();
        coder = this.coder.isPresent() ? this.coder.get() : null;
        if (coder == null) {
          if (typeDescriptor.isPresent()) {
            try {
              coder =
                  SchemaCoder.of(
                      schemaRegistry.getSchema(typeDescriptor.get()),
                      typeDescriptor.get(),
                      schemaRegistry.getToRowFunction(typeDescriptor.get()),
                      schemaRegistry.getFromRowFunction(typeDescriptor.get()));
            } catch (NoSuchSchemaException e) {
              // No schema registered.
            }
            if (coder == null) {
              coder = coderRegistry.getCoder(typeDescriptor.get());
            }
          } else {
            coder = getDefaultCreateCoder(coderRegistry, schemaRegistry, elems);
          }
        }
      } catch (CannotProvideCoderException e) {
        throw new IllegalArgumentException(
            "Unable to infer a coder and no Coder was specified. "
                + "Please set a coder by invoking Create.withCoder() explicitly "
                + " or a schema by invoking Create.withSchema().",
            e);
      }
      try {
        CreateSource<T> source = CreateSource.fromIterable(elems, coder);
        return input.getPipeline().apply(Read.from(source));
      } catch (IOException e) {
        throw new RuntimeException(
            String.format("Unable to apply Create %s using Coder %s.", this, coder), e);
      }
    }

    /////////////////////////////////////////////////////////////////////////////

    /** The elements of the resulting PCollection. */
    private final transient Iterable<T> elems;

    /** The coder used to encode the values to and from a binary representation. */
    private final transient Optional<Coder<T>> coder;

    /** The value type. */
    private final transient Optional<TypeDescriptor<T>> typeDescriptor;

    /**
     * Constructs a {@code Create.Values} transform that produces a {@link PCollection} containing
     * the specified elements.
     *
     * <p>The arguments should not be modified after this is called.
     */
    private Values(
        Iterable<T> elems, Optional<Coder<T>> coder, Optional<TypeDescriptor<T>> typeDescriptor) {
      this.elems = elems;
      this.coder = coder;
      this.typeDescriptor = typeDescriptor;
    }

    @VisibleForTesting
    static class CreateSource<T> extends OffsetBasedSource<T> {
      private final List<byte[]> allElementsBytes;
      private final long totalSize;
      private final Coder<T> coder;

      public static <T> CreateSource<T> fromIterable(Iterable<T> elements, Coder<T> elemCoder)
          throws CoderException, IOException {
        ImmutableList.Builder<byte[]> allElementsBytes = ImmutableList.builder();
        long totalSize = 0L;
        for (T element : elements) {
          byte[] bytes = CoderUtils.encodeToByteArray(elemCoder, element);
          allElementsBytes.add(bytes);
          totalSize += bytes.length;
        }
        return new CreateSource<>(allElementsBytes.build(), totalSize, elemCoder);
      }

      /**
       * Create a new source with the specified bytes. The new source owns the input element bytes,
       * which must not be modified after this constructor is called.
       */
      private CreateSource(List<byte[]> elementBytes, long totalSize, Coder<T> coder) {
        super(0, elementBytes.size(), 1);
        this.allElementsBytes = ImmutableList.copyOf(elementBytes);
        this.totalSize = totalSize;
        this.coder = coder;
      }

      @Override
      public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
        return totalSize;
      }

      @Override
      public BoundedSource.BoundedReader<T> createReader(PipelineOptions options)
          throws IOException {
        return new BytesReader<>(this);
      }

      @Override
      public Coder<T> getOutputCoder() {
        return coder;
      }

      @Override
      public long getMaxEndOffset(PipelineOptions options) throws Exception {
        return allElementsBytes.size();
      }

      @Override
      public OffsetBasedSource<T> createSourceForSubrange(long start, long end) {
        List<byte[]> primaryElems = allElementsBytes.subList((int) start, (int) end);
        long primarySizeEstimate =
            (long) (totalSize * primaryElems.size() / (double) allElementsBytes.size());
        return new CreateSource<>(primaryElems, primarySizeEstimate, coder);
      }

      @Override
      public long getBytesPerOffset() {
        if (allElementsBytes.isEmpty()) {
          return 1L;
        }
        return Math.max(1, totalSize / allElementsBytes.size());
      }
    }

    private static class BytesReader<T> extends OffsetBasedReader<T> {
      private int index;
      /**
       * Use an optional to distinguish between null next element (as Optional.absent()) and no next
       * element (next is null).
       */
      private @Nullable Optional<T> next;

      public BytesReader(CreateSource<T> source) {
        super(source);
        index = -1;
      }

      @Override
      public @Nullable T getCurrent() throws NoSuchElementException {
        if (next == null) {
          throw new NoSuchElementException();
        }
        return next.orNull();
      }

      @Override
      public void close() throws IOException {}

      @Override
      protected long getCurrentOffset() {
        return index;
      }

      @Override
      protected boolean startImpl() throws IOException {
        return advanceImpl();
      }

      @Override
      public synchronized CreateSource<T> getCurrentSource() {
        return (CreateSource<T>) super.getCurrentSource();
      }

      @Override
      protected boolean advanceImpl() throws IOException {
        CreateSource<T> source = getCurrentSource();
        if (index + 1 >= source.allElementsBytes.size()) {
          next = null;
          return false;
        }
        index++;
        next =
            Optional.fromNullable(
                CoderUtils.decodeFromByteArray(source.coder, source.allElementsBytes.get(index)));
        return true;
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #ofProvider}. */
  public static class OfValueProvider<T> extends PTransform<PBegin, PCollection<T>> {
    private final ValueProvider<T> provider;
    private final Coder<T> coder;

    private OfValueProvider(ValueProvider<T> provider, Coder<T> coder) {
      this.provider = checkNotNull(provider, "provider");
      this.coder = checkNotNull(coder, "coder");
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      if (provider.isAccessible()) {
        Values<T> values = Create.of(provider.get());
        return input.apply(values.withCoder(coder));
      }
      return input
          .apply(Create.of((Void) null))
          .apply(
              MapElements.via(
                  new SimpleFunction<Void, T>() {
                    @Override
                    public T apply(Void input) {
                      return provider.get();
                    }
                  }))
          .setCoder(coder);
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@code PTransform} that creates a {@code PCollection} whose elements have associated
   * timestamps.
   */
  public static class TimestampedValues<T> extends PTransform<PBegin, PCollection<T>> {
    /**
     * Returns a {@link Create.TimestampedValues} PTransform like this one that uses the given
     * {@code Coder<T>} to decode each of the objects into a value of type {@code T}.
     *
     * <p>By default, {@code Create.TimestampedValues} can automatically determine the {@code Coder}
     * to use if all elements have the same non-parameterized run-time class, and a default coder is
     * registered for that class. See {@link CoderRegistry} for details on how defaults are
     * determined.
     *
     * <p>Note that for {@link Create.TimestampedValues with no elements}, the {@link VoidCoder} is
     * used.
     */
    public TimestampedValues<T> withCoder(Coder<T> coder) {
      return new TimestampedValues<>(timestampedElements, Optional.of(coder), typeDescriptor);
    }

    /**
     * Returns a {@link Create.TimestampedValues} PTransform like this one that uses the given
     * {@code Schema} to represent objects.
     */
    @Experimental(Kind.SCHEMAS)
    public TimestampedValues<T> withSchema(
        Schema schema,
        TypeDescriptor<T> typeDescriptor,
        SerializableFunction<T, Row> toRowFunction,
        SerializableFunction<Row, T> fromRowFunction) {
      return withCoder(SchemaCoder.of(schema, typeDescriptor, toRowFunction, fromRowFunction));
    }

    /**
     * Returns a {@link Create.TimestampedValues} PTransform like this one that uses the given
     * {@code TypeDescriptor<T>} to determine the {@code Coder} to use to decode each of the objects
     * into a value of type {@code T}. Note that a default coder must be registered for the class
     * described in the {@code TypeDescriptor<T>}.
     *
     * <p>By default, {@code Create.TimestampedValues} can automatically determine the {@code Coder}
     * to use if all elements have the same non-parameterized run-time class, and a default coder is
     * registered for that class. See {@link CoderRegistry} for details on how defaults are
     * determined.
     *
     * <p>Note that for {@link Create.TimestampedValues} with no elements, the {@link VoidCoder} is
     * used.
     */
    public TimestampedValues<T> withType(TypeDescriptor<T> type) {
      return new TimestampedValues<>(timestampedElements, elementCoder, Optional.of(type));
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      try {
        Coder<T> coder = null;
        CoderRegistry coderRegistry = input.getPipeline().getCoderRegistry();
        SchemaRegistry schemaRegistry = input.getPipeline().getSchemaRegistry();
        if (elementCoder.isPresent()) {
          coder = elementCoder.get();
        } else if (typeDescriptor.isPresent()) {
          try {
            coder =
                SchemaCoder.of(
                    schemaRegistry.getSchema(typeDescriptor.get()),
                    typeDescriptor.get(),
                    schemaRegistry.getToRowFunction(typeDescriptor.get()),
                    schemaRegistry.getFromRowFunction(typeDescriptor.get()));
          } catch (NoSuchSchemaException e) {
            // No schema registered.
          }
          if (coder == null) {
            coder = coderRegistry.getCoder(typeDescriptor.get());
          }
        } else {
          Iterable<T> rawElements =
              Iterables.transform(timestampedElements, TimestampedValue::getValue);
          coder = getDefaultCreateCoder(coderRegistry, schemaRegistry, rawElements);
        }

        PCollection<TimestampedValue<T>> intermediate =
            Pipeline.applyTransform(
                input, Create.of(timestampedElements).withCoder(TimestampedValueCoder.of(coder)));

        PCollection<T> output = intermediate.apply(ParDo.of(new ConvertTimestamps<>()));
        output.setCoder(coder);
        return output;
      } catch (CannotProvideCoderException e) {
        throw new IllegalArgumentException(
            "Unable to infer a coder and no Coder was specified. "
                + "Please set a coder by invoking CreateTimestamped.withCoder() explicitly.",
            e);
      }
    }

    /////////////////////////////////////////////////////////////////////////////

    /** The timestamped elements of the resulting PCollection. */
    private final transient Iterable<TimestampedValue<T>> timestampedElements;

    /** The coder used to encode the values to and from a binary representation. */
    private final transient Optional<Coder<T>> elementCoder;

    /** The value type. */
    private final transient Optional<TypeDescriptor<T>> typeDescriptor;

    private TimestampedValues(
        Iterable<TimestampedValue<T>> timestampedElements,
        Optional<Coder<T>> elementCoder,
        Optional<TypeDescriptor<T>> typeDescriptor) {
      this.timestampedElements = timestampedElements;
      this.elementCoder = elementCoder;
      this.typeDescriptor = typeDescriptor;
    }

    private static class ConvertTimestamps<T> extends DoFn<TimestampedValue<T>, T> {
      @ProcessElement
      public void processElement(@Element TimestampedValue<T> element, OutputReceiver<T> r) {
        r.outputWithTimestamp(element.getValue(), element.getTimestamp());
      }
    }
  }

  private static <T> Coder<T> getDefaultCreateCoder(
      CoderRegistry coderRegistry, SchemaRegistry schemaRegistry, Iterable<T> elems)
      throws CannotProvideCoderException {
    checkArgument(
        !Iterables.isEmpty(elems),
        "Can not determine a default Coder for a 'Create' PTransform that "
            + "has no elements.  Either add elements, call Create.empty(Coder),"
            + " Create.empty(TypeDescriptor), or call 'withCoder(Coder)' or "
            + "'withType(TypeDescriptor)' on the PTransform.");
    // First try to deduce a coder using the types of the elements.
    Class<?> elementClazz = Void.class;
    for (T elem : elems) {
      if (elem == null) {
        continue;
      }
      Class<?> clazz = elem.getClass();
      if (elementClazz.equals(Void.class)) {
        elementClazz = clazz;
      } else if (!elementClazz.equals(clazz)) {
        // Elements are not the same type, require a user-specified coder.
        throw new CannotProvideCoderException(
            String.format(
                "Cannot provide coder for %s: The elements are not all of the same class.",
                Create.class.getSimpleName()));
      }
    }

    TypeDescriptor<T> typeDescriptor = (TypeDescriptor<T>) TypeDescriptor.of(elementClazz);
    if (elementClazz.getTypeParameters().length == 0) {
      try {
        Coder<T> coder =
            SchemaCoder.of(
                schemaRegistry.getSchema(typeDescriptor),
                typeDescriptor,
                schemaRegistry.getToRowFunction(typeDescriptor),
                schemaRegistry.getFromRowFunction(typeDescriptor));
        return coder;
      } catch (NoSuchSchemaException e) {
        // No schema.
      }

      try {
        @SuppressWarnings("unchecked") // elementClazz is a wildcard type
        Coder<T> coder = (Coder<T>) coderRegistry.getCoder(typeDescriptor);
        return coder;
      } catch (CannotProvideCoderException exc) {
        // Can't get a coder from the class of the elements, try from elements next.
      }
    }

    // If that fails, try to deduce a coder using the elements themselves
    return (Coder<T>) inferCoderFromObjects(coderRegistry, schemaRegistry, elems);
  }

  /**
   * Attempts to infer the {@link Coder} of the elements ensuring that the returned coder is
   * equivalent for all elements.
   */
  private static Coder<?> inferCoderFromObjects(
      CoderRegistry coderRegistry, SchemaRegistry schemaRegistry, Iterable<?> elems)
      throws CannotProvideCoderException {
    Optional<Coder<?>> coder = Optional.absent();
    for (Object elem : elems) {
      Coder<?> c = inferCoderFromObject(coderRegistry, schemaRegistry, elem);
      if (!coder.isPresent()) {
        coder = (Optional) Optional.of(c);
      } else if (!Objects.equals(c, coder.get())) {
        throw new CannotProvideCoderException(
            "Cannot provide coder for elements of "
                + Create.class.getSimpleName()
                + ":"
                + " For their common class, no coder could be provided."
                + " Based on their values, they do not all default to the same Coder.");
      }
    }
    if (coder.isPresent()) {
      return coder.get();
    }

    throw new CannotProvideCoderException(
        "Cannot provide coder for elements of "
            + Create.class.getSimpleName()
            + ":"
            + " For their common class, no coder could be provided."
            + " Based on their values, no coder could be inferred.");
  }

  /**
   * Attempt to infer the type for some very common Apache Beam parameterized types.
   *
   * <p>TODO: Instead, build a TypeDescriptor so that the {@link CoderRegistry} is invoked for the
   * type instead of hard coding the coders for common types.
   */
  private static Coder<?> inferCoderFromObject(
      CoderRegistry coderRegistry, SchemaRegistry schemaRegistry, Object o)
      throws CannotProvideCoderException {

    if (o == null) {
      return VoidCoder.of();
    }

    try {
      return SchemaCoder.of(
          schemaRegistry.getSchema(o.getClass()),
          TypeDescriptor.of(o.getClass()),
          (SerializableFunction) schemaRegistry.getToRowFunction(o.getClass()),
          (SerializableFunction) schemaRegistry.getFromRowFunction(o.getClass()));
    } catch (NoSuchSchemaException e) {
      // No schema.
    }

    if (o instanceof TimestampedValue) {
      return TimestampedValueCoder.of(
          inferCoderFromObject(coderRegistry, schemaRegistry, ((TimestampedValue) o).getValue()));
    } else if (o instanceof List) {
      return ListCoder.of(inferCoderFromObjects(coderRegistry, schemaRegistry, (Iterable) o));
    } else if (o instanceof Set) {
      return SetCoder.of(inferCoderFromObjects(coderRegistry, schemaRegistry, (Iterable) o));
    } else if (o instanceof Collection) {
      return CollectionCoder.of(inferCoderFromObjects(coderRegistry, schemaRegistry, (Iterable) o));
    } else if (o instanceof Iterable) {
      return IterableCoder.of(inferCoderFromObjects(coderRegistry, schemaRegistry, (Iterable) o));
    } else if (o instanceof Map) {
      return MapCoder.of(
          inferCoderFromObjects(coderRegistry, schemaRegistry, ((Map) o).keySet()),
          inferCoderFromObjects(coderRegistry, schemaRegistry, ((Map) o).entrySet()));
    } else if (o instanceof KV) {
      return KvCoder.of(
          inferCoderFromObject(coderRegistry, schemaRegistry, ((KV) o).getKey()),
          inferCoderFromObject(coderRegistry, schemaRegistry, ((KV) o).getValue()));
    } else {
      return coderRegistry.getCoder(o.getClass());
    }
  }
}
