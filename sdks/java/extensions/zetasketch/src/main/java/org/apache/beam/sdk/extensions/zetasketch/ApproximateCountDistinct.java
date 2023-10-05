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
package org.apache.beam.sdk.extensions.zetasketch;

import com.google.auto.value.AutoValue;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.zetasketch.HllCount.Init.Builder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Contextful.Fn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/**
 * {@code PTransform}s for estimating the number of distinct elements in a {@code PCollection}, or
 * the number of distinct values associated with each key in a {@code PCollection} of {@code KV}s.
 *
 * <p>We make use of the {@link HllCount} implementation for this transform. Please use {@link
 * HllCount} directly if you need access to the sketches.
 *
 * <p>If the object is not one of {@link Byte[]} {@link Integer} {@link Double} {@link String} make
 * use of {@link Globally#via} or {@link PerKey#via}
 *
 * <h3>Examples</h3>
 *
 * <h4>Example 1: Approximate Count of Ints {@code PCollection<Integer>} and specify precision</h4>
 *
 * <pre>{@code
 * p.apply("Int", Create.of(ints)).apply("IntHLL", ApproximateCountDistinct.globally()
 *   .withPercision(PRECISION));
 *
 * }</pre>
 *
 * <h4>Example 2: Approximate Count of Key Value {@code PCollection<KV<Integer,Foo>>}</h4>
 *
 * <pre>{@code
 * PCollection<KV<Integer, Long>> result =
 *   p.apply("Long", Create.of(longs)).apply("LongHLL", ApproximateCountDistinct.perKey());
 *
 * }</pre>
 *
 * <h4>Example 3: Approximate Count of Key Value {@code PCollection<KV<Integer,Foo>>}</h4>
 *
 * <pre>{@code
 * PCollection<KV<Integer, Foo>> approxResultInteger =
 *   p.apply("Int", Create.of(Foo))
 *     .apply("IntHLL", ApproximateCountDistinct.<Integer, KV<Integer, Integer>>perKey()
 *       .via(kv -> KV.of(kv.getKey(), (long) kv.getValue().hashCode())));
 * }</pre>
 */
public class ApproximateCountDistinct {

  private static final List<TypeDescriptor<?>> HLL_IMPLEMENTED_TYPES =
      ImmutableList.of(
          TypeDescriptors.strings(),
          TypeDescriptors.longs(),
          TypeDescriptors.integers(),
          new TypeDescriptor<byte[]>() {});

  public static <T> Globally<T> globally() {
    return new AutoValue_ApproximateCountDistinct_Globally.Builder<T>()
        .setPrecision(HllCount.DEFAULT_PRECISION)
        .build();
  }

  public static <K, V> PerKey<K, V> perKey() {
    return new AutoValue_ApproximateCountDistinct_PerKey.Builder<K, V>()
        .setPrecision(HllCount.DEFAULT_PRECISION)
        .build();
  }

  public static <T> Combine.CombineFn<T, ?, byte[]> getUdaf(TypeDescriptor<T> input) {
    return ApproximateCountDistinct.<T>builderForType(input).asUdaf();
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * {@code PTransform} for estimating the number of distinct elements in a {@code PCollection}.
   *
   * @param <T> the type of the elements in the input {@code PCollection}
   */
  @AutoValue
  public abstract static class Globally<T> extends PTransform<PCollection<T>, PCollection<Long>> {

    public abstract int getPrecision();

    public abstract Builder<T> toBuilder();

    @Nullable
    public abstract Contextful<Fn<T, Long>> getMapping();

    @AutoValue.Builder
    public abstract static class Builder<T> {

      public abstract Builder<T> setPrecision(int precision);

      public abstract Builder<T> setMapping(Contextful<Fn<T, Long>> value);

      public abstract Globally<T> build();
    }

    public Globally<T> via(ProcessFunction<T, Long> fn) {

      return toBuilder().setMapping(Contextful.<T, Long>fn(fn)).build();
    }

    public <V> Globally<V> withPercision(Integer withPercision) {
      @SuppressWarnings("unchecked")
      Globally<V> globally = (Globally<V>) toBuilder().setPrecision(withPercision).build();
      return globally;
    }

    @Override
    public PCollection<Long> expand(PCollection<T> input) {

      TypeDescriptor<T> type = input.getCoder().getEncodedTypeDescriptor();

      if (HLL_IMPLEMENTED_TYPES.contains(type)) {

        HllCount.Init.Builder<T> builder = builderForType(type);

        return input.apply(builder.globally()).apply(HllCount.Extract.globally());
      }

      // Boiler plate to avoid  [argument] NonNull vs Nullable
      Contextful<Fn<T, Long>> mapping = getMapping();

      if (mapping != null) {
        return input
            .apply(MapElements.into(TypeDescriptors.longs()).via(mapping))
            .apply(HllCount.Init.forLongs().globally())
            .apply(HllCount.Extract.globally());
      }

      throw new IllegalArgumentException(
          String.format(
              "%s supports Integer,"
                  + " Long, String and byte[] objects directly. For other types you must provide a Mapping function.",
              this.getClass().getCanonicalName()));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      ApproximateCountDistinct.populateDisplayData(builder, getPrecision());
    }
  }

  @AutoValue
  public abstract static class PerKey<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Long>>> {

    public abstract Integer getPrecision();

    @Nullable
    public abstract Contextful<Fn<KV<K, V>, KV<K, Long>>> getMapping();

    public abstract Builder<K, V> toBuilder();

    @AutoValue.Builder
    public abstract static class Builder<K, V> {

      public abstract Builder<K, V> setPrecision(Integer precision);

      public abstract Builder<K, V> setMapping(Contextful<Fn<KV<K, V>, KV<K, Long>>> value);

      public abstract PerKey<K, V> build();
    }

    public <K2, V2> PerKey<K2, V2> withPercision(Integer withPercision) {
      // Work around for loss of type inference when using API.
      @SuppressWarnings("unchecked")
      PerKey<K2, V2> perKey = (PerKey<K2, V2>) this.toBuilder().setPrecision(withPercision).build();
      return perKey;
    }

    public PerKey<K, V> via(ProcessFunction<KV<K, V>, KV<K, Long>> fn) {

      return this.toBuilder().setMapping(Contextful.<KV<K, V>, KV<K, Long>>fn(fn)).build();
    }

    @Override
    public PCollection<KV<K, Long>> expand(PCollection<KV<K, V>> input) {

      Coder<V> coder = ((KvCoder<K, V>) input.getCoder()).getValueCoder();

      TypeDescriptor<V> type = coder.getEncodedTypeDescriptor();

      if (HLL_IMPLEMENTED_TYPES.contains(type)) {

        HllCount.Init.Builder<V> builder = builderForType(type);

        return input.apply(builder.perKey()).apply(HllCount.Extract.perKey());
      }

      // Boiler plate to avoid  [argument] NonNull vs Nullable
      Contextful<Fn<KV<K, V>, KV<K, Long>>> mapping = getMapping();

      if (mapping != null) {
        Coder<K> keyCoder = ((KvCoder<K, V>) input.getCoder()).getKeyCoder();
        return input
            .apply(
                MapElements.into(
                        TypeDescriptors.kvs(
                            keyCoder.getEncodedTypeDescriptor(), TypeDescriptors.longs()))
                    .via(mapping))
            .apply(HllCount.Init.forLongs().perKey())
            .apply(HllCount.Extract.perKey());
      }

      throw new IllegalArgumentException(
          String.format(
              "%s supports Integer,"
                  + " Long, String and byte[] objects directly not for %s type, you must provide a Mapping use via.",
              this.getClass().getCanonicalName(), type.toString()));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      ApproximateCountDistinct.populateDisplayData(builder, getPrecision());
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  private static void populateDisplayData(DisplayData.Builder builder, Integer precision) {
    builder.add(DisplayData.item("precision", precision).withLabel("Precision"));
  }

  // HLLCount supports, Long, Integers, String and Byte primitives.
  // We will return an appropriate builder
  protected static <T> HllCount.Init.Builder<T> builderForType(TypeDescriptor<T> input) {

    @SuppressWarnings("rawtypes")
    HllCount.Init.Builder builder = null;

    if (input.equals(TypeDescriptors.strings())) {
      builder = HllCount.Init.forStrings();
    }
    if (input.equals(TypeDescriptors.longs())) {
      builder = HllCount.Init.forLongs();
    }
    if (input.equals(TypeDescriptors.integers())) {
      builder = HllCount.Init.forIntegers();
    }
    if (input.equals(new TypeDescriptor<byte[]>() {})) {
      builder = HllCount.Init.forBytes();
    }

    if (builder == null) {
      throw new IllegalArgumentException(String.format("Type not supported %s", input));
    }

    // Safe to ignore warning, as we know the type based on the check we do above.
    @SuppressWarnings("unchecked")
    HllCount.Init.Builder<T> output = (HllCount.Init.Builder<T>) builder;

    return output;
  }
}
