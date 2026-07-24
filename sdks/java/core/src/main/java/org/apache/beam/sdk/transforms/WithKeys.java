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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.CheckForNull;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@code WithKeys<K, V>} takes a {@code PCollection<V>}, and either a constant key of type {@code
 * K} or a function from {@code V} to {@code K}, and returns a {@code PCollection<KV<K, V>>}, where
 * each of the values in the input {@code PCollection} has been paired with either the constant key
 * or a key computed from the value.
 *
 * <p>Example of use:
 *
 * <pre>{@code
 * PCollection<String> words = ...;
 * PCollection<KV<Integer, String>> lengthsToWords =
 *     words.apply(WithKeys.of(new SerializableFunction<String, Integer>() {
 *         public Integer apply(String s) { return s.length(); } }));
 * }</pre>
 *
 * <p>Each output element has the same timestamp and is in the same windows as its corresponding
 * input element, and the output {@code PCollection} has the same {@link
 * org.apache.beam.sdk.transforms.windowing.WindowFn} associated with it as the input.
 *
 * @param <K> the type of the keys in the output {@code PCollection}
 * @param <V> the type of the elements in the input {@code PCollection} and the values in the output
 *     {@code PCollection}
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class WithKeys<K, V> extends PTransform<PCollection<V>, PCollection<KV<K, V>>> {
  /**
   * Returns a {@code PTransform} that takes a {@code PCollection<V>} and returns a {@code
   * PCollection<KV<K, V>>}, where each of the values in the input {@code PCollection} has been
   * paired with a key computed from the value by invoking the given {@code SerializableFunction}.
   *
   * <p>If using a lambda in Java 8, {@link #withKeyType(TypeDescriptor)} must be called on the
   * result {@link PTransform}.
   */
  public static <K, V> WithKeys<K, V> of(SerializableFunction<V, K> fn) {
    checkNotNull(
        fn, "WithKeys constructed with null function. Did you mean WithKeys.of((Void) null)?");
    return new WithKeys<>(fn, null);
  }

  /**
   * Returns a {@code PTransform} that takes a {@code PCollection<V>} and returns a {@code
   * PCollection<KV<K, V>>}, where each of the values in the input {@code PCollection} has been
   * paired with the given key.
   */
  @SuppressWarnings("unchecked")
  public static <K, V> WithKeys<K, V> of(final @Nullable K key) {
    return new WithKeys<>(
        value -> key,
        (TypeDescriptor<K>)
            (key == null ? TypeDescriptors.voids() : TypeDescriptor.of(key.getClass())));
  }

  /////////////////////////////////////////////////////////////////////////////

  private SerializableFunction<V, K> fn;
  @CheckForNull private transient TypeDescriptor<K> keyType;

  private WithKeys(SerializableFunction<V, K> fn, TypeDescriptor<K> keyType) {
    this.fn = fn;
    this.keyType = keyType;
  }

  /**
   * Return a {@link WithKeys} that is like this one with the specified key type descriptor.
   *
   * <p>For use with lambdas in Java 8, either this method must be called with an appropriate type
   * descriptor or {@link PCollection#setCoder(Coder)} must be called on the output {@link
   * PCollection}.
   */
  public WithKeys<K, V> withKeyType(TypeDescriptor<K> keyType) {
    return new WithKeys<>(fn, keyType);
  }

  @Override
  public PCollection<KV<K, V>> expand(PCollection<V> in) {
    PCollection<KV<K, V>> result =
        in.apply(
            "AddKeys",
            MapElements.via(
                new SimpleFunction<V, KV<K, V>>() {
                  @Override
                  public KV<K, V> apply(V element) {
                    return KV.of(fn.apply(element), element);
                  }
                }));

    try {
      Coder<K> keyCoder;
      CoderRegistry coderRegistry = in.getPipeline().getCoderRegistry();
      if (keyType == null) {
        keyCoder = coderRegistry.getOutputCoder(fn, in.getCoder());
      } else {
        keyCoder = coderRegistry.getCoder(keyType);
      }
      // TODO: Remove when we can set the coder inference context.
      result.setCoder(KvCoder.of(keyCoder, in.getCoder()));
    } catch (CannotProvideCoderException exc) {
      if (keyType != null) {
        try {
          SchemaRegistry schemaRegistry = SchemaRegistry.createDefault();
          SchemaCoder<K> schemaCoder =
              SchemaCoder.of(
                  schemaRegistry.getSchema(keyType),
                  keyType,
                  schemaRegistry.getToRowFunction(keyType),
                  schemaRegistry.getFromRowFunction(keyType));
          result.setCoder(KvCoder.of(schemaCoder, in.getCoder()));
        } catch (NoSuchSchemaException exception) {
          // No Schema.
        }
      }
      // let lazy coder inference have a try
    }

    return result;
  }
}
