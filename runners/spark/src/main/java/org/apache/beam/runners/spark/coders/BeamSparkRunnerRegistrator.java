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

package org.apache.beam.runners.spark.coders;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableListSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableSetSerializer;
import de.javakaffee.kryoserializers.guava.ReverseListSerializer;
import java.util.Arrays;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.Source;
import org.apache.spark.serializer.KryoRegistrator;
import org.reflections.Reflections;


/**
 * Custom {@link com.esotericsoftware.kryo.Serializer}s for Beam's Spark runner needs.
 */
public class BeamSparkRunnerRegistrator implements KryoRegistrator {

  private static final Class<?>[] CLASSES_FOR_JAVA_SERIALIZATION = new Class<?>[]{
      Coder.class, Source.class
  };

  private static volatile Iterable<Class<?>> subclassesForJavaSerialization;

  /**
   * Register coders and sources with {@link StatelessJavaSerializer} since sources and coders
   * aren't guaranteed to be Kryo-serializable.
   **/
  private static Iterable<Class<?>> getClassesForJavaSerialization() {
    if (subclassesForJavaSerialization != null) {
      return subclassesForJavaSerialization;
    }
    synchronized (CLASSES_FOR_JAVA_SERIALIZATION) {
      if (subclassesForJavaSerialization != null) {
        return subclassesForJavaSerialization;
      }
      final Reflections reflections = new Reflections();
      subclassesForJavaSerialization = Iterables.concat(Lists.transform(Arrays.asList
              (CLASSES_FOR_JAVA_SERIALIZATION),
          new Function<Class, Set<Class<?>>>() {
            @SuppressWarnings({"unchecked", "ConstantConditions"})
            @Nullable
            @Override
            public Set<Class<?>> apply(@Nullable Class clazz) {
              return reflections.getSubTypesOf(clazz);
            }
          }));
      return subclassesForJavaSerialization;
    }
  }

  @Override
  public void registerClasses(Kryo kryo) {
    for (Class<?> clazz : getClassesForJavaSerialization()) {
      kryo.register(clazz, new StatelessJavaSerializer());
    }
    UnmodifiableCollectionsSerializer.registerSerializers(kryo);
    // Guava
    ImmutableListSerializer.registerSerializers(kryo);
    ImmutableSetSerializer.registerSerializers(kryo);
    ImmutableMapSerializer.registerSerializers(kryo);
    ImmutableMultimapSerializer.registerSerializers(kryo);
    ReverseListSerializer.registerSerializers(kryo);
  }
}
