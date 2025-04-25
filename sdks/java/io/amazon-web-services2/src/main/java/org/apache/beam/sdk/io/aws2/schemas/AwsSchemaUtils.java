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
package org.apache.beam.sdk.io.aws2.schemas;

import static net.bytebuddy.matcher.ElementMatchers.isStatic;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.beam.sdk.util.ByteBuddyUtils.getClassLoadingStrategy;

import java.util.function.BiConsumer;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import net.bytebuddy.description.type.TypeDescription.Generic;
import net.bytebuddy.implementation.MethodCall;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueSetter;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.utils.builder.SdkBuilder;

class AwsSchemaUtils {
  private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();
  private static final TypeDescription FACTORY_TYPE = ForLoadedType.of(AwsBuilderFactory.class);

  private AwsSchemaUtils() {}

  /**
   * Generate an efficient implementation of {@link AwsBuilderFactory} for the given {@code clazz}
   * in byte code avoiding reflective access.
   */
  static <PojoT extends SdkPojo, BuilderT extends SdkBuilder<BuilderT, PojoT> & SdkPojo>
      AwsBuilderFactory<PojoT, BuilderT> builderFactory(Class<PojoT> clazz) {

    Generic pojoType = new ForLoadedType(clazz).asGenericType();
    MethodDescription builderMethod =
        pojoType.getDeclaredMethods().filter(named("builder").and(isStatic())).getOnly();
    Generic providerType =
        Generic.Builder.parameterizedType(FACTORY_TYPE, pojoType, builderMethod.getReturnType())
            .build();

    try {
      return (AwsBuilderFactory<PojoT, BuilderT>)
          BYTE_BUDDY
              .with(new ByteBuddyUtils.InjectPackageStrategy(clazz))
              .subclass(providerType)
              .method(named("get"))
              .intercept(MethodCall.invoke(builderMethod))
              .make()
              .load(ReflectHelpers.findClassLoader(), getClassLoadingStrategy(clazz))
              .getLoaded()
              .getDeclaredConstructor()
              .newInstance();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Unable to generate builder factory for " + clazz, e);
    }
  }

  static SdkBuilderSetter setter(String name, BiConsumer<SdkBuilder<?, ?>, Object> setter) {
    return new ValueSetter(name, setter);
  }

  static <ObjT extends @NonNull Object, ValT> FieldValueGetter<ObjT, ValT> getter(
      String name, SerializableFunction<ObjT, ValT> getter) {
    return new ValueGetter<>(name, getter);
  }

  interface SdkBuilderSetter extends FieldValueSetter<SdkBuilder<?, ?>, Object> {}

  private static class ValueSetter implements SdkBuilderSetter {
    private final BiConsumer<SdkBuilder<?, ?>, Object> setter;
    private final String name;

    ValueSetter(String name, BiConsumer<SdkBuilder<?, ?>, Object> setter) {
      this.name = name;
      this.setter = setter;
    }

    @Override
    public void set(SdkBuilder<?, ?> builder, @Nullable Object value) {
      if (value != null) {
        setter.accept(builder, value); // don't call setter if value is absent
      }
    }

    @Override
    public String name() {
      return name;
    }
  }

  private static class ValueGetter<ObjT extends @NonNull Object, ValT>
      implements FieldValueGetter<ObjT, ValT> {
    private final SerializableFunction<ObjT, ValT> getter;
    private final String name;

    ValueGetter(String name, SerializableFunction<ObjT, ValT> getter) {
      this.name = name;
      this.getter = getter;
    }

    @Override
    @Nullable
    public ValT get(ObjT object) {
      return getter.apply(object);
    }

    @Override
    public String name() {
      return name;
    }
  }
}
