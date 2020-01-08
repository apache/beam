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
package org.apache.beam.sdk.schemas.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaUserTypeCreator;
import org.apache.beam.sdk.schemas.utils.AvroUtils.AvroTypeConversionFactory;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.InjectPackageStrategy;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.TypeConversion;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.TypeConversionsFactory;
import org.apache.beam.sdk.schemas.utils.ReflectUtils.ClassWithSchema;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.ByteBuddy;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.dynamic.DynamicType;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.MethodCall;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.StackManipulation;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.assign.TypeCasting;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.collection.ArrayAccess;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.constant.IntegerConstant;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;

class AvroByteBuddyUtils {
  private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();

  // Cache the generated constructors.
  private static final Map<ClassWithSchema, SchemaUserTypeCreator> CACHED_CREATORS =
      Maps.newConcurrentMap();

  static <T extends SpecificRecord> SchemaUserTypeCreator getCreator(
      Class<T> clazz, Schema schema) {
    return CACHED_CREATORS.computeIfAbsent(
        ClassWithSchema.create(clazz, schema), c -> createCreator(clazz, schema));
  }

  private static <T> SchemaUserTypeCreator createCreator(Class<T> clazz, Schema schema) {
    Constructor baseConstructor = null;
    Constructor[] constructors = clazz.getDeclaredConstructors();
    for (Constructor constructor : constructors) {
      // TODO: This assumes that Avro only generates one constructor with this many fields.
      if (constructor.getParameterCount() == schema.getFieldCount()) {
        baseConstructor = constructor;
      }
    }
    if (baseConstructor == null) {
      throw new RuntimeException("No matching constructor found for class " + clazz);
    }

    // Generate a method call to create and invoke the SpecificRecord's constructor. .
    MethodCall construct = MethodCall.construct(baseConstructor);
    for (int i = 0; i < baseConstructor.getParameterTypes().length; ++i) {
      Class<?> baseType = baseConstructor.getParameterTypes()[i];
      construct = construct.with(readAndConvertParameter(baseType, i), baseType);
    }

    try {
      DynamicType.Builder<SchemaUserTypeCreator> builder =
          BYTE_BUDDY
              .with(new InjectPackageStrategy(clazz))
              .subclass(SchemaUserTypeCreator.class)
              .method(ElementMatchers.named("create"))
              .intercept(construct);

      return builder
          .make()
          .load(
              ReflectHelpers.findClassLoader(clazz.getClassLoader()),
              ClassLoadingStrategy.Default.INJECTION)
          .getLoaded()
          .getDeclaredConstructor()
          .newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new RuntimeException(
          "Unable to generate a getter for class " + clazz + " with schema " + schema);
    }
  }

  private static StackManipulation readAndConvertParameter(
      Class<?> constructorParameterType, int index) {
    TypeConversionsFactory typeConversionsFactory = new AvroTypeConversionFactory();

    // The types in the AVRO-generated constructor might be the types returned by Beam's Row class,
    // so we have to convert the types used by Beam's Row class.
    // We know that AVRO generates constructor parameters in the same order as fields
    // in the schema, so we can just add the parameters sequentially.
    TypeConversion<Type> convertType = typeConversionsFactory.createTypeConversion(true);

    // Map the AVRO-generated type to the one Beam will use.
    ForLoadedType convertedType =
        new ForLoadedType((Class) convertType.convert(TypeDescriptor.of(constructorParameterType)));

    // This will run inside the generated creator. Read the parameter and convert it to the
    // type required by the SpecificRecord constructor.
    StackManipulation readParameter =
        new StackManipulation.Compound(
            MethodVariableAccess.REFERENCE.loadFrom(1),
            IntegerConstant.forValue(index),
            ArrayAccess.REFERENCE.load(),
            TypeCasting.to(convertedType));

    // Convert to the parameter accepted by the SpecificRecord constructor.
    return typeConversionsFactory
        .createSetterConversions(readParameter)
        .convert(TypeDescriptor.of(constructorParameterType));
  }
}
