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

import static java.util.Collections.singleton;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static software.amazon.awssdk.core.protocol.MarshallingType.INSTANT;
import static software.amazon.awssdk.core.protocol.MarshallingType.LIST;
import static software.amazon.awssdk.core.protocol.MarshallingType.MAP;
import static software.amazon.awssdk.core.protocol.MarshallingType.SDK_BYTES;
import static software.amazon.awssdk.core.protocol.MarshallingType.SDK_POJO;

import java.io.Serializable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import org.apache.beam.sdk.schemas.Factory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Ascii;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.joda.time.Instant;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.core.protocol.MarshallingType;
import software.amazon.awssdk.core.traits.ListTrait;
import software.amazon.awssdk.core.traits.MapTrait;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructMap;
import software.amazon.awssdk.utils.ImmutableMap;

public class AwsTypes {
  // Mapping of simple AWS types to schema field types
  private static final Map<MarshallingType<?>, FieldType> typeMapping =
      ImmutableMap.<MarshallingType<?>, FieldType>builder()
          .put(MarshallingType.STRING, FieldType.STRING)
          .put(MarshallingType.SHORT, FieldType.INT16)
          .put(MarshallingType.INTEGER, FieldType.INT32)
          .put(MarshallingType.LONG, FieldType.INT64)
          .put(MarshallingType.FLOAT, FieldType.FLOAT)
          .put(MarshallingType.DOUBLE, FieldType.DOUBLE)
          .put(MarshallingType.BIG_DECIMAL, FieldType.DECIMAL)
          .put(MarshallingType.BOOLEAN, FieldType.BOOLEAN)
          .put(INSTANT, FieldType.DATETIME)
          .put(SDK_BYTES, FieldType.BYTES)
          .build();

  private static FieldType fieldType(SdkField<?> field, Set<Class<?>> seen) {
    MarshallingType<?> type = field.marshallingType();
    if (type == LIST) {
      return FieldType.array(fieldType(elementField(field), seen));
    } else if (type == MAP) {
      return FieldType.map(FieldType.STRING, fieldType(valueField(field), seen));
    } else if (type == SDK_POJO) {
      SdkPojo builder = field.constructor().get();
      Class<?> clazz = targetClassOf(builder);
      checkState(!seen.contains(clazz), "Self-recursive types are not supported: %s", clazz);
      return FieldType.row(schemaFor(builder.sdkFields(), Sets.union(seen, singleton(clazz))));
    }
    FieldType fieldType = typeMapping.get(type);
    if (fieldType != null) {
      return fieldType;
    }
    throw new RuntimeException(
        String.format("Type %s of field %s is unknown.", type, normalizedNameOf(field)));
  }

  private static Schema schemaFor(List<SdkField<?>> fields, Set<Class<?>> seen) {
    Schema.Builder builder = Schema.builder();
    for (SdkField<?> sdkField : fields) {
      // AWS SDK fields are all optional and marked as nullable
      builder.addField(Field.nullable(normalizedNameOf(sdkField), fieldType(sdkField, seen)));
    }
    return builder.build();
  }

  static Schema schemaFor(List<SdkField<?>> fields) {
    return schemaFor(fields, ImmutableSet.of());
  }

  /**
   * Converter factory to handle specific AWS types.
   *
   * <p>Any occurrences of {@link java.time.Instant} or {@link SdkBytes} are converted to & from the
   * corresponding Beam types. When used with {@link org.apache.beam.sdk.schemas.FieldValueSetter},
   * any {@link Row} has to be converted back to the respective {@link SdkPojo}.
   */
  @SuppressWarnings("rawtypes")
  abstract static class ConverterFactory implements Serializable {
    @SuppressWarnings("nullness")
    private static final SerializableFunction IDENTITY = x -> x;

    private final SerializableFunction instantConverter;
    private final SerializableFunction bytesConverter;
    private final boolean convertPojoType;

    private ConverterFactory(
        SerializableFunction instantConverter,
        SerializableFunction bytesConverter,
        boolean convertPojoType) {
      this.instantConverter = instantConverter;
      this.bytesConverter = bytesConverter;
      this.convertPojoType = convertPojoType;
    }

    static ConverterFactory toAws(Factory<SerializableFunction<Row, ?>> fromRowFactory) {
      return new ToAws(fromRowFactory);
    }

    static ConverterFactory fromAws() {
      return FromAws.INSTANCE;
    }

    static <T, X1, X2> BiConsumer<T, X1> createSetter(
        BiConsumer<T, X2> set, SerializableFunction fn) {
      return (obj, value) -> set.accept(obj, ((SerializableFunction<X1, X2>) fn).apply(value));
    }

    SerializableFunction pojoTypeConverter(SdkField<?> field) {
      throw new UnsupportedOperationException();
    }

    SerializableFunction create(SdkField<?> field) {
      return create(IDENTITY, field);
    }

    SerializableFunction create(SerializableFunction fn, SdkField<?> field) {
      MarshallingType<?> awsType = field.marshallingType();
      SerializableFunction converter;
      if (awsType == SDK_POJO) {
        converter = pojoTypeConverter(field);
      } else if (awsType == INSTANT) {
        converter = instantConverter;
      } else if (awsType == SDK_BYTES) {
        converter = bytesConverter;
      } else if (awsType == LIST) {
        converter = transformList(create(elementField(field)));
      } else if (awsType == MAP) {
        converter = transformMap(create(valueField(field)));
      } else {
        throw new IllegalStateException("Unexpected marshalling type " + awsType);
      }
      return fn != IDENTITY ? andThen(fn, nullSafe(converter)) : nullSafe(converter);
    }

    boolean needsConversion(SdkField<?> field) {
      MarshallingType<?> type = field.marshallingType();
      return (convertPojoType && type.equals(MarshallingType.SDK_POJO))
          || type.equals(INSTANT)
          || type.equals(SDK_BYTES)
          || (type.equals(MAP) && needsConversion(valueField(field)))
          || (type.equals(LIST) && needsConversion(elementField(field)));
    }

    @SuppressWarnings("nullness")
    private static SerializableFunction andThen(
        SerializableFunction fn1, SerializableFunction fn2) {
      return v -> fn2.apply(fn1.apply(v));
    }

    @SuppressWarnings("nullness")
    private static SerializableFunction nullSafe(SerializableFunction fn) {
      return v -> v == null ? null : fn.apply(v);
    }

    @SuppressWarnings("nullness")
    private static SerializableFunction transformList(SerializableFunction fn) {
      return list -> Lists.transform((List) list, fn::apply);
    }

    @SuppressWarnings("nullness")
    private static SerializableFunction transformMap(SerializableFunction fn) {
      return map -> Maps.transformValues((Map) map, fn::apply);
    }

    /** Converter factory from Beam row value types to AWS types. This is applicable for setters. */
    private static class ToAws extends ConverterFactory {
      private final Factory<SerializableFunction<Row, ?>> fromRowFactory;

      ToAws(Factory<SerializableFunction<Row, ?>> fromRowFactory) {
        super(AwsTypes::toJavaInstant, AwsTypes::toSdkBytes, true);
        this.fromRowFactory = fromRowFactory;
      }

      @Override
      @SuppressWarnings("nullness") // schema nullable for this factory
      protected SerializableFunction pojoTypeConverter(SdkField<?> field) {
        return fromRowFactory.create(
            TypeDescriptor.of(targetClassOf(field.constructor().get())), null);
      }
    }

    /**
     * Converter factory from AWS types to Beam raw unmodified row types. This is applicable for
     * getters and also removes default values for lists & maps to avoid serializing those.
     */
    private static class FromAws extends ConverterFactory {
      private static final ConverterFactory INSTANCE = new FromAws();

      FromAws() {
        super(AwsTypes::toJodaInstant, AwsTypes::toBytes, false);
      }

      @Override
      SerializableFunction create(SerializableFunction fn, SdkField<?> field) {
        MarshallingType<?> type = field.marshallingType();
        if (type.equals(MAP)) {
          fn = skipDefaultMap(fn);
        } else if (type.equals(LIST)) {
          fn = skipDefaultList(fn);
        }
        return needsConversion(field) ? super.create(fn, field) : fn;
      }

      @SuppressWarnings("nullness")
      private static SerializableFunction skipDefaultList(SerializableFunction fn) {
        return in -> {
          Object list = fn.apply(in);
          return list != DefaultSdkAutoConstructList.getInstance() ? list : null;
        };
      }

      @SuppressWarnings("nullness")
      private static SerializableFunction skipDefaultMap(SerializableFunction fn) {
        return in -> {
          Object map = fn.apply(in);
          return map != DefaultSdkAutoConstructMap.getInstance() ? map : null;
        };
      }
    }
  }

  // Convert upper camel SDK field names to lower camel
  static String normalizedNameOf(SdkField<?> field) {
    String name = field.memberName();
    return name.length() > 1 && Ascii.isLowerCase(name.charAt(1))
        ? Ascii.toLowerCase(name.charAt(0)) + name.substring(1)
        : name.toLowerCase(Locale.ROOT);
  }

  static java.time.Instant toJavaInstant(Object instant) {
    return java.time.Instant.ofEpochMilli(((Instant) instant).getMillis());
  }

  private static Instant toJodaInstant(Object instant) {
    return Instant.ofEpochMilli(((java.time.Instant) instant).toEpochMilli());
  }

  private static SdkBytes toSdkBytes(Object sdkBytes) {
    // Unsafe operation, wrapping bytes directly as done by core for byte arrays /  buffers
    return SdkBytes.fromByteArrayUnsafe((byte[]) sdkBytes);
  }

  private static byte[] toBytes(Object sdkBytes) {
    // Unsafe operation, exposing bytes directly as done by core for byte arrays /  buffers
    return ((SdkBytes) sdkBytes).asByteArrayUnsafe();
  }

  private static SdkField<?> elementField(SdkField<?> field) {
    return field.getTrait(ListTrait.class).memberFieldInfo();
  }

  private static SdkField<?> valueField(SdkField<?> field) {
    return field.getTrait(MapTrait.class).valueFieldInfo();
  }

  private static Class<?> targetClassOf(SdkPojo builder) {
    // the declaring class is the class this builder produces
    return checkArgumentNotNull(
        builder.getClass().getDeclaringClass(),
        "Expected nested builder class, but got %s",
        builder.getClass());
  }
}
