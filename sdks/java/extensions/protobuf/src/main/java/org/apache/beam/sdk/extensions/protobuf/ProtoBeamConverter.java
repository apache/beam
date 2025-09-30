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
package org.apache.beam.sdk.extensions.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.NanosDuration;
import org.apache.beam.sdk.schemas.logicaltypes.NanosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.initialization.qual.UnknownInitialization;
import org.checkerframework.checker.nullness.qual.EnsuresNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Provides converts between Protobuf Message and Beam Row.
 *
 * <p>Read <a href="https://s.apache.org/beam-protobuf">https://s.apache.org/beam-protobuf</a>
 */
public class ProtoBeamConverter {

  /** Returns a conversion method from Beam Row to Protobuf Message. */
  public static SerializableFunction<Row, Message> toProto(Descriptors.Descriptor descriptor) {
    return new ToProto(descriptor);
  }

  /** Returns a conversion method from Protobuf Message to Beam Row. */
  public static SerializableFunction<Message, Row> toRow(Schema schema) {
    return new FromProto(schema);
  }

  static ProtoToBeamConverter<Object, Object> createProtoToBeamConverter(
      Schema.FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case BOOLEAN:
        return createWrappableProtoToBeamConverter(ProtoToBeamConverter.identity());
      case BYTES:
        return createWrappableProtoToBeamConverter(ByteString::toByteArray);
      case ARRAY:
      case ITERABLE:
        ProtoToBeamConverter<Object, Object> elementConverter =
            createProtoToBeamConverter(
                Preconditions.checkNotNull(fieldType.getCollectionElementType()));
        return proto ->
            ((List<Object>) proto)
                .stream()
                    .map(element -> Preconditions.checkNotNull(elementConverter.convert(element)))
                    .collect(Collectors.toList());
      case MAP:
        ProtoToBeamConverter<Object, Object> keyConverter =
            createProtoToBeamConverter(Preconditions.checkNotNull(fieldType.getMapKeyType()));
        ProtoToBeamConverter<Object, Object> valueConverter =
            createProtoToBeamConverter(Preconditions.checkNotNull(fieldType.getMapValueType()));

        return proto -> {
          List<Message> list = (List<Message>) proto;
          if (list.isEmpty()) {
            return Collections.emptyMap();
          }
          Descriptors.Descriptor descriptor = list.get(0).getDescriptorForType();
          Descriptors.FieldDescriptor keyFieldDescriptor = descriptor.findFieldByNumber(1);
          Descriptors.FieldDescriptor valueFieldDescriptor = descriptor.findFieldByNumber(2);
          return list.stream()
              .collect(
                  Collectors.toMap(
                      protoElement ->
                          keyConverter.convert(protoElement.getField(keyFieldDescriptor)),
                      protoElement ->
                          valueConverter.convert(protoElement.getField(valueFieldDescriptor)),
                      (a, b) -> b));
        };
      case ROW:
        SerializableFunction<Message, Row> converter =
            toRow(Preconditions.checkNotNull(fieldType.getRowSchema()));
        return message -> converter.apply((Message) message);

      case LOGICAL_TYPE:
        switch (Preconditions.checkNotNull(fieldType.getLogicalType()).getIdentifier()) {
          case ProtoSchemaLogicalTypes.UInt32.IDENTIFIER:
          case ProtoSchemaLogicalTypes.SInt32.IDENTIFIER:
          case ProtoSchemaLogicalTypes.Fixed32.IDENTIFIER:
          case ProtoSchemaLogicalTypes.SFixed32.IDENTIFIER:
          case ProtoSchemaLogicalTypes.UInt64.IDENTIFIER:
          case ProtoSchemaLogicalTypes.SInt64.IDENTIFIER:
          case ProtoSchemaLogicalTypes.Fixed64.IDENTIFIER:
          case ProtoSchemaLogicalTypes.SFixed64.IDENTIFIER:
            return createWrappableProtoToBeamConverter(ProtoToBeamConverter.identity());
          case NanosDuration.IDENTIFIER:
            return proto -> {
              Message message = (Message) proto;
              Descriptors.Descriptor durationDescriptor = message.getDescriptorForType();
              Descriptors.FieldDescriptor secondsFieldDescriptor =
                  durationDescriptor.findFieldByNumber(1);
              Descriptors.FieldDescriptor nanosFieldDescriptor =
                  durationDescriptor.findFieldByNumber(2);
              long seconds = (long) message.getField(secondsFieldDescriptor);
              int nanos = (int) message.getField(nanosFieldDescriptor);
              return Duration.ofSeconds(seconds, nanos);
            };
          case NanosInstant.IDENTIFIER:
            return proto -> {
              Message message = (Message) proto;
              Descriptors.Descriptor timestampDescriptor = message.getDescriptorForType();
              Descriptors.FieldDescriptor secondsFieldDescriptor =
                  timestampDescriptor.findFieldByNumber(1);
              Descriptors.FieldDescriptor nanosFieldDescriptor =
                  timestampDescriptor.findFieldByNumber(2);
              long seconds = (long) message.getField(secondsFieldDescriptor);
              int nanos = (int) message.getField(nanosFieldDescriptor);
              return Instant.ofEpochSecond(seconds, nanos);
            };
          case EnumerationType.IDENTIFIER:
            EnumerationType enumerationType = fieldType.getLogicalType(EnumerationType.class);
            return enumValue ->
                enumerationType.toInputType(
                    ((Descriptors.EnumValueDescriptor) enumValue).getNumber());
          default:
            throw new UnsupportedOperationException();
        }
      default:
        throw new UnsupportedOperationException(
            "Unsupported field type: " + fieldType.getTypeName());
    }
  }

  static BeamToProtoConverter<Object, Object> createBeamToProtoConverter(
      Descriptors.FieldDescriptor fieldDescriptor) {
    if (fieldDescriptor.isRepeated()) {
      if (fieldDescriptor.isMapField()) {
        Descriptors.Descriptor mapDescriptor = fieldDescriptor.getMessageType();
        Descriptors.FieldDescriptor keyDescriptor = mapDescriptor.findFieldByNumber(1);
        Descriptors.FieldDescriptor valueDescriptor = mapDescriptor.findFieldByNumber(2);
        BeamToProtoConverter<Object, Object> keyToProto =
            createBeamToProtoSingularConverter(keyDescriptor);
        BeamToProtoConverter<Object, Object> valueToProto =
            createBeamToProtoSingularConverter(valueDescriptor);
        return map -> {
          ImmutableList.Builder<Message> protoList = ImmutableList.builder();
          ((Map<Object, Object>) map)
              .forEach(
                  (k, v) -> {
                    DynamicMessage.Builder message = DynamicMessage.newBuilder(mapDescriptor);
                    Object protoKey = Preconditions.checkNotNull(keyToProto.convert(k));
                    message.setField(keyDescriptor, protoKey);
                    Object protoValue = Preconditions.checkNotNull(valueToProto.convert(v));
                    message.setField(valueDescriptor, protoValue);
                    protoList.add(message.build());
                  });
          return protoList.build();
        };
      } else {
        BeamToProtoConverter<Object, Object> converter =
            createBeamToProtoSingularConverter(fieldDescriptor);
        return list ->
            ((List<Object>) list)
                .stream()
                    .map(beamElement -> converter.convert(beamElement))
                    .collect(Collectors.toList());
      }
    } else {
      return createBeamToProtoSingularConverter(fieldDescriptor);
    }
  }

  @SuppressWarnings({"JavaInstantGetSecondsGetNano", "JavaDurationGetSecondsGetNano"})
  static BeamToProtoConverter<Object, Object> createBeamToProtoSingularConverter(
      Descriptors.FieldDescriptor fieldDescriptor) {
    switch (fieldDescriptor.getJavaType()) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case STRING:
        return createWrappableBeamToProtoConverter(
            fieldDescriptor, BeamToProtoConverter.identity());
      case BYTE_STRING:
        return createWrappableBeamToProtoConverter(
            fieldDescriptor, bytes -> ByteString.copyFrom((byte[]) bytes));
      case ENUM:
        return value ->
            fieldDescriptor
                .getEnumType()
                .findValueByNumber(((EnumerationType.Value) value).getValue());
      case MESSAGE:
        String fullName = fieldDescriptor.getMessageType().getFullName();
        switch (fullName) {
          case "google.protobuf.Int32Value":
          case "google.protobuf.UInt32Value":
          case "google.protobuf.Int64Value":
          case "google.protobuf.UInt64Value":
          case "google.protobuf.FloatValue":
          case "google.protobuf.DoubleValue":
          case "google.protobuf.StringValue":
          case "google.protobuf.BoolValue":
            return createWrappableBeamToProtoConverter(
                fieldDescriptor, BeamToProtoConverter.identity());
          case "google.protobuf.BytesValue":
            return createWrappableBeamToProtoConverter(
                fieldDescriptor, bytes -> ByteString.copyFrom((byte[]) bytes));
          case "google.protobuf.Timestamp":
            return beam -> {
              Instant instant = (Instant) beam;
              return Timestamp.newBuilder()
                  .setSeconds(instant.getEpochSecond())
                  .setNanos(instant.getNano())
                  .build();
            };
          case "google.protobuf.Duration":
            return beam -> {
              Duration duration = (Duration) beam;
              return com.google.protobuf.Duration.newBuilder()
                  .setSeconds(duration.getSeconds())
                  .setNanos(duration.getNano())
                  .build();
            };
          case "google.protobuf.Any":
            throw new UnsupportedOperationException("google.protobuf.Any is not supported");
          default:
            SerializableFunction<Row, Message> converter =
                toProto(fieldDescriptor.getMessageType());
            return value -> converter.apply((Row) value);
        }
      default:
        throw new UnsupportedOperationException(
            "Unsupported proto type: " + fieldDescriptor.getJavaType());
    }
  }

  /** Gets a converter from non-null Proto value to non-null Beam. */
  static <ProtoUnwrappedT, BeamT>
      ProtoToBeamConverter<Object, BeamT> createWrappableProtoToBeamConverter(
          ProtoToBeamConverter<ProtoUnwrappedT, BeamT> converter) {
    return protoValue -> {
      @NonNull ProtoUnwrappedT unwrappedProtoValue;
      if (protoValue instanceof Message) {
        // A google protobuf wrapper
        Message protoWrapper = (Message) protoValue;
        Descriptors.FieldDescriptor wrapperValueFieldDescriptor =
            protoWrapper.getDescriptorForType().findFieldByNumber(1);
        unwrappedProtoValue =
            (@NonNull ProtoUnwrappedT)
                Preconditions.checkNotNull(protoWrapper.getField(wrapperValueFieldDescriptor));
      } else {
        unwrappedProtoValue = (@NonNull ProtoUnwrappedT) protoValue;
      }
      return converter.convert(unwrappedProtoValue);
    };
  }

  static <BeamT, ProtoUnwrappedT>
      BeamToProtoConverter<BeamT, Object> createWrappableBeamToProtoConverter(
          Descriptors.FieldDescriptor fieldDescriptor,
          BeamToProtoConverter<BeamT, ProtoUnwrappedT> converter) {
    return beamValue -> {
      ProtoUnwrappedT protoValue = converter.convert(beamValue);
      if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
        // A google.protobuf wrapper
        Descriptors.Descriptor wrapperDescriptor = fieldDescriptor.getMessageType();
        Descriptors.FieldDescriptor wrapperValueFieldDescriptor =
            wrapperDescriptor.findFieldByNumber(1);
        DynamicMessage.Builder wrapper = DynamicMessage.newBuilder(wrapperDescriptor);
        wrapper.setField(wrapperValueFieldDescriptor, protoValue);
        return wrapper.build();
      } else {
        return protoValue;
      }
    };
  }

  interface BeamToProtoConverter<BeamT, ProtoT> {
    BeamToProtoConverter<?, ?> IDENTITY = value -> value;

    static <T> BeamToProtoConverter<T, T> identity() {
      return (BeamToProtoConverter<T, T>) IDENTITY;
    }

    @NonNull
    ProtoT convert(@NonNull BeamT value);
  }

  interface FromProtoGetter<BeamT> {
    @Nullable
    BeamT getFromProto(Message message);
  }

  @FunctionalInterface
  interface ProtoToBeamConverter<ProtoT, BeamT> {
    ProtoToBeamConverter<?, ?> IDENTITY = protoValue -> protoValue;

    static <T> ProtoToBeamConverter<T, T> identity() {
      return (ProtoToBeamConverter<T, T>) IDENTITY;
    }

    @NonNull
    BeamT convert(@NonNull ProtoT protoValue);
  }

  interface ToProtoSetter<BeamT> {
    void setToProto(
        Message.Builder message, Schema.FieldType fieldType, @Nullable BeamT beamFieldValue);
  }

  static class FromProto implements SerializableFunction<Message, Row> {
    private transient Schema schema;
    private transient List<FromProtoGetter<?>> toBeams;

    public FromProto(Schema schema) {
      initialize(schema);
    }

    @Override
    public Row apply(Message message) {
      Row.Builder rowBuilder = Row.withSchema(schema);
      for (FromProtoGetter<?> toBeam : toBeams) {
        rowBuilder.addValue(toBeam.getFromProto(message));
      }
      return rowBuilder.build();
    }

    @EnsuresNonNull({"this.schema", "this.toBeams"})
    private void initialize(@UnknownInitialization FromProto this, Schema schema) {
      this.schema = schema;
      toBeams = new ArrayList<>();
      for (Schema.Field field : schema.getFields()) {
        Schema.FieldType fieldType = field.getType();
        if (fieldType.isLogicalType(OneOfType.IDENTIFIER)) {
          toBeams.add(new FromProtoOneOfGetter(field));
        } else {
          toBeams.add(new FromProtoFieldGetter<>(field));
        }
      }
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
      oos.writeObject(schema);
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
      initialize((Schema) ois.readObject());
    }
  }

  static class FromProtoFieldGetter<ProtoT, BeamT> implements FromProtoGetter<BeamT> {
    private final Schema.Field field;
    private final ProtoToBeamConverter<ProtoT, BeamT> converter;

    FromProtoFieldGetter(Schema.Field field) {
      this.field = field;
      converter = (ProtoToBeamConverter<ProtoT, BeamT>) createProtoToBeamConverter(field.getType());
    }

    @Override
    public @Nullable BeamT getFromProto(Message message) {
      try {
        Descriptors.Descriptor descriptor = message.getDescriptorForType();
        Descriptors.FieldDescriptor fieldDescriptor =
            Preconditions.checkNotNull(descriptor.findFieldByName(field.getName()));

        @Nullable Object protoValue;
        if (field.getType().getNullable()
            && ProtoSchemaTranslator.isNullable(fieldDescriptor)
            && !message.hasField(fieldDescriptor)) {
          // Set null field value only if the Beam field type is nullable and the proto value is
          // null,
          protoValue = null;
        } else {
          // can be a default value. e.g., an optional field.
          protoValue = message.getField(fieldDescriptor);
        }

        return protoValue != null ? converter.convert((@NonNull ProtoT) protoValue) : null;
      } catch (RuntimeException e) {
        throw new RuntimeException(
            String.format("Failed to get field from proto. field: %s", field.getName()), e);
      }
    }
  }

  static class FromProtoOneOfGetter implements FromProtoGetter<OneOfType.@Nullable Value> {
    private final Schema.Field field;
    private final OneOfType oneOfType;
    private final Map<String, ProtoToBeamConverter<Object, Object>> converter;

    FromProtoOneOfGetter(Schema.Field field) {
      this.field = field;
      this.oneOfType = Preconditions.checkNotNull(field.getType().getLogicalType(OneOfType.class));
      this.converter = createConverters(oneOfType.getOneOfSchema());
    }

    private static Map<String, ProtoToBeamConverter<Object, Object>> createConverters(
        Schema schema) {
      Map<String, ProtoToBeamConverter<Object, Object>> converters = new HashMap<>();
      for (Schema.Field field : schema.getFields()) {
        converters.put(field.getName(), createProtoToBeamConverter(field.getType()));
      }
      return converters;
    }

    @Override
    public OneOfType.@Nullable Value getFromProto(Message message) {
      Descriptors.Descriptor descriptor = message.getDescriptorForType();
      for (Map.Entry<String, ProtoToBeamConverter<Object, Object>> entry : converter.entrySet()) {
        String subFieldName = entry.getKey();
        try {
          ProtoToBeamConverter<Object, Object> value = entry.getValue();
          Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(subFieldName);
          if (message.hasField(fieldDescriptor)) {
            Object protoValue = message.getField(fieldDescriptor);
            return oneOfType.createValue(subFieldName, value.convert(protoValue));
          }
        } catch (RuntimeException e) {
          throw new RuntimeException(
              String.format(
                  "Failed to get oneof from proto. oneof: %s, subfield: %s",
                  field.getName(), subFieldName),
              e);
        }
      }
      return null;
    }
  }

  static class ToProto implements SerializableFunction<Row, Message> {
    private transient Descriptors.Descriptor descriptor;
    private transient Map<String, ToProtoSetter<Object>> toProtos;

    public ToProto(Descriptors.Descriptor descriptor) {
      initialize(descriptor);
    }

    @EnsuresNonNull({"this.descriptor", "this.toProtos"})
    private void initialize(
        @UnknownInitialization ToProto this, Descriptors.Descriptor descriptor) {
      this.descriptor = descriptor;
      toProtos = new LinkedHashMap<>();
      for (Descriptors.FieldDescriptor fieldDescriptor : descriptor.getFields()) {
        if (fieldDescriptor.getRealContainingOneof() != null) {
          Descriptors.OneofDescriptor realContainingOneof =
              fieldDescriptor.getRealContainingOneof();
          if (realContainingOneof.getField(0) == fieldDescriptor) {
            ToProtoSetter<?> setter = new ToProtoOneOfSetter(realContainingOneof);
            toProtos.put(realContainingOneof.getName(), (ToProtoSetter<Object>) setter);
          }
          // continue
        } else {
          toProtos.put(fieldDescriptor.getName(), new ToProtoFieldSetter<>(fieldDescriptor));
        }
      }
    }

    @Override
    public Message apply(Row row) {
      Schema schema = row.getSchema();
      DynamicMessage.Builder message = DynamicMessage.newBuilder(descriptor);
      for (Map.Entry<String, ToProtoSetter<Object>> entry : toProtos.entrySet()) {
        String fieldName = entry.getKey();
        ToProtoSetter<Object> converter = entry.getValue();
        converter.setToProto(
            message, schema.getField(fieldName).getType(), row.getValue(fieldName));
      }
      return message.build();
    }

    // writeObject() needs to be implemented because Descriptor is not serializable.
    private void writeObject(ObjectOutputStream oos) throws IOException {
      ProtobufUtil.serializeDescriptor(oos, descriptor);
    }

    // readObject() needs to be implemented because Descriptor is not serializable.
    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
      initialize(ProtobufUtil.deserializeDescriptor(ois));
    }
  }

  static class ToProtoFieldSetter<BeamT, ProtoT> implements ToProtoSetter<BeamT> {
    private final Descriptors.FieldDescriptor fieldDescriptor;
    private final BeamToProtoConverter<BeamT, ProtoT> converter;

    ToProtoFieldSetter(Descriptors.FieldDescriptor fieldDescriptor) {
      this.fieldDescriptor = fieldDescriptor;
      this.converter =
          (BeamToProtoConverter<BeamT, ProtoT>) createBeamToProtoConverter(fieldDescriptor);
    }

    @Override
    public void setToProto(
        Message.Builder message, Schema.FieldType fieldType, @Nullable BeamT beamFieldValue) {
      try {
        if (beamFieldValue != null) {
          ProtoT protoValue = converter.convert(beamFieldValue);
          message.setField(fieldDescriptor, protoValue);
        }
      } catch (RuntimeException e) {
        throw new RuntimeException(
            String.format("Failed to set field to proto. field:%s", fieldDescriptor.getName()), e);
      }
    }
  }

  static class ToProtoOneOfSetter implements ToProtoSetter<OneOfType.@Nullable Value> {
    private final Descriptors.OneofDescriptor oneofDescriptor;
    private final Map<String, ToProtoFieldSetter<Object, Object>> protoSetters;

    ToProtoOneOfSetter(Descriptors.OneofDescriptor oneofDescriptor) {
      this.oneofDescriptor = oneofDescriptor;
      this.protoSetters = createConverters(oneofDescriptor.getFields());
    }

    private static Map<String, ToProtoFieldSetter<Object, Object>> createConverters(
        List<Descriptors.FieldDescriptor> fieldDescriptors) {
      Map<String, ToProtoFieldSetter<Object, Object>> converters = new LinkedHashMap<>();
      for (Descriptors.FieldDescriptor fieldDescriptor : fieldDescriptors) {
        Preconditions.checkState(!fieldDescriptor.isRepeated());
        converters.put(fieldDescriptor.getName(), new ToProtoFieldSetter<>(fieldDescriptor));
      }
      return converters;
    }

    @Override
    public void setToProto(
        Message.Builder message, Schema.FieldType fieldType, OneOfType.@Nullable Value oneOfValue) {
      if (oneOfValue != null) {
        OneOfType oneOfType = fieldType.getLogicalType(OneOfType.class);
        int number = oneOfValue.getCaseType().getValue();
        try {
          String subFieldName =
              Preconditions.checkNotNull(oneOfType.getCaseEnumType().getEnumName(number));

          ToProtoFieldSetter<Object, Object> protoSetter =
              Preconditions.checkNotNull(
                  protoSetters.get(subFieldName), "No setter for field '%s'", subFieldName);
          protoSetter.setToProto(
              message,
              oneOfType.getOneOfSchema().getField(subFieldName).getType(),
              oneOfValue.getValue());
        } catch (RuntimeException e) {
          throw new RuntimeException(
              String.format(
                  "Failed to set oneof to proto. oneof: %s, number: %d",
                  oneofDescriptor.getName(), number),
              e);
        }
      }
    }
  }
}
