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
  public static SerializableFunction<@NonNull Row, @NonNull Message> toProto(
      Descriptors.Descriptor descriptor) {
    return new ToProto(descriptor);
  }

  /** Returns a conversion method from Protobuf Message to Beam Row. */
  public static SerializableFunction<@NonNull Message, @NonNull Row> toRow(Schema schema) {
    return new FromProto(schema);
  }

  static BeamConverter<?, ?> createBeamConverter(Schema.FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case BYTE:
        throw new UnsupportedOperationException();
      case INT16:
        throw new UnsupportedOperationException();
      case INT32:
        return new BeamPassThroughConverter<>();
      case INT64:
        return new BeamPassThroughConverter<>();
      case DECIMAL:
        throw new UnsupportedOperationException();
      case FLOAT:
        return new BeamPassThroughConverter<>();
      case DOUBLE:
        return new BeamPassThroughConverter<>();
      case STRING:
        return new BeamPassThroughConverter<>();
      case DATETIME:
        throw new UnsupportedOperationException();
      case BOOLEAN:
        return new BeamPassThroughConverter<>();
      case BYTES:
        return new BeamBytesConverter();
      case ARRAY:
      case ITERABLE:
        return new BeamListConverter<>(fieldType);
      case MAP:
        return new BeamMapConverter<>(fieldType);
      case ROW:
        return new BeamRowConverter(fieldType);
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
            return new BeamPassThroughConverter<>();
          case NanosDuration.IDENTIFIER:
            return new BeamNanosDurationConverter();
          case NanosInstant.IDENTIFIER:
            return new BeamNanosInstantConverter();
          case EnumerationType.IDENTIFIER:
            return new BeamEnumerationConverter(fieldType);
          default:
            throw new UnsupportedOperationException();
        }
      default:
        throw new UnsupportedOperationException();
    }
  }

  static @NonNull ProtoConverter<?, ?> createToProtoConverter(
      Descriptors.FieldDescriptor fieldDescriptor) {
    if (fieldDescriptor.isRepeated()) {
      if (fieldDescriptor.isMapField()) {
        return new ProtoMapConverter<>(fieldDescriptor);
      } else {
        return new ProtoListConverter<>(fieldDescriptor);
      }
    } else {
      return createToProtoSingularConverter(fieldDescriptor);
    }
  }

  static @NonNull ProtoConverter<?, ?> createToProtoSingularConverter(
      Descriptors.FieldDescriptor fieldDescriptor) {
    switch (fieldDescriptor.getJavaType()) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case STRING:
        return new ProtoPassThroughConverter<>(fieldDescriptor);
      case BYTE_STRING:
        return new ProtoByteStringConverter(fieldDescriptor);
      case ENUM:
        return new ProtoEnumConverter(fieldDescriptor);
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
            return new ProtoPassThroughConverter<>(fieldDescriptor);
          case "google.protobuf.BytesValue":
            return new ProtoByteStringConverter(fieldDescriptor);
          case "google.protobuf.Timestamp":
            return new ProtoTimestampConverter(fieldDescriptor);
          case "google.protobuf.Duration":
            return new ProtoDurationConverter(fieldDescriptor);
          case "google.protobuf.Any":
            throw new UnsupportedOperationException("google.protobuf.Any is not supported");
          default:
            return new ProtoMessageConverter(fieldDescriptor);
        }
      default:
        throw new UnsupportedOperationException(
            "Unsupported proto type: " + fieldDescriptor.getJavaType());
    }
  }

  interface FromProtoGetter<BeamT> {
    @Nullable
    BeamT getFromProto(Message message);
  }

  interface ToProtoSetter<BeamT> {
    void setToProto(Message.Builder message, Schema.FieldType fieldType, BeamT beamFieldValue);
  }

  abstract static class BeamConverter<ProtoT, BeamT> {
    abstract @Nullable BeamT convert(@Nullable ProtoT protoValue);
  }

  abstract static class BeamNoWrapConverter<ProtoT, BeamT> extends BeamConverter<ProtoT, BeamT> {
    @Override
    @Nullable
    BeamT convert(@Nullable ProtoT protoValue) {
      if (protoValue == null) {
        return null;
      }

      return convertNonNull(protoValue);
    }

    abstract @NonNull BeamT convertNonNull(@NonNull ProtoT protoValue);
  }

  abstract static class BeamWrapConverter<ProtoT, ProtoUnwrappedT, BeamT>
      extends BeamConverter<ProtoT, BeamT> {

    @SuppressWarnings("unchecked")
    @Override
    public @Nullable BeamT convert(@Nullable ProtoT protoValue) {
      if (protoValue == null) {
        return null;
      }

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
      return convertNonNullUnwrapped(unwrappedProtoValue);
    }

    abstract @NonNull BeamT convertNonNullUnwrapped(@NonNull ProtoUnwrappedT protoValue);
  }

  abstract static class ProtoConverter<BeamT, ProtoT> {
    private transient Descriptors.FieldDescriptor fieldDescriptor;

    ProtoConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      this.fieldDescriptor = fieldDescriptor;
    }

    public abstract @Nullable ProtoT convert(@Nullable BeamT beamValue);

    public Descriptors.FieldDescriptor getFieldDescriptor() {
      return fieldDescriptor;
    }
  }

  abstract static class ProtoNoWrapConverter<BeamT, ProtoT> extends ProtoConverter<BeamT, ProtoT> {
    ProtoNoWrapConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    public @Nullable ProtoT convert(@Nullable BeamT beamValue) {
      if (beamValue != null) {
        return convertNonNull(beamValue);
      } else {
        return null;
      }
    }

    protected abstract @NonNull ProtoT convertNonNull(@NonNull BeamT beamValue);
  }

  abstract static class ProtoWrapConverter<BeamT, ProtoUnwrappedT>
      extends ProtoConverter<BeamT, Object> {
    ProtoWrapConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    public @Nullable Object convert(@Nullable BeamT beamValue) {
      Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor();
      if (beamValue == null) {
        return null;
      }

      ProtoUnwrappedT protoValue = convertNonNullUnwrapped(beamValue);
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
    }

    protected abstract @NonNull ProtoUnwrappedT convertNonNullUnwrapped(@NonNull BeamT beamValue);
  }

  static class BeamBytesConverter extends BeamWrapConverter<Object, ByteString, byte[]> {

    @Override
    protected byte @NonNull [] convertNonNullUnwrapped(@NonNull ByteString protoValue) {
      return protoValue.toByteArray();
    }
  }

  static class BeamEnumerationConverter
      extends BeamNoWrapConverter<Descriptors.EnumValueDescriptor, EnumerationType.Value> {
    private final EnumerationType enumerationType;

    BeamEnumerationConverter(Schema.FieldType fieldType) {
      enumerationType = fieldType.getLogicalType(EnumerationType.class);
    }

    @Override
    protected EnumerationType.@NonNull Value convertNonNull(
        Descriptors.@NonNull EnumValueDescriptor protoValue) {
      int number = protoValue.getNumber();
      return enumerationType.toInputType(number);
    }
  }

  static class BeamListConverter<ProtoT, BeamT>
      extends BeamNoWrapConverter<List<@NonNull ProtoT>, List<@NonNull BeamT>> {
    private final BeamConverter<ProtoT, BeamT> elementConverter;

    @SuppressWarnings("unchecked")
    public BeamListConverter(Schema.FieldType fieldType) {
      elementConverter =
          (BeamConverter<ProtoT, BeamT>)
              createBeamConverter(Preconditions.checkNotNull(fieldType.getCollectionElementType()));
    }

    @Override
    protected @NonNull List<@NonNull BeamT> convertNonNull(
        @NonNull List<@NonNull ProtoT> protoList) {
      List<@NonNull BeamT> beamList = new ArrayList<>();
      for (@NonNull ProtoT element : protoList) {
        beamList.add(Preconditions.checkNotNull(elementConverter.convert(element)));
      }
      return beamList;
    }
  }

  static class BeamMapConverter<ProtoKeyT, ProtoValueT, BeamKeyT, BeamValueT>
      extends BeamNoWrapConverter<
          List<@NonNull Message>, Map<@NonNull BeamKeyT, @NonNull BeamValueT>> {
    private final BeamConverter<ProtoKeyT, BeamKeyT> keyConverter;
    private final BeamConverter<ProtoValueT, BeamValueT> valueConverter;

    @SuppressWarnings("unchecked")
    public BeamMapConverter(Schema.FieldType fieldType) {
      keyConverter =
          (BeamConverter<ProtoKeyT, BeamKeyT>)
              createBeamConverter(Preconditions.checkNotNull(fieldType.getMapKeyType()));
      valueConverter =
          (BeamConverter<ProtoValueT, BeamValueT>)
              createBeamConverter(Preconditions.checkNotNull(fieldType.getMapValueType()));
    }

    @SuppressWarnings("unchecked")
    @Override
    protected @NonNull Map<@NonNull BeamKeyT, @NonNull BeamValueT> convertNonNull(
        @NonNull List<@NonNull Message> protoList) {
      if (protoList.isEmpty()) {
        return Collections.emptyMap();
      }
      Descriptors.Descriptor descriptor = protoList.get(0).getDescriptorForType();
      Descriptors.FieldDescriptor keyFieldDescriptor = descriptor.findFieldByNumber(1);
      Descriptors.FieldDescriptor valueFieldDescriptor = descriptor.findFieldByNumber(2);
      Map<@NonNull BeamKeyT, @NonNull BeamValueT> beamMap = new HashMap<>();
      protoList.forEach(
          protoElement -> {
            ProtoKeyT protoKey =
                Preconditions.checkNotNull((ProtoKeyT) protoElement.getField(keyFieldDescriptor));
            ProtoValueT protoValue =
                Preconditions.checkNotNull(
                    (ProtoValueT) protoElement.getField(valueFieldDescriptor));
            BeamKeyT beamKey = Preconditions.checkNotNull(keyConverter.convert(protoKey));
            BeamValueT beamValue = Preconditions.checkNotNull(valueConverter.convert(protoValue));
            beamMap.put(beamKey, beamValue);
          });
      return beamMap;
    }
  }

  static class BeamNanosDurationConverter extends BeamNoWrapConverter<Message, Duration> {

    @Override
    protected @NonNull Duration convertNonNull(@NonNull Message protoValue) {
      Descriptors.Descriptor durationDescriptor = protoValue.getDescriptorForType();
      Descriptors.FieldDescriptor secondsFieldDescriptor = durationDescriptor.findFieldByNumber(1);
      Descriptors.FieldDescriptor nanosFieldDescriptor = durationDescriptor.findFieldByNumber(2);
      long seconds = (long) protoValue.getField(secondsFieldDescriptor);
      int nanos = (int) protoValue.getField(nanosFieldDescriptor);
      return Duration.ofSeconds(seconds, nanos);
    }
  }

  static class BeamNanosInstantConverter extends BeamNoWrapConverter<Message, Instant> {

    @Override
    protected @NonNull Instant convertNonNull(@NonNull Message protoValue) {
      Descriptors.Descriptor timestampDescriptor = protoValue.getDescriptorForType();
      Descriptors.FieldDescriptor secondsFieldDescriptor = timestampDescriptor.findFieldByNumber(1);
      Descriptors.FieldDescriptor nanosFieldDescriptor = timestampDescriptor.findFieldByNumber(2);
      long seconds = (long) protoValue.getField(secondsFieldDescriptor);
      int nanos = (int) protoValue.getField(nanosFieldDescriptor);
      return Instant.ofEpochSecond(seconds, nanos);
    }
  }

  static class BeamPassThroughConverter<T> extends BeamWrapConverter<Object, T, T> {
    @Override
    protected @NonNull T convertNonNullUnwrapped(@NonNull T protoValue) {
      return protoValue;
    }
  }

  static class BeamRowConverter extends BeamNoWrapConverter<Message, Row> {
    private final SerializableFunction<@NonNull Message, @NonNull Row> converter;

    public BeamRowConverter(Schema.FieldType fieldType) {
      Schema rowSchema = Preconditions.checkNotNull(fieldType.getRowSchema());
      converter = toRow(rowSchema);
    }

    @Override
    protected @NonNull Row convertNonNull(@NonNull Message protoValue) {
      return converter.apply(protoValue);
    }
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
    private final BeamConverter<ProtoT, BeamT> converter;

    @SuppressWarnings("unchecked")
    FromProtoFieldGetter(Schema.Field field) {
      this.field = field;
      converter = (BeamConverter<ProtoT, BeamT>) createBeamConverter(field.getType());
    }

    @SuppressWarnings("unchecked")
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

        return converter.convert((ProtoT) protoValue);
      } catch (RuntimeException e) {
        throw new RuntimeException(
            String.format("Failed to get field from proto. field: %s", field.getName()), e);
      }
    }
  }

  static class FromProtoOneOfGetter implements FromProtoGetter<OneOfType.Value> {
    private final Schema.Field field;
    private final OneOfType oneOfType;
    private final Map<String, BeamConverter<Object, ?>> converter;

    FromProtoOneOfGetter(Schema.Field field) {
      this.field = field;
      this.oneOfType = Preconditions.checkNotNull(field.getType().getLogicalType(OneOfType.class));
      this.converter = createConverters(oneOfType.getOneOfSchema());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, BeamConverter<Object, ?>> createConverters(Schema schema) {
      Map<String, BeamConverter<Object, ?>> converters = new HashMap<>();
      for (Schema.Field field : schema.getFields()) {
        converters.put(
            field.getName(), (BeamConverter<Object, ?>) createBeamConverter(field.getType()));
      }
      return converters;
    }

    @Override
    public OneOfType.@Nullable Value getFromProto(Message message) {
      Descriptors.Descriptor descriptor = message.getDescriptorForType();
      for (Map.Entry<String, BeamConverter<Object, ?>> entry : converter.entrySet()) {
        String subFieldName = entry.getKey();
        try {
          BeamConverter<Object, ?> value = entry.getValue();
          Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(subFieldName);
          if (message.hasField(fieldDescriptor)) {
            Object protoValue = Preconditions.checkNotNull(message.getField(fieldDescriptor));
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

  static class ProtoByteStringConverter extends ProtoWrapConverter<byte[], ByteString> {
    ProtoByteStringConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    protected @NonNull ByteString convertNonNullUnwrapped(byte @NonNull [] beamValue) {
      return ByteString.copyFrom(beamValue);
    }
  }

  static class ProtoDurationConverter
      extends ProtoNoWrapConverter<Duration, com.google.protobuf.Duration> {
    ProtoDurationConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    protected com.google.protobuf.@NonNull Duration convertNonNull(@NonNull Duration beamValue) {
      return com.google.protobuf.Duration.newBuilder()
          .setSeconds(beamValue.getSeconds())
          .setNanos(beamValue.getNano())
          .build();
    }
  }

  static class ProtoEnumConverter
      extends ProtoNoWrapConverter<EnumerationType.Value, Descriptors.EnumValueDescriptor> {
    ProtoEnumConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    protected Descriptors.@NonNull EnumValueDescriptor convertNonNull(
        EnumerationType.@NonNull Value beamValue) {
      return getFieldDescriptor().getEnumType().findValueByNumber(beamValue.getValue());
    }
  }

  static class ProtoListConverter<BeamT, ProtoT>
      extends ProtoNoWrapConverter<Iterable<@NonNull BeamT>, List<@NonNull ProtoT>> {
    private final @NonNull ProtoConverter<BeamT, ProtoT> converter;

    @SuppressWarnings("unchecked")
    ProtoListConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
      converter = (ProtoConverter<BeamT, ProtoT>) createToProtoSingularConverter(fieldDescriptor);
    }

    @Override
    protected @NonNull List<@NonNull ProtoT> convertNonNull(
        @NonNull Iterable<@NonNull BeamT> beamValue) {
      List<@NonNull ProtoT> protoList = new ArrayList<>();
      for (@NonNull BeamT beamElement : beamValue) {
        ProtoT protoElement = Preconditions.checkNotNull(converter.convert(beamElement));
        protoList.add(protoElement);
      }
      return protoList;
    }
  }

  static class ProtoMapConverter<BeamKeyT, BeamValueT, ProtoKeyT, ProtoValueT>
      extends ProtoNoWrapConverter<Map<BeamKeyT, BeamValueT>, List<@NonNull Message>> {

    private final Descriptors.Descriptor mapDescriptor;
    private final Descriptors.FieldDescriptor keyDescriptor;
    private final Descriptors.FieldDescriptor valueDescriptor;
    private final ProtoConverter<BeamKeyT, ProtoKeyT> keyToProto;
    private final ProtoConverter<BeamValueT, ProtoValueT> valueToProto;

    @SuppressWarnings("unchecked")
    ProtoMapConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
      mapDescriptor = fieldDescriptor.getMessageType();
      keyDescriptor = mapDescriptor.findFieldByNumber(1);
      valueDescriptor = mapDescriptor.findFieldByNumber(2);
      keyToProto =
          (ProtoConverter<BeamKeyT, ProtoKeyT>) createToProtoSingularConverter(keyDescriptor);
      valueToProto =
          (ProtoConverter<BeamValueT, ProtoValueT>) createToProtoSingularConverter(valueDescriptor);
    }

    @Override
    protected @NonNull List<@NonNull Message> convertNonNull(
        @NonNull Map<BeamKeyT, BeamValueT> beamValue) {
      ImmutableList.Builder<Message> protoList = ImmutableList.builder();
      beamValue.forEach(
          (k, v) -> {
            DynamicMessage.Builder message = DynamicMessage.newBuilder(mapDescriptor);
            Object protoKey = Preconditions.checkNotNull(keyToProto.convert(k));
            message.setField(keyDescriptor, protoKey);
            Object protoValue = Preconditions.checkNotNull(valueToProto.convert(v));
            message.setField(valueDescriptor, protoValue);
            protoList.add(message.build());
          });
      return protoList.build();
    }
  }

  static class ProtoMessageConverter extends ProtoNoWrapConverter<Row, Message> {
    private final SerializableFunction<Row, Message> converter;

    ProtoMessageConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
      this.converter = toProto(fieldDescriptor.getMessageType());
    }

    @Override
    protected @NonNull Message convertNonNull(@NonNull Row beamValue) {
      return converter.apply(beamValue);
    }
  }

  static class ProtoPassThroughConverter<T> extends ProtoWrapConverter<T, T> {
    ProtoPassThroughConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    protected @NonNull T convertNonNullUnwrapped(@NonNull T beamValue) {
      return beamValue;
    }
  }

  static class ProtoTimestampConverter extends ProtoNoWrapConverter<Instant, Timestamp> {
    ProtoTimestampConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    protected @NonNull Timestamp convertNonNull(@NonNull Instant beamValue) {
      return com.google.protobuf.Timestamp.newBuilder()
          .setSeconds(beamValue.getEpochSecond())
          .setNanos(beamValue.getNano())
          .build();
    }
  }

  static class ToProto implements SerializableFunction<Row, Message> {
    private transient Descriptors.Descriptor descriptor;
    private transient Map<String, ToProtoSetter<?>> toProtos;

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
            toProtos.put(
                realContainingOneof.getName(), new ToProtoOneOfSetter(realContainingOneof));
          }
          // continue
        } else {
          toProtos.put(
              fieldDescriptor.getName(),
              new ToProtoFieldSetter<Object, Object>(fieldDescriptor) {});
        }
      }
    }

    @Override
    public Message apply(Row row) {
      Schema schema = row.getSchema();
      DynamicMessage.Builder message = DynamicMessage.newBuilder(descriptor);
      for (Map.Entry<String, ToProtoSetter<?>> entry : toProtos.entrySet()) {
        String fieldName = entry.getKey();
        @SuppressWarnings("unchecked")
        ToProtoSetter<Object> converter = (ToProtoSetter<Object>) entry.getValue();
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
    private final @NonNull ProtoConverter<BeamT, ProtoT> converter;

    @SuppressWarnings("unchecked")
    ToProtoFieldSetter(Descriptors.FieldDescriptor fieldDescriptor) {
      this.converter = (ProtoConverter<BeamT, ProtoT>) createToProtoConverter(fieldDescriptor);
    }

    @Override
    public void setToProto(
        Message.Builder message, Schema.FieldType fieldType, BeamT beamFieldValue) {
      try {
        @Nullable ProtoT protoValue = converter.convert(beamFieldValue);
        if (protoValue != null) {
          message.setField(converter.getFieldDescriptor(), protoValue);
        }
      } catch (RuntimeException e) {
        throw new RuntimeException(
            String.format(
                "Failed to set field to proto. field:%s", converter.getFieldDescriptor().getName()),
            e);
      }
    }
  }

  static class ToProtoOneOfSetter implements ToProtoSetter<OneOfType.Value> {
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
        Message.Builder message, Schema.FieldType fieldType, OneOfType.Value oneOfValue) {
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
