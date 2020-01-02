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

import static org.apache.beam.sdk.extensions.protobuf.ProtoSchemaTranslator.SCHEMA_OPTION_META_NUMBER;
import static org.apache.beam.sdk.extensions.protobuf.ProtoSchemaTranslator.SCHEMA_OPTION_META_TYPE_NAME;
import static org.apache.beam.sdk.extensions.protobuf.ProtoSchemaTranslator.getFieldNumber;
import static org.apache.beam.sdk.extensions.protobuf.ProtoSchemaTranslator.withFieldNumber;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.NanosDuration;
import org.apache.beam.sdk.schemas.logicaltypes.NanosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;

@Experimental(Experimental.Kind.SCHEMAS)
public class ProtoDynamicMessageSchema<T> implements Serializable {
  public static final long serialVersionUID = 1L;

  /**
   * Context of the schema, the context can be generated from a source schema or descriptors. The
   * ability of converting back from Row to proto depends on the type of context.
   */
  private final Context context;

  /** The toRow function to convert the Message to a Row. */
  private transient SerializableFunction<T, Row> toRowFunction;

  /** The fromRow function to convert the Row to a Message. */
  private transient SerializableFunction<Row, T> fromRowFunction;

  /** List of field converters for each field in the row. */
  private transient List<Convert> converters;

  private ProtoDynamicMessageSchema(String messageName, ProtoDomain domain) {
    this.context = new DescriptorContext(messageName, domain);
    readResolve();
  }

  private ProtoDynamicMessageSchema(Context context) {
    this.context = context;
    readResolve();
  }

  /**
   * Create a new ProtoDynamicMessageSchema from a {@link ProtoDomain} and for a message. The
   * message need to be in the domain and needs to be the fully qualified name.
   */
  public static ProtoDynamicMessageSchema forDescriptor(ProtoDomain domain, String messageName) {
    return new ProtoDynamicMessageSchema(messageName, domain);
  }

  /**
   * Create a new ProtoDynamicMessageSchema from a {@link ProtoDomain} and for a descriptor. The
   * descriptor is only used for it's name, that name will be used for a search in the domain.
   */
  public static ProtoDynamicMessageSchema<DynamicMessage> forDescriptor(
      ProtoDomain domain, Descriptors.Descriptor descriptor) {
    return new ProtoDynamicMessageSchema<>(descriptor.getFullName(), domain);
  }

  static ProtoDynamicMessageSchema<?> forContext(Context context, Schema.Field field) {
    return new ProtoDynamicMessageSchema<>(context.getSubContext(field));
  }

  static ProtoDynamicMessageSchema<Message> forSchema(Schema schema) {
    return new ProtoDynamicMessageSchema<>(new Context(schema, Message.class));
  }

  /** Initialize the transient fields after deserialization or construction. */
  private Object readResolve() {
    converters = createConverters(context.getSchema());
    toRowFunction = new MessageToRowFunction();
    fromRowFunction = new RowToMessageFunction();
    return this;
  }

  Convert createConverter(Schema.Field field) {
    Schema.FieldType fieldType = field.getType();
    if (fieldType.getNullable()) {
      Schema.Field valueField =
          withFieldNumber(Schema.Field.of("value", Schema.FieldType.BOOLEAN), 1);
      switch (fieldType.getTypeName()) {
        case BYTE:
        case INT16:
        case INT32:
        case INT64:
        case FLOAT:
        case DOUBLE:
        case STRING:
        case BOOLEAN:
          return new WrapperConvert(field, new PrimitiveConvert(valueField));
        case BYTES:
          return new WrapperConvert(field, new BytesConvert(valueField));
        case LOGICAL_TYPE:
          String identifier = field.getType().getLogicalType().getIdentifier();
          switch (identifier) {
            case ProtoSchemaLogicalTypes.UInt32.IDENTIFIER:
            case ProtoSchemaLogicalTypes.UInt64.IDENTIFIER:
              return new WrapperConvert(field, new PrimitiveConvert(valueField));
            default:
          }
          // fall through
        default:
      }
    }

    switch (fieldType.getTypeName()) {
      case BYTE:
      case INT16:
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case BOOLEAN:
        return new PrimitiveConvert(field);
      case BYTES:
        return new BytesConvert(field);
      case ARRAY:
      case ITERABLE:
        return new ArrayConvert(this, field);
      case MAP:
        return new MapConvert(this, field);
      case LOGICAL_TYPE:
        String identifier = field.getType().getLogicalType().getIdentifier();
        switch (identifier) {
          case ProtoSchemaLogicalTypes.Fixed32.IDENTIFIER:
          case ProtoSchemaLogicalTypes.Fixed64.IDENTIFIER:
          case ProtoSchemaLogicalTypes.SFixed32.IDENTIFIER:
          case ProtoSchemaLogicalTypes.SFixed64.IDENTIFIER:
          case ProtoSchemaLogicalTypes.SInt32.IDENTIFIER:
          case ProtoSchemaLogicalTypes.SInt64.IDENTIFIER:
          case ProtoSchemaLogicalTypes.UInt32.IDENTIFIER:
          case ProtoSchemaLogicalTypes.UInt64.IDENTIFIER:
            return new LogicalTypeConvert(field, fieldType.getLogicalType());
          case NanosInstant.IDENTIFIER:
            return new TimestampConvert(field);
          case NanosDuration.IDENTIFIER:
            return new DurationConvert(field);
          case EnumerationType.IDENTIFIER:
            return new EnumConvert(field, fieldType.getLogicalType());
          case OneOfType.IDENTIFIER:
            return new OneOfConvert(this, field, fieldType.getLogicalType());
          default:
            throw new IllegalStateException("Unexpected logical type : " + identifier);
        }
      case ROW:
        return new MessageConvert(this, field);
      default:
        throw new IllegalStateException("Unexpected value: " + fieldType);
    }
  }

  private List<Convert> createConverters(Schema schema) {
    List<Convert> fieldOverlays = new ArrayList<>();
    for (Schema.Field field : schema.getFields()) {
      fieldOverlays.add(createConverter(field));
    }
    return fieldOverlays;
  }

  public Schema getSchema() {
    return context.getSchema();
  }

  public SerializableFunction<T, Row> getToRowFunction() {
    return toRowFunction;
  }

  public SerializableFunction<Row, T> getFromRowFunction() {
    return fromRowFunction;
  }

  /**
   * Context that only has enough information to convert a proto message to a Row. This can be used
   * for arbitrary conventions, like decoding messages in proto options.
   */
  static class Context<T> implements Serializable {
    private final Schema schema;

    /**
     * Base class for the protobuf message. Normally this is DynamicMessage, but as this schema
     * class is also used to decode protobuf options this can be normal Message instances.
     */
    private Class<T> baseClass;

    Context(Schema schema, Class<T> baseClass) {
      this.schema = schema;
      this.baseClass = baseClass;
    }

    public Schema getSchema() {
      return schema;
    }

    public Class<T> getBaseClass() {
      return baseClass;
    }

    public DynamicMessage.Builder invokeNewBuilder() {
      throw new IllegalStateException("Should not be calling invokeNewBuilder");
    }

    public Context getSubContext(Schema.Field field) {
      return new Context(field.getType().getRowSchema(), Message.class);
    }
  }

  /**
   * Context the contains the full {@link ProtoDomain} and a reference to the message name. The full
   * domain is needed for creating Rows back to the original proto messages.
   */
  static class DescriptorContext extends Context<DynamicMessage> {
    private final String messageName;
    private final ProtoDomain domain;
    private transient Descriptors.Descriptor descriptor;

    DescriptorContext(String messageName, ProtoDomain domain) {
      super(
          ProtoSchemaTranslator.getSchema(domain.getDescriptor(messageName)), DynamicMessage.class);
      this.messageName = messageName;
      this.domain = domain;
    }

    @Override
    public DynamicMessage.Builder invokeNewBuilder() {
      if (descriptor == null) {
        descriptor = domain.getDescriptor(messageName);
      }
      return DynamicMessage.newBuilder(descriptor);
    }

    @Override
    public Context getSubContext(Schema.Field field) {
      String messageName =
          field.getType().getRowSchema().getOptions().getValue(SCHEMA_OPTION_META_TYPE_NAME);
      return new DescriptorContext(messageName, domain);
    }
  }

  /**
   * Base converter class for converting from proto values to row values. The converter mainly works
   * on fields in proto messages but also has methods to convert individual elements (example, for
   * elements in Lists or Maps).
   */
  abstract static class Convert<ValueT, InT> {
    private int number;

    Convert(Schema.Field field) {
      Schema.Options options = field.getOptions();
      if (options.hasOption(SCHEMA_OPTION_META_NUMBER)) {
        this.number = options.getValue(SCHEMA_OPTION_META_NUMBER);
      } else {
        this.number = -1;
      }
    }

    FieldDescriptor getFieldDescriptor(Message message) {
      return message.getDescriptorForType().findFieldByNumber(number);
    }

    FieldDescriptor getFieldDescriptor(Message.Builder message) {
      return message.getDescriptorForType().findFieldByNumber(number);
    }

    /** Get a proto field and convert it into a row value. */
    abstract Object getFromProtoMessage(Message message);

    /** Convert a proto value into a row value. */
    abstract ValueT convertFromProtoValue(Object object);

    /** Convert a row value and set it on a proto message. */
    abstract void setOnProtoMessage(Message.Builder object, InT value);

    /** Convert a row value into a proto value. */
    abstract Object convertToProtoValue(FieldDescriptor fieldDescriptor, Object value);
  }

  /** Converter for primitive proto values. */
  static class PrimitiveConvert extends Convert<Object, Object> {
    PrimitiveConvert(Schema.Field field) {
      super(field);
    }

    @Override
    Object getFromProtoMessage(Message message) {
      FieldDescriptor fieldDescriptor = getFieldDescriptor(message);
      return convertFromProtoValue(message.getField(fieldDescriptor));
    }

    @Override
    Object convertFromProtoValue(Object object) {
      return object;
    }

    @Override
    void setOnProtoMessage(Message.Builder message, Object value) {
      message.setField(getFieldDescriptor(message), value);
    }

    @Override
    Object convertToProtoValue(FieldDescriptor fieldDescriptor, Object value) {
      return value;
    }
  }

  /**
   * Converter for Bytes. Protobuf Bytes are natively represented as ByteStrings that requires
   * special handling for byte[] of size 0.
   */
  static class BytesConvert extends PrimitiveConvert {
    BytesConvert(Schema.Field field) {
      super(field);
    }

    @Override
    Object convertFromProtoValue(Object object) {
      // return object;
      return ((ByteString) object).toByteArray();
    }

    @Override
    void setOnProtoMessage(Message.Builder message, Object value) {
      if (value != null && ((byte[]) value).length > 0) {
        // Protobuf messages BYTES doesn't like empty bytes?!
        FieldDescriptor fieldDescriptor = getFieldDescriptor(message);
        message.setField(fieldDescriptor, convertToProtoValue(fieldDescriptor, value));
      }
    }

    @Override
    Object convertToProtoValue(FieldDescriptor fieldDescriptor, Object value) {
      if (value != null) {
        return ByteString.copyFrom((byte[]) value);
      }
      return null;
    }
  }

  /**
   * Specific converter for Proto Wrapper values as they are translated into nullable row values.
   */
  static class WrapperConvert extends Convert<Object, Object> {
    private Convert valueConvert;

    WrapperConvert(Schema.Field field, Convert valueConvert) {
      super(field);
      this.valueConvert = valueConvert;
    }

    @Override
    Object getFromProtoMessage(Message message) {
      if (message.hasField(getFieldDescriptor(message))) {
        Message wrapper = (Message) message.getField(getFieldDescriptor(message));
        return valueConvert.getFromProtoMessage(wrapper);
      }
      return null;
    }

    @Override
    Object convertFromProtoValue(Object object) {
      return object;
    }

    @Override
    void setOnProtoMessage(Message.Builder message, Object value) {
      if (value != null) {
        DynamicMessage.Builder builder =
            DynamicMessage.newBuilder(getFieldDescriptor(message).getMessageType());
        valueConvert.setOnProtoMessage(builder, value);
        message.setField(getFieldDescriptor(message), builder.build());
      }
    }

    @Override
    Object convertToProtoValue(FieldDescriptor fieldDescriptor, Object value) {
      return value;
    }
  }

  static class TimestampConvert extends Convert<Object, Object> {

    TimestampConvert(Schema.Field field) {
      super(field);
    }

    @Override
    Object getFromProtoMessage(Message message) {
      FieldDescriptor fieldDescriptor = getFieldDescriptor(message);
      if (message.hasField(fieldDescriptor)) {
        Message wrapper = (Message) message.getField(fieldDescriptor);
        return convertFromProtoValue(wrapper);
      }
      return null;
    }

    @Override
    Object convertFromProtoValue(Object object) {
      Message timestamp = (Message) object;
      Descriptors.Descriptor timestampDescriptor = timestamp.getDescriptorForType();
      FieldDescriptor secondField = timestampDescriptor.findFieldByNumber(1);
      FieldDescriptor nanoField = timestampDescriptor.findFieldByNumber(2);
      long second = (long) timestamp.getField(secondField);
      int nano = (int) timestamp.getField(nanoField);
      return Instant.ofEpochSecond(second, nano);
    }

    @Override
    void setOnProtoMessage(Message.Builder message, Object value) {
      if (value != null) {
        FieldDescriptor fieldDescriptor = getFieldDescriptor(message);
        message.setField(fieldDescriptor, convertToProtoValue(fieldDescriptor, value));
      }
    }

    @Override
    Object convertToProtoValue(FieldDescriptor fieldDescriptor, Object value) {
      Instant ts = (Instant) value;
      return com.google.protobuf.Timestamp.newBuilder()
          .setSeconds(ts.getEpochSecond())
          .setNanos(ts.getNano())
          .build();
    }
  }

  static class DurationConvert extends Convert<Object, Object> {

    DurationConvert(Schema.Field field) {
      super(field);
    }

    @Override
    Object getFromProtoMessage(Message message) {
      FieldDescriptor fieldDescriptor = getFieldDescriptor(message);
      if (message.hasField(fieldDescriptor)) {
        Message wrapper = (Message) message.getField(fieldDescriptor);
        return convertFromProtoValue(wrapper);
      }
      return null;
    }

    @Override
    Duration convertFromProtoValue(Object object) {
      Message timestamp = (Message) object;
      Descriptors.Descriptor timestampDescriptor = timestamp.getDescriptorForType();
      FieldDescriptor secondField = timestampDescriptor.findFieldByNumber(1);
      FieldDescriptor nanoField = timestampDescriptor.findFieldByNumber(2);
      long second = (long) timestamp.getField(secondField);
      int nano = (int) timestamp.getField(nanoField);
      return Duration.ofSeconds(second, nano);
    }

    @Override
    void setOnProtoMessage(Message.Builder message, Object value) {
      if (value != null) {
        FieldDescriptor fieldDescriptor = getFieldDescriptor(message);
        message.setField(fieldDescriptor, convertToProtoValue(fieldDescriptor, value));
      }
    }

    @Override
    Object convertToProtoValue(FieldDescriptor fieldDescriptor, Object value) {
      Duration duration = (Duration) value;
      return com.google.protobuf.Duration.newBuilder()
          .setSeconds(duration.getSeconds())
          .setNanos(duration.getNano())
          .build();
    }
  }

  static class MessageConvert extends Convert<Object, Object> {
    private final SerializableFunction fromRowFunction;
    private final SerializableFunction toRowFunction;

    MessageConvert(ProtoDynamicMessageSchema rootProtoSchema, Schema.Field field) {
      super(field);
      ProtoDynamicMessageSchema protoSchema =
          ProtoDynamicMessageSchema.forContext(rootProtoSchema.context, field);
      toRowFunction = protoSchema.getToRowFunction();
      fromRowFunction = protoSchema.getFromRowFunction();
    }

    @Override
    Object getFromProtoMessage(Message message) {
      FieldDescriptor fieldDescriptor = getFieldDescriptor(message);
      if (message.hasField(fieldDescriptor)) {
        return convertFromProtoValue(message.getField(fieldDescriptor));
      }
      return null;
    }

    @Override
    Object convertFromProtoValue(Object object) {
      return toRowFunction.apply(object);
    }

    @Override
    void setOnProtoMessage(Message.Builder message, Object value) {
      if (value != null) {
        FieldDescriptor fieldDescriptor = getFieldDescriptor(message);
        message.setField(fieldDescriptor, convertToProtoValue(fieldDescriptor, value));
      }
    }

    @Override
    Object convertToProtoValue(FieldDescriptor fieldDescriptor, Object value) {
      return fromRowFunction.apply(value);
    }
  }

  /**
   * Proto has a well defined way of storing maps, by having a Message with two fields, named "key"
   * and "value" in a repeatable field. This overlay translates between Row.map and the Protobuf
   * map.
   */
  static class MapConvert extends Convert<Map, Map> {
    private Convert key;
    private Convert value;

    MapConvert(ProtoDynamicMessageSchema protoSchema, Schema.Field field) {
      super(field);
      Schema.FieldType fieldType = field.getType();
      key = protoSchema.createConverter(Schema.Field.of("KEY", fieldType.getMapKeyType()));
      value = protoSchema.createConverter(Schema.Field.of("VALUE", fieldType.getMapValueType()));
    }

    @Override
    Map getFromProtoMessage(Message message) {
      List<Message> list = (List<Message>) message.getField(getFieldDescriptor(message));
      Map<Object, Object> rowMap = new HashMap<>();
      if (list.size() == 0) {
        return rowMap;
      }
      list.forEach(
          entryMessage -> {
            Descriptors.Descriptor entryDescriptor = entryMessage.getDescriptorForType();
            FieldDescriptor keyFieldDescriptor = entryDescriptor.findFieldByName("key");
            FieldDescriptor valueFieldDescriptor = entryDescriptor.findFieldByName("value");
            rowMap.put(
                key.convertFromProtoValue(entryMessage.getField(keyFieldDescriptor)),
                this.value.convertFromProtoValue(entryMessage.getField(valueFieldDescriptor)));
          });
      return rowMap;
    }

    @Override
    Map convertFromProtoValue(Object object) {
      throw new RuntimeException("?");
    }

    @Override
    void setOnProtoMessage(Message.Builder message, Map map) {
      if (map != null) {
        FieldDescriptor fieldDescriptor = getFieldDescriptor(message);
        List<Message> messageMap = new ArrayList<>();
        map.forEach(
            (k, v) -> {
              DynamicMessage.Builder builder =
                  DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
              FieldDescriptor keyFieldDescriptor =
                  fieldDescriptor.getMessageType().findFieldByName("key");
              builder.setField(
                  keyFieldDescriptor, this.key.convertToProtoValue(keyFieldDescriptor, k));
              FieldDescriptor valueFieldDescriptor =
                  fieldDescriptor.getMessageType().findFieldByName("value");
              builder.setField(
                  valueFieldDescriptor, value.convertToProtoValue(valueFieldDescriptor, v));
              messageMap.add(builder.build());
            });
        message.setField(fieldDescriptor, messageMap);
      }
    }

    @Override
    Object convertToProtoValue(FieldDescriptor fieldDescriptor, Object value) {
      return value;
    }
  }

  static class ArrayConvert extends Convert<List, List> {
    private Convert element;

    ArrayConvert(ProtoDynamicMessageSchema protoSchema, Schema.Field field) {
      super(field);
      Schema.FieldType collectionElementType = field.getType().getCollectionElementType();
      this.element = protoSchema.createConverter(Schema.Field.of("ELEMENT", collectionElementType));
    }

    @Override
    List getFromProtoMessage(Message message) {
      FieldDescriptor fieldDescriptor = getFieldDescriptor(message);
      return convertFromProtoValue(message.getField(fieldDescriptor));
    }

    @Override
    List convertFromProtoValue(Object value) {
      List list = (List) value;
      List<Object> arrayList = new ArrayList<>();
      list.forEach(
          entry -> {
            arrayList.add(element.convertFromProtoValue(entry));
          });
      return arrayList;
    }

    @Override
    void setOnProtoMessage(Message.Builder message, List list) {
      if (list != null) {
        FieldDescriptor fieldDescriptor = getFieldDescriptor(message);
        List<Object> targetList = new ArrayList<>();
        list.forEach(
            (e) -> {
              targetList.add(element.convertToProtoValue(fieldDescriptor, e));
            });
        message.setField(fieldDescriptor, targetList);
      }
    }

    @Override
    Object convertToProtoValue(FieldDescriptor fieldDescriptor, Object value) {
      return value;
    }
  }

  /** Enum overlay handles the conversion between a string and a ProtoBuf Enum. */
  static class EnumConvert extends Convert<Object, Object> {
    EnumerationType logicalType;

    EnumConvert(Schema.Field field, Schema.LogicalType logicalType) {
      super(field);
      this.logicalType = (EnumerationType) logicalType;
    }

    @Override
    Object getFromProtoMessage(Message message) {
      FieldDescriptor fieldDescriptor = getFieldDescriptor(message);
      return convertFromProtoValue(message.getField(fieldDescriptor));
    }

    @Override
    EnumerationType.Value convertFromProtoValue(Object in) {
      return logicalType.valueOf(((Descriptors.EnumValueDescriptor) in).getNumber());
    }

    @Override
    void setOnProtoMessage(Message.Builder message, Object value) {
      FieldDescriptor fieldDescriptor = getFieldDescriptor(message);
      message.setField(fieldDescriptor, convertToProtoValue(fieldDescriptor, value));
    }

    @Override
    Object convertToProtoValue(FieldDescriptor fieldDescriptor, Object value) {
      Descriptors.EnumDescriptor enumType = fieldDescriptor.getEnumType();
      return enumType.findValueByNumber(((EnumerationType.Value) value).getValue());
    }
  }

  /** Convert Proto oneOf fields into the {@link OneOfType} logical type. */
  static class OneOfConvert extends Convert<OneOfType.Value, OneOfType.Value> {
    OneOfType logicalType;
    Map<Integer, Convert> oneOfConvert = new HashMap<>();

    OneOfConvert(
        ProtoDynamicMessageSchema protoSchema, Schema.Field field, Schema.LogicalType logicalType) {
      super(field);
      this.logicalType = (OneOfType) logicalType;
      for (Schema.Field oneOfField : this.logicalType.getOneOfSchema().getFields()) {
        int fieldNumber = getFieldNumber(oneOfField);
        oneOfConvert.put(
            fieldNumber,
            new NullableConvert(
                oneOfField, protoSchema.createConverter(oneOfField.withNullable(false))));
      }
    }

    @Override
    Object getFromProtoMessage(Message message) {
      for (Map.Entry<Integer, Convert> entry : this.oneOfConvert.entrySet()) {
        Object value = entry.getValue().getFromProtoMessage(message);
        if (value != null) {
          return logicalType.createValue(entry.getKey(), value);
        }
      }
      return null;
    }

    @Override
    OneOfType.Value convertFromProtoValue(Object in) {
      throw new IllegalStateException("Value conversion can't be done outside a protobuf message");
    }

    @Override
    void setOnProtoMessage(Message.Builder message, OneOfType.Value oneOf) {
      int caseIndex = oneOf.getCaseType().getValue();
      oneOfConvert.get(caseIndex).setOnProtoMessage(message, oneOf.getValue());
    }

    @Override
    Object convertToProtoValue(FieldDescriptor fieldDescriptor, Object value) {
      throw new IllegalStateException("Value conversion can't be done outside a protobuf message");
    }
  }

  /**
   * This overlay handles nullable fields. If a primitive field needs to be nullable this overlay is
   * wrapped around the original overlay.
   */
  static class NullableConvert extends Convert<Object, Object> {

    private Convert fieldOverlay;

    NullableConvert(Schema.Field field, Convert fieldOverlay) {
      super(field);
      this.fieldOverlay = fieldOverlay;
    }

    @Override
    Object getFromProtoMessage(Message message) {
      if (message.hasField(getFieldDescriptor(message))) {
        return fieldOverlay.getFromProtoMessage(message);
      }
      return null;
    }

    @Override
    Object convertFromProtoValue(Object object) {
      throw new IllegalStateException("Value conversion can't be done outside a protobuf message");
    }

    @Override
    void setOnProtoMessage(Message.Builder message, Object value) {
      if (value != null) {
        fieldOverlay.setOnProtoMessage(message, value);
      }
    }

    @Override
    Object convertToProtoValue(FieldDescriptor fieldDescriptor, Object value) {
      throw new IllegalStateException("Value conversion can't be done outside a protobuf message");
    }
  }

  static class LogicalTypeConvert extends Convert<Object, Object> {

    private Schema.LogicalType logicalType;

    LogicalTypeConvert(Schema.Field field, Schema.LogicalType logicalType) {
      super(field);
      this.logicalType = logicalType;
    }

    @Override
    Object getFromProtoMessage(Message message) {
      FieldDescriptor fieldDescriptor = getFieldDescriptor(message);
      return convertFromProtoValue(message.getField(fieldDescriptor));
    }

    @Override
    Object convertFromProtoValue(Object object) {
      return logicalType.toBaseType(object);
    }

    @Override
    void setOnProtoMessage(Message.Builder message, Object value) {
      message.setField(getFieldDescriptor(message), value);
    }

    @Override
    Object convertToProtoValue(FieldDescriptor fieldDescriptor, Object value) {
      return value;
    }
  }

  private class MessageToRowFunction implements SerializableFunction<T, Row> {

    private MessageToRowFunction() {}

    @Override
    public Row apply(T input) {
      Schema schema = context.getSchema();
      Row.Builder builder = Row.withSchema(schema);
      for (Convert convert : converters) {
        builder.addValue(convert.getFromProtoMessage((Message) input));
      }
      return builder.build();
    }
  }

  private class RowToMessageFunction implements SerializableFunction<Row, T> {

    private RowToMessageFunction() {}

    @Override
    public T apply(Row input) {
      DynamicMessage.Builder builder = context.invokeNewBuilder();
      Iterator values = input.getValues().iterator();
      Iterator<Convert> convertIterator = converters.iterator();

      for (int i = 0; i < input.getValues().size(); i++) {
        Convert convert = convertIterator.next();
        Object value = values.next();
        convert.setOnProtoMessage(builder, value);
      }
      return (T) builder.build();
    }
  }
}
