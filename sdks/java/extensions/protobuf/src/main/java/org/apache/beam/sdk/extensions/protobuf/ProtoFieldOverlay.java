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
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Instant;

/**
 * Protobuf ProtoFieldOverlay is the interface that each implementation needs to implement to handle
 * a specific field types.
 */
@Experimental(Experimental.Kind.SCHEMAS)
public interface ProtoFieldOverlay<ValueT> extends FieldValueGetter<Message, ValueT> {

  ValueT convertGetObject(FieldDescriptor fieldDescriptor, Object object);

  /** Convert the Row field and set it on the overlayed field of the message. */
  void set(Message.Builder object, ValueT value);

  Object convertSetObject(FieldDescriptor fieldDescriptor, Object value);

  /** Return the Beam Schema Field of this overlayed field. */
  Schema.Field getSchemaField();

  abstract class ProtoFieldOverlayBase<ValueT> implements ProtoFieldOverlay<ValueT> {

    protected int number;

    private Schema.Field field;

    FieldDescriptor getFieldDescriptor(Message message) {
      return message.getDescriptorForType().findFieldByNumber(number);
    }

    FieldDescriptor getFieldDescriptor(Message.Builder message) {
      return message.getDescriptorForType().findFieldByNumber(number);
    }

    protected void setField(Schema.Field field) {
      this.field = field;
    }

    ProtoFieldOverlayBase(ProtoSchema protoSchema, FieldDescriptor fieldDescriptor) {
      // this.fieldDescriptor = fieldDescriptor;
      this.number = fieldDescriptor.getNumber();
    }

    @Override
    public String name() {
      return field.getName();
    }

    @Override
    public Schema.Field getSchemaField() {
      return field;
    }
  }

  /** Overlay for Protobuf primitive types. Primitive values are just passed through. */
  class PrimitiveOverlay extends ProtoFieldOverlayBase<Object> {
    PrimitiveOverlay(ProtoSchema<?> protoSchema, FieldDescriptor fieldDescriptor) {
      // this.fieldDescriptor = fieldDescriptor;
      super(protoSchema, fieldDescriptor);
      setField(
          Schema.Field.of(
              fieldDescriptor.getName(),
              ProtoSchema.convertType(fieldDescriptor.getType())
                  .withMetadata(protoSchema.convertOptions(fieldDescriptor))));
    }

    @Override
    public Object get(Message message) {
      FieldDescriptor fieldDescriptor = getFieldDescriptor(message);
      return convertGetObject(fieldDescriptor, message.getField(fieldDescriptor));
    }

    @Override
    public Object convertGetObject(FieldDescriptor fieldDescriptor, Object object) {
      return object;
    }

    @Override
    public void set(Message.Builder message, Object value) {
      message.setField(getFieldDescriptor(message), value);
    }

    @Override
    public Object convertSetObject(FieldDescriptor fieldDescriptor, Object value) {
      return value;
    }
  }

  /**
   * Overlay for Bytes. Protobuf Bytes are natively represented as ByteStrings that requires special
   * handling for byte[] of size 0.
   */
  class BytesOverlay extends PrimitiveOverlay {
    BytesOverlay(ProtoSchema protoSchema, FieldDescriptor fieldDescriptor) {
      super(protoSchema, fieldDescriptor);
    }

    @Override
    public Object convertGetObject(FieldDescriptor fieldDescriptor, Object object) {
      // return object;
      return ((ByteString) object).toByteArray();
    }

    @Override
    public void set(Message.Builder message, Object value) {
      if (value != null && ((byte[]) value).length > 0) {
        // Protobuf messages BYTES doesn't like empty bytes?!
        FieldDescriptor fieldDescriptor = message.getDescriptorForType().findFieldByNumber(number);
        message.setField(fieldDescriptor, convertSetObject(fieldDescriptor, value));
      }
    }

    @Override
    public Object convertSetObject(FieldDescriptor fieldDescriptor, Object value) {
      if (value != null) {
        return ByteString.copyFrom((byte[]) value);
      }
      return null;
    }
  }

  /**
   * Overlay handler for the Well Known Type "Wrapper". These wrappers make it possible to have
   * nullable primitives.
   */
  class WrapperOverlay<ValueT> extends ProtoFieldOverlayBase<ValueT> {
    private ProtoFieldOverlay value;

    WrapperOverlay(ProtoSchema<?> protoSchema, FieldDescriptor fieldDescriptor) {
      super(protoSchema, fieldDescriptor);
      FieldDescriptor valueDescriptor = fieldDescriptor.getMessageType().findFieldByName("value");
      this.value = protoSchema.createFieldLayer(valueDescriptor, false);
      setField(
          Schema.Field.of(
              fieldDescriptor.getName(), value.getSchemaField().getType().withNullable(true)));
    }

    @Override
    public ValueT get(Message message) {
      if (message.hasField(getFieldDescriptor(message))) {
        Message wrapper = (Message) message.getField(getFieldDescriptor(message));
        return (ValueT) value.get(wrapper);
      }
      return null;
    }

    @Override
    public ValueT convertGetObject(FieldDescriptor fieldDescriptor, Object object) {
      return (ValueT) object;
    }

    @Override
    public void set(Message.Builder message, ValueT value) {
      if (value != null) {
        DynamicMessage.Builder builder =
            DynamicMessage.newBuilder(getFieldDescriptor(message).getMessageType());
        this.value.set(builder, value);
        message.setField(getFieldDescriptor(message), builder.build());
      }
    }

    @Override
    public Object convertSetObject(FieldDescriptor fieldDescriptor, Object value) {
      return value;
    }
  }

  /**
   * Overlay handler for the Well Known Type "Timestamp". This wrappers converts from a single Row
   * DATETIME and a protobuf "Timestamp" messsage.
   */
  class TimestampOverlay extends ProtoFieldOverlayBase<Instant> {
    TimestampOverlay(ProtoSchema<?> protoSchema, FieldDescriptor fieldDescriptor) {
      super(protoSchema, fieldDescriptor);
      setField(
          Schema.Field.of(
                  fieldDescriptor.getName(),
                  Schema.FieldType.DATETIME.withMetadata(
                      protoSchema.convertOptions(fieldDescriptor)))
              .withNullable(true));
    }

    @Override
    public Instant get(Message message) {
      FieldDescriptor fieldDescriptor = getFieldDescriptor(message);
      if (message.hasField(fieldDescriptor)) {
        Message wrapper = (Message) message.getField(fieldDescriptor);
        return convertGetObject(fieldDescriptor, wrapper);
      }
      return null;
    }

    @Override
    public Instant convertGetObject(FieldDescriptor fieldDescriptor, Object object) {
      Message timestamp = (Message) object;
      Descriptors.Descriptor timestampFieldDescriptor = timestamp.getDescriptorForType();
      return new Instant(
          (Long) timestamp.getField(timestampFieldDescriptor.findFieldByName("seconds")) * 1000
              + (Integer) timestamp.getField(timestampFieldDescriptor.findFieldByName("nanos"))
                  / 1000000);
    }

    @Override
    public void set(Message.Builder message, Instant value) {
      if (value != null) {
        long totalMillis = value.getMillis();
        long seconds = totalMillis / 1000;
        int ns = (int) (totalMillis % 1000 * 1000000);
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(seconds).setNanos(ns).build();
        message.setField(getFieldDescriptor(message), timestamp);
      }
    }

    @Override
    public Object convertSetObject(FieldDescriptor fieldDescriptor, Object value) {
      return value;
    }
  }

  /** This overlay converts a nested Message into a nested Row. */
  class MessageOverlay extends ProtoFieldOverlayBase<Object> {
    private final SerializableFunction toRowFunction;
    private final SerializableFunction fromRowFunction;

    MessageOverlay(ProtoSchema<?> rootProtoSchema, FieldDescriptor fieldDescriptor) {
      super(rootProtoSchema, fieldDescriptor);

      ProtoSchema protoSchema =
          ProtoSchema.newBuilder(rootProtoSchema).forDescriptor(fieldDescriptor.getMessageType());
      SchemaCoder<Message> schemaCoder = protoSchema.getSchemaCoder();
      toRowFunction = schemaCoder.getToRowFunction();
      fromRowFunction = schemaCoder.getFromRowFunction();
      setField(
          Schema.Field.of(
              fieldDescriptor.getName(),
              Schema.FieldType.row(protoSchema.getSchema())
                  .withMetadata(protoSchema.convertOptions(fieldDescriptor))
                  .withNullable(true)));
    }

    @Override
    public Object get(Message message) {
      FieldDescriptor fieldDescriptor = getFieldDescriptor(message);
      if (message.hasField(fieldDescriptor)) {
        return convertGetObject(fieldDescriptor, message.getField(fieldDescriptor));
      }
      return null;
    }

    @Override
    public Object convertGetObject(FieldDescriptor fieldDescriptor, Object object) {
      return toRowFunction.apply(object);
    }

    @Override
    public void set(Message.Builder message, Object value) {
      if (value != null) {
        FieldDescriptor fieldDescriptor = getFieldDescriptor(message);
        message.setField(fieldDescriptor, convertSetObject(fieldDescriptor, value));
      }
    }

    @Override
    public Object convertSetObject(FieldDescriptor fieldDescriptor, Object value) {
      return fromRowFunction.apply(value);
    }
  }

  /**
   * Proto has a well defined way of storing maps, by having a Message with two fields, named "key"
   * and "value" in a repeatable field. This overlay translates between Row.map and the Protobuf
   * map.
   */
  class MapOverlay extends ProtoFieldOverlayBase<Map> {
    private ProtoFieldOverlay key;
    private ProtoFieldOverlay value;

    MapOverlay(ProtoSchema<?> protoSchema, FieldDescriptor fieldDescriptor) {
      super(protoSchema, fieldDescriptor);
      key =
          protoSchema.createFieldLayer(
              fieldDescriptor.getMessageType().findFieldByName("key"), false);
      value =
          protoSchema.createFieldLayer(
              fieldDescriptor.getMessageType().findFieldByName("value"), false);
      setField(
          Schema.Field.of(
              fieldDescriptor.getName(),
              Schema.FieldType.map(
                      key.getSchemaField().getType(),
                      value
                          .getSchemaField()
                          .getType()
                          .withMetadata(protoSchema.convertOptions(fieldDescriptor)))
                  .withNullable(true)));
    }

    @Override
    public Map get(Message message) {
      List list = (List) message.getField(getFieldDescriptor(message));
      if (list.size() == 0) {
        return null;
      }
      Map rowMap = new HashMap();
      list.forEach(
          entry -> {
            Message entryMessage = (Message) entry;
            Descriptors.Descriptor entryDescriptor = entryMessage.getDescriptorForType();
            FieldDescriptor keyFieldDescriptor = entryDescriptor.findFieldByName("key");
            FieldDescriptor valueFieldDescriptor = entryDescriptor.findFieldByName("value");
            rowMap.put(
                key.convertGetObject(keyFieldDescriptor, entryMessage.getField(keyFieldDescriptor)),
                this.value.convertGetObject(
                    valueFieldDescriptor, entryMessage.getField(valueFieldDescriptor)));
          });
      return rowMap;
    }

    @Override
    public Map convertGetObject(FieldDescriptor fieldDescriptor, Object object) {
      throw new RuntimeException("?");
    }

    @Override
    public void set(Message.Builder message, Map map) {
      if (map != null) {
        FieldDescriptor fieldDescriptor = getFieldDescriptor(message);
        List messageMap = new ArrayList();
        map.forEach(
            (k, v) -> {
              DynamicMessage.Builder builder =
                  DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
              FieldDescriptor keyFieldDescriptor =
                  fieldDescriptor.getMessageType().findFieldByName("key");
              builder.setField(
                  keyFieldDescriptor, this.key.convertSetObject(keyFieldDescriptor, k));
              FieldDescriptor valueFieldDescriptor =
                  fieldDescriptor.getMessageType().findFieldByName("value");
              builder.setField(
                  valueFieldDescriptor, value.convertSetObject(valueFieldDescriptor, v));
              messageMap.add(builder.build());
            });
        message.setField(fieldDescriptor, messageMap);
      }
    }

    @Override
    public Object convertSetObject(FieldDescriptor fieldDescriptor, Object value) {
      return value;
    }
  }

  /**
   * This overlay handles repeatable fields. It handles the Array conversion, but delegates the
   * conversion of the individual elements to an embedded overlay.
   */
  class ArrayOverlay extends ProtoFieldOverlayBase<List> {
    private ProtoFieldOverlay element;

    ArrayOverlay(ProtoSchema<?> protoSchema, FieldDescriptor fieldDescriptor) {
      super(protoSchema, fieldDescriptor);
      this.element = protoSchema.createFieldLayer(fieldDescriptor, false);
      setField(
          Schema.Field.of(
              fieldDescriptor.getName(),
              Schema.FieldType.array(
                      element
                          .getSchemaField()
                          .getType()
                          .withMetadata(protoSchema.convertOptions(fieldDescriptor)))
                  .withNullable(true)));
    }

    @Override
    public List get(Message message) {
      FieldDescriptor fieldDescriptor = getFieldDescriptor(message);
      List list = (List) message.getField(fieldDescriptor);
      if (list.size() == 0) {
        return null;
      }
      List arrayList = new ArrayList<>();
      list.forEach(
          entry -> {
            arrayList.add(element.convertGetObject(fieldDescriptor, entry));
          });
      return arrayList;
    }

    @Override
    public List convertGetObject(FieldDescriptor fieldDescriptor, Object object) {
      throw new RuntimeException("?");
    }

    @Override
    public void set(Message.Builder message, List list) {
      if (list != null) {
        FieldDescriptor fieldDescriptor = getFieldDescriptor(message);
        List targetList = new ArrayList();
        list.forEach(
            (e) -> {
              targetList.add(element.convertSetObject(fieldDescriptor, e));
            });
        message.setField(fieldDescriptor, targetList);
      }
    }

    @Override
    public Object convertSetObject(FieldDescriptor fieldDescriptor, Object value) {
      return value;
    }
  }

  /** Enum overlay handles the conversion between a string and a ProtoBuf Enum. */
  class EnumOverlay extends ProtoFieldOverlayBase<Object> {

    EnumOverlay(ProtoSchema<?> protoSchema, FieldDescriptor fieldDescriptor) {
      super(protoSchema, fieldDescriptor);
      setField(
          Schema.Field.of(
              fieldDescriptor.getName(),
              Schema.FieldType.STRING.withMetadata(protoSchema.convertOptions(fieldDescriptor))));
    }

    @Override
    public Object get(Message message) {
      FieldDescriptor fieldDescriptor = getFieldDescriptor(message);
      return convertGetObject(fieldDescriptor, message.getField(fieldDescriptor));
    }

    @Override
    public Object convertGetObject(FieldDescriptor fieldDescriptor, Object in) {
      return in.toString();
    }

    @Override
    public void set(Message.Builder message, Object value) {
      //     builder.setField(fieldDescriptor,
      // convertSetObject(row.getString(fieldDescriptor.getName())));
      FieldDescriptor fieldDescriptor = getFieldDescriptor(message);
      message.setField(fieldDescriptor, convertSetObject(fieldDescriptor, value));
    }

    @Override
    public Object convertSetObject(FieldDescriptor fieldDescriptor, Object value) {
      Descriptors.EnumDescriptor enumType = fieldDescriptor.getEnumType();
      return enumType.findValueByName(value.toString());
    }
  }

  /**
   * This overlay handles nullable fields. If a primitive field needs to be nullable this overlay is
   * wrapped around the original overlay.
   */
  class NullableOverlay extends ProtoFieldOverlayBase<Object> {

    private ProtoFieldOverlay fieldOverlay;

    NullableOverlay(
        ProtoSchema<?> protoSchema,
        FieldDescriptor fieldDescriptor,
        ProtoFieldOverlay fieldOverlay) {
      super(protoSchema, fieldDescriptor);
      this.fieldOverlay = fieldOverlay;
      setField(fieldOverlay.getSchemaField().withNullable(true));
    }

    @Override
    public Object get(Message message) {
      if (message.hasField(getFieldDescriptor(message))) {
        return fieldOverlay.get(message);
      }
      return null;
    }

    @Override
    public Object convertGetObject(FieldDescriptor fieldDescriptor, Object object) {
      throw new RuntimeException("Value conversion should never be allowed in nullable fields");
    }

    @Override
    public void set(Message.Builder message, Object value) {
      if (value != null) {
        fieldOverlay.set(message, value);
      }
    }

    @Override
    public Object convertSetObject(FieldDescriptor fieldDescriptor, Object value) {
      throw new RuntimeException("Value conversion should never be allowed in nullable fields");
    }
  }
}
