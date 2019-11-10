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

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Factory;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * ProtoSchema is a top level anchor point. It makes sure it can recreate the complete schema and
 * overlay with just the Message raw type or if it's a DynamicMessage with the serialised
 * Descriptor.
 *
 * <p>ProtoDomain is an integral part of a ProtoSchema, it it contains all the information needed to
 * iterpret and reconstruct messages.
 *
 * <ul>
 *   <li>Protobuf oneOf fields are mapped to nullable fields and flattened into the parent row.
 *   <li>Protobuf primitives are mapped to it's non nullable counter part.
 *   <li>Protobuf maps are mapped to nullable maps, where empty maps are mapped to the null value.
 *   <li>Protobuf repeatables are mapped to nullable arrays, where empty arrays are mapped to the
 *       null value.
 *   <li>Protobuf enums are mapped to non-nullable string values.
 * </ul>
 *
 * <p>Protobuf Well Know Types are handled by the Beam Schema system. Beam knows of the following
 * Well Know Types:
 *
 * <ul>
 *   <li>google.protobuf.Timestamp maps to a nullable Field.DATATIME
 *   <li>google.protobuf.StringValue maps to a nullable Field.STRING
 *   <li>google.protobuf.DoubleValue maps to a nullable Field.DOUBLE
 *   <li>google.protobuf.FloatValue maps to a nullable Field.FLOAT
 *   <li>google.protobuf.BytesValue maps to a nullable Field.BYTES
 *   <li>google.protobuf.BoolValue maps to a nullable Field.BOOL
 *   <li>google.protobuf.Int64Value maps to a nullable Field.INT64
 *   <li>google.protobuf.Int32Value maps to a nullable Field.INT32
 *   <li>google.protobuf.UInt64Value maps to a nullable Field.INT64
 *   <li>google.protobuf.UInt32Value maps to a nullable Field.INT32
 * </ul>
 */
@Experimental(Experimental.Kind.SCHEMAS)
public class ProtoSchema<T extends Message> implements Serializable {
  public static final long serialVersionUID = 1L;
  private static final ProtoDomain STATIC_COMPILED_DOMAIN = new ProtoDomain();
  private static Map<UUID, ProtoSchema> globalSchemaCache = new HashMap<>();
  private final Class<T> rawType;
  private final Map<String, Class> typeMapping;
  private final ProtoDomain domain;
  private transient Descriptors.Descriptor descriptor;
  private transient SchemaCoder schemaCoder;
  private transient Method fnNewBuilder;
  private transient ArrayList<ProtoFieldOverlay> getters;

  private ProtoSchema(
      Class<T> rawType,
      Descriptors.Descriptor descriptor,
      ProtoDomain domain,
      Map<String, Class> overlayClasses) {
    this.rawType = rawType;
    this.descriptor = descriptor;
    this.typeMapping = overlayClasses;
    this.domain = domain;
    init();
  }

  /**
   * Create a new ProtoSchema Builder with the static compiled proto domain. This domain references
   * only statically compiled Java Protobuf messages.
   */
  public static Builder newBuilder() {
    return new Builder(STATIC_COMPILED_DOMAIN);
  }

  /**
   * Create a new ProtoSchema Builder with a specific proto domain. It does not contain any messages
   * of the static domain. A Domain is used for grouping different messages that belong together.
   * Creating different schema builders with the same domain is safe. The resulting Protobuf
   * messages created from the same domain with be equal.
   */
  public static Builder newBuilder(ProtoDomain protoDomain) {
    return new Builder(protoDomain);
  }

  static Builder newBuilder(ProtoSchema protoSchema) {
    return new Builder(protoSchema.domain).addTypeMapping(protoSchema.typeMapping);
  }

  static ProtoSchema fromSchema(Schema schema) {
    return globalSchemaCache.get(schema.getUUID());
  }

  static Schema.FieldType convertType(Descriptors.FieldDescriptor.Type type) {
    switch (type) {
      case DOUBLE:
        return Schema.FieldType.DOUBLE;
      case FLOAT:
        return Schema.FieldType.FLOAT;
      case INT64:
      case UINT64:
      case SINT64:
      case FIXED64:
      case SFIXED64:
        return Schema.FieldType.INT64;
      case INT32:
      case FIXED32:
      case UINT32:
      case SFIXED32:
      case SINT32:
        return Schema.FieldType.INT32;
      case BOOL:
        return Schema.FieldType.BOOLEAN;
      case STRING:
      case ENUM:
        return Schema.FieldType.STRING;
      case BYTES:
        return Schema.FieldType.BYTES;
      case MESSAGE:
      case GROUP:
        break;
    }
    throw new RuntimeException("Field type not matched.");
  }

  Map<String, byte[]> convertOptions(Descriptors.FieldDescriptor protoField) {
    Map<String, byte[]> metadata = new HashMap<>();
    DescriptorProtos.FieldOptions options = protoField.getOptions();
    options
        .getAllFields()
        .forEach(
            (fd, value) -> {
              String name = fd.getFullName();
              if (name.startsWith("google.protobuf.FieldOptions")) {
                name = fd.getName();
              }
              if (value instanceof Message) {
                Message message = (Message) value;
                Descriptors.Descriptor descriptorForType = message.getDescriptorForType();
                List<Descriptors.FieldDescriptor> fields = descriptorForType.getFields();
                for (Descriptors.FieldDescriptor field : fields) {
                  metadata.put(
                      name + "." + field.getName(),
                      message.getField(field).toString().getBytes(StandardCharsets.UTF_8));
                }
              } else {
                metadata.put(name, value.toString().getBytes(StandardCharsets.UTF_8));
              }
            });

    options
        .getUnknownFields()
        .asMap()
        .forEach(
            (ix, ufs) -> {
              Descriptors.FieldDescriptor fieldOptionById = domain.getFieldOptionById(ix);
              if (fieldOptionById != null) {
                String name = fieldOptionById.getFullName();
                decodeUnknownOptionValue(metadata, name, fieldOptionById, ufs);
              }
            });
    return metadata;
  }

  private void decodeUnknownOptionValue(
      Map<String, byte[]> metadata,
      String name,
      Descriptors.FieldDescriptor fieldDescriptor,
      UnknownFieldSet.Field value) {

    switch (fieldDescriptor.getType()) {
      case MESSAGE:
        break;
      case FIXED64:
        metadata.put(
            name,
            value.getFixed64List().stream()
                .map(
                    l -> {
                      if (l >= 0) {
                        return Long.toString(l);
                      } else {
                        return BigInteger.valueOf(l & 0x7FFFFFFFFFFFFFFFL).setBit(63).toString();
                      }
                    })
                .collect(Collectors.joining("\n"))
                .getBytes(StandardCharsets.UTF_8));
        break;
      case FIXED32:
        break;
      case BOOL:
        metadata.put(
            name,
            value.getVarintList().stream()
                .map(l -> Boolean.valueOf(l.intValue() > 0).toString())
                .collect(Collectors.joining("\n"))
                .getBytes(StandardCharsets.UTF_8));
        break;
      case ENUM:
        metadata.put(
            name,
            value.getVarintList().stream()
                .map(l -> fieldDescriptor.getEnumType().findValueByNumber(l.intValue()).getName())
                .collect(Collectors.joining("\n"))
                .getBytes(StandardCharsets.UTF_8));
        break;
      case STRING:
        metadata.put(
            name,
            value.getLengthDelimitedList().stream()
                .map(l -> (l.toString(StandardCharsets.UTF_8)))
                .collect(Collectors.joining("\n"))
                .getBytes(StandardCharsets.UTF_8));
        break;
      case INT32:
      case INT64:
      case SINT32:
      case SINT64:
      case UINT32:
        metadata.put(
            name,
            value.getVarintList().stream()
                .map(l -> l.toString())
                .collect(Collectors.joining("\n"))
                .getBytes(StandardCharsets.UTF_8));
        break;
      case UINT64:
        metadata.put(
            name,
            value.getVarintList().stream()
                .map(
                    l -> {
                      if (l >= 0) {
                        return Long.toString(l);
                      } else {
                        return BigInteger.valueOf(l & 0x7FFFFFFFFFFFFFFFL).setBit(63).toString();
                      }
                    })
                .collect(Collectors.joining("\n"))
                .getBytes(StandardCharsets.UTF_8));
        break;
      case DOUBLE:
        metadata.put(
            name,
            value.getFixed64List().stream()
                .map(l -> String.valueOf(Double.longBitsToDouble(l)))
                .collect(Collectors.joining("\n"))
                .getBytes(StandardCharsets.UTF_8));
        break;
      case FLOAT:
        metadata.put(
            name,
            value.getFixed32List().stream()
                .map(l -> String.valueOf(Float.intBitsToFloat(l)))
                .collect(Collectors.joining("\n"))
                .getBytes(StandardCharsets.UTF_8));
        break;
      case BYTES:
        if (value.getLengthDelimitedList().size() > 0) {
          metadata.put(name, value.getLengthDelimitedList().get(0).toByteArray());
        }
        break;
      case SFIXED32:
        break;
      case SFIXED64:
        break;
      case GROUP:
        break;
      default:
        throw new IllegalStateException(
            "Conversion of Unknown Field for type "
                + fieldDescriptor.getType().toString()
                + " not implemented");
    }
  }

  private static boolean isMap(Descriptors.FieldDescriptor protoField) {
    return protoField.getType() == Descriptors.FieldDescriptor.Type.MESSAGE
        && protoField.getMessageType().getFullName().endsWith("Entry")
        && (protoField.getMessageType().findFieldByName("key") != null)
        && (protoField.getMessageType().findFieldByName("value") != null);
  }

  ProtoFieldOverlay createFieldLayer(Descriptors.FieldDescriptor protoField, boolean nullable) {
    Descriptors.FieldDescriptor.Type fieldDescriptor = protoField.getType();
    ProtoFieldOverlay fieldOverlay;
    switch (fieldDescriptor) {
      case DOUBLE:
      case FLOAT:
      case INT64:
      case UINT64:
      case SINT64:
      case FIXED64:
      case SFIXED64:
      case INT32:
      case FIXED32:
      case UINT32:
      case SFIXED32:
      case SINT32:
      case BOOL:
      case STRING:
        fieldOverlay = new ProtoFieldOverlay.PrimitiveOverlay(this, protoField);
        break;
      case BYTES:
        fieldOverlay = new ProtoFieldOverlay.BytesOverlay(this, protoField);
        break;
      case ENUM:
        fieldOverlay = new ProtoFieldOverlay.EnumOverlay(this, protoField);
        break;
      case MESSAGE:
        String fullName = protoField.getMessageType().getFullName();
        if (typeMapping.containsKey(fullName)) {
          Class aClass = typeMapping.get(fullName);
          try {
            Constructor constructor = aClass.getConstructor(Descriptors.FieldDescriptor.class);
            return (ProtoFieldOverlay) constructor.newInstance(protoField);
          } catch (NoSuchMethodException e) {
            throw new RuntimeException("Unable to find constructor for Overlay mapper.");
          } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException("Unable to invoke Overlay mapper.");
          }
        }
        switch (fullName) {
          case "google.protobuf.Timestamp":
            return new ProtoFieldOverlay.TimestampOverlay(this, protoField);
          case "google.protobuf.StringValue":
          case "google.protobuf.DoubleValue":
          case "google.protobuf.FloatValue":
          case "google.protobuf.BoolValue":
          case "google.protobuf.Int64Value":
          case "google.protobuf.Int32Value":
          case "google.protobuf.UInt64Value":
          case "google.protobuf.UInt32Value":
          case "google.protobuf.BytesValue":
            return new ProtoFieldOverlay.WrapperOverlay(this, protoField);
          case "google.protobuf.Duration":
          default:
            if (isMap(protoField)) {
              return new ProtoFieldOverlay.MapOverlay(this, protoField);
            } else {
              return new ProtoFieldOverlay.MessageOverlay(this, protoField);
            }
        }
      case GROUP:
      default:
        throw new RuntimeException("Field type not matched.");
    }
    if (nullable) {
      return new ProtoFieldOverlay.NullableOverlay(this, protoField, fieldOverlay);
    }
    return fieldOverlay;
  }

  private ArrayList<ProtoFieldOverlay> createFieldLayer(Descriptors.Descriptor descriptor) {
    // Oneof fields are nullable, even as they are primitive or enums
    List<Descriptors.FieldDescriptor> oneofMap =
        descriptor.getOneofs().stream()
            .flatMap(oneofDescriptor -> oneofDescriptor.getFields().stream())
            .collect(Collectors.toList());

    ArrayList<ProtoFieldOverlay> fieldOverlays = new ArrayList<>();
    Iterator<Descriptors.FieldDescriptor> protoFields = descriptor.getFields().iterator();
    for (int i = 0; i < descriptor.getFields().size(); i++) {
      Descriptors.FieldDescriptor protoField = protoFields.next();
      if (protoField.isRepeated() && !isMap(protoField)) {
        fieldOverlays.add(new ProtoFieldOverlay.ArrayOverlay(this, protoField));
      } else {
        fieldOverlays.add(createFieldLayer(protoField, oneofMap.contains(protoField)));
      }
    }
    return fieldOverlays;
  }

  private void init() {
    this.getters = createFieldLayer(descriptor);

    Schema.Builder builder = Schema.builder();
    for (ProtoFieldOverlay field : getters) {
      builder.addField(field.getSchemaField());
    }

    Schema schema = builder.build();
    schema.setUUID(UUID.randomUUID());
    schemaCoder =
        SchemaCoder.of(
            schema,
            TypeDescriptor.of(rawType),
            new MessageToRowFunction(),
            new RowToMessageFunction());

    globalSchemaCache.put(schema.getUUID(), this);
    try {
      if (DynamicMessage.class.equals(rawType)) {
        this.fnNewBuilder = rawType.getMethod("newBuilder", Descriptors.Descriptor.class);
      } else {
        this.fnNewBuilder = rawType.getMethod("newBuilder");
      }
    } catch (NoSuchMethodException e) {
    }
  }

  public Schema getSchema() {
    return this.schemaCoder.getSchema();
  }

  public SchemaCoder<T> getSchemaCoder() {
    return schemaCoder;
  }

  public SerializableFunction<T, Row> getToRowFunction() {
    return schemaCoder.getToRowFunction();
  }

  public SerializableFunction<Row, T> getFromRowFunction() {
    return schemaCoder.getFromRowFunction();
  }

  private void writeObject(ObjectOutputStream oos) throws IOException {
    oos.defaultWriteObject();
    if (DynamicMessage.class.equals(this.rawType)) {
      if (this.descriptor == null) {
        throw new RuntimeException("DynamicMessages require provider a Descriptor to the coder.");
      }
      oos.writeUTF(descriptor.getFullName());
    }
  }

  private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
    ois.defaultReadObject();
    if (DynamicMessage.class.equals(rawType)) {
      descriptor = domain.getDescriptor(ois.readUTF());
    } else {
      descriptor = ProtobufUtil.getDescriptorForClass(rawType);
    }
    init();
  }

  public ProtoDomain getDomain() {
    return domain;
  }

  public static class Builder implements Serializable {

    private ProtoDomain domain;
    private Map<String, Class> mappings = new HashMap<>();

    public Builder(ProtoDomain domain) {
      this.domain = domain;
    }

    public Builder addTypeMapping(Map<String, Class> mappings) {
      this.mappings.putAll(mappings);
      return this;
    }

    public Builder addTypeMapping(String message, Class mappingClass) {
      this.mappings.put(message, mappingClass);
      return this;
    }

    public ProtoSchema forType(Class rawType) {
      return new ProtoSchema(
          rawType,
          ProtobufUtil.getDescriptorForClass(rawType),
          domain,
          ImmutableMap.copyOf(mappings));
    }

    public ProtoSchema forDescriptor(Descriptors.Descriptor descriptor) {
      return new ProtoSchema(
          DynamicMessage.class, descriptor, domain, ImmutableMap.copyOf(mappings));
    }
  }

  /** Overlay. */
  public static class ProtoOverlayFactory implements Factory<List<FieldValueGetter>> {

    public ProtoOverlayFactory() {}

    @Override
    public List<FieldValueGetter> create(Class<?> clazz, Schema schema) {
      return ProtoSchema.fromSchema(schema).getters;
    }
  }

  private class MessageToRowFunction implements SerializableFunction<T, Row> {

    private MessageToRowFunction() {}

    @Override
    public Row apply(Message input) {
      return Row.withSchema(schemaCoder.getSchema())
          .withFieldValueGettersHandleCollections(true)
          .withFieldValueGetters(new ProtoOverlayFactory(), input)
          .build();
    }
  }

  private class RowToMessageFunction implements SerializableFunction<Row, T> {

    private RowToMessageFunction() {}

    @Override
    public T apply(Row input) {
      Message.Builder builder;
      try {
        if (DynamicMessage.class.equals(rawType)) {
          builder = (Message.Builder) fnNewBuilder.invoke(rawType, descriptor);
        } else {
          builder = (Message.Builder) fnNewBuilder.invoke(rawType);
        }
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException("Can't invoke newBuilder on the Protobuf message class.", e);
      }

      Iterator values = input.getValues().iterator();
      Iterator<ProtoFieldOverlay> overlayIterator = getters.iterator();

      for (int i = 0; i < input.getValues().size(); i++) {
        ProtoFieldOverlay getter = overlayIterator.next();
        Object value = values.next();
        getter.set(builder, value);
      }
      return (T) builder.build();
    }
  }
}
