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

import static org.apache.beam.sdk.extensions.protobuf.ProtoByteBuddyUtils.getProtoGetter;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.protobuf.ProtoByteBuddyUtils.ProtoTypeConversionsFactory;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueTypeInformation;
import org.apache.beam.sdk.schemas.GetterBasedSchemaProviderV2;
import org.apache.beam.sdk.schemas.RowMessages;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.SchemaUserTypeCreator;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.sdk.schemas.utils.FieldValueTypeSupplier;
import org.apache.beam.sdk.schemas.utils.JavaBeanUtils;
import org.apache.beam.sdk.schemas.utils.ReflectUtils;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.checkerframework.checker.nullness.qual.Nullable;

@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class ProtoMessageSchema extends GetterBasedSchemaProviderV2 {

  private static final class ProtoClassFieldValueTypeSupplier implements FieldValueTypeSupplier {
    @Override
    public List<FieldValueTypeInformation> get(TypeDescriptor<?> typeDescriptor) {
      throw new RuntimeException("Unexpected call.");
    }

    @Override
    public List<FieldValueTypeInformation> get(TypeDescriptor<?> typeDescriptor, Schema schema) {
      Multimap<String, Method> methods = ReflectUtils.getMethodsMap(typeDescriptor.getRawType());
      List<FieldValueTypeInformation> types =
          Lists.newArrayListWithCapacity(schema.getFieldCount());
      for (int i = 0; i < schema.getFieldCount(); ++i) {
        Field field = schema.getField(i);
        if (field.getType().isLogicalType(OneOfType.IDENTIFIER)) {
          // This is a OneOf. Look for the getters for each OneOf option.
          OneOfType oneOfType = field.getType().getLogicalType(OneOfType.class);
          Map<String, FieldValueTypeInformation> oneOfTypes = Maps.newHashMap();
          for (Field oneOfField : oneOfType.getOneOfSchema().getFields()) {
            Method method = getProtoGetter(methods, oneOfField.getName(), oneOfField.getType());
            oneOfTypes.put(
                oneOfField.getName(),
                FieldValueTypeInformation.forGetter(method, i).withName(field.getName()));
          }
          // Add an entry that encapsulates information about all possible getters.
          types.add(
              FieldValueTypeInformation.forOneOf(
                      field.getName(), field.getType().getNullable(), oneOfTypes)
                  .withName(field.getName()));
        } else {
          // This is a simple field. Add the getter.
          Method method = getProtoGetter(methods, field.getName(), field.getType());
          types.add(FieldValueTypeInformation.forGetter(method, i).withName(field.getName()));
        }
      }
      return types;
    }
  }

  @Override
  public <T> @Nullable Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
    checkForDynamicType(typeDescriptor);
    return ProtoSchemaTranslator.getSchema((Class<Message>) typeDescriptor.getRawType());
  }

  @Override
  public List<FieldValueGetter> fieldValueGetters(
      TypeDescriptor<?> targetTypeDescriptor, Schema schema) {
    return ProtoByteBuddyUtils.getGetters(
        targetTypeDescriptor.getRawType(),
        schema,
        new ProtoClassFieldValueTypeSupplier(),
        new ProtoTypeConversionsFactory());
  }

  @Override
  public List<FieldValueTypeInformation> fieldValueTypeInformations(
      TypeDescriptor<?> targetTypeDescriptor, Schema schema) {
    return JavaBeanUtils.getFieldTypes(
        targetTypeDescriptor, schema, new ProtoClassFieldValueTypeSupplier());
  }

  @Override
  public SchemaUserTypeCreator schemaTypeCreator(
      TypeDescriptor<?> targetTypeDescriptor, Schema schema) {
    SchemaUserTypeCreator creator =
        ProtoByteBuddyUtils.getBuilderCreator(
            targetTypeDescriptor.getRawType(), schema, new ProtoClassFieldValueTypeSupplier());
    if (creator == null) {
      throw new RuntimeException("Cannot create creator for " + targetTypeDescriptor);
    }
    return creator;
  }

  // Other modules are not allowed to use non-vendored Message class
  @SuppressWarnings({
    "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
    "unchecked"
  })
  public static <T> SimpleFunction<byte[], Row> getProtoBytesToRowFn(Class<T> clazz) {
    Class<Message> protoClass = ensureMessageType(clazz);
    ProtoCoder<Message> protoCoder = ProtoCoder.of(protoClass);
    return RowMessages.bytesToRowFn(
        new ProtoMessageSchema(),
        TypeDescriptor.of(protoClass),
        bytes -> protoCoder.getParser().parseFrom(bytes));
  }

  // Other modules are not allowed to use non-vendored Message class
  @SuppressWarnings({
    "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
    "unchecked"
  })
  public static <T> SimpleFunction<Row, byte[]> getRowToProtoBytesFn(Class<T> clazz) {
    Class<Message> protoClass = ensureMessageType(clazz);
    return RowMessages.rowToBytesFn(
        new ProtoMessageSchema(), TypeDescriptor.of(protoClass), Message::toByteArray);
  }

  private <T> void checkForDynamicType(TypeDescriptor<T> typeDescriptor) {
    if (typeDescriptor.getRawType().equals(DynamicMessage.class)) {
      throw new RuntimeException(
          "DynamicMessage is not allowed for the standard ProtoSchemaProvider, use ProtoDynamicMessageSchema  instead.");
    }
  }

  private static Class<Message> ensureMessageType(Class<?> clazz) {
    checkArgument(
        Message.class.isAssignableFrom(clazz),
        "%s is not a subtype of %s",
        clazz.getName(),
        Message.class.getSimpleName());
    return (Class<Message>) clazz;
  }
}
