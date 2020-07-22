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

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.extensions.protobuf.ProtoByteBuddyUtils.ProtoTypeConversionsFactory;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueTypeInformation;
import org.apache.beam.sdk.schemas.GetterBasedSchemaProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.SchemaUserTypeCreator;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.sdk.schemas.utils.FieldValueTypeSupplier;
import org.apache.beam.sdk.schemas.utils.JavaBeanUtils;
import org.apache.beam.sdk.schemas.utils.ReflectUtils;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;
import org.checkerframework.checker.nullness.qual.Nullable;

@Experimental(Kind.SCHEMAS)
public class ProtoMessageSchema extends GetterBasedSchemaProvider {

  private static final class ProtoClassFieldValueTypeSupplier implements FieldValueTypeSupplier {
    @Override
    public List<FieldValueTypeInformation> get(Class<?> clazz) {
      throw new RuntimeException("Unexpected call.");
    }

    @Override
    public List<FieldValueTypeInformation> get(Class<?> clazz, Schema schema) {
      Multimap<String, Method> methods = ReflectUtils.getMethodsMap(clazz);
      List<FieldValueTypeInformation> types =
          Lists.newArrayListWithCapacity(schema.getFieldCount());
      for (Field field : schema.getFields()) {
        if (field.getType().isLogicalType(OneOfType.IDENTIFIER)) {
          // This is a OneOf. Look for the getters for each OneOf option.
          OneOfType oneOfType = field.getType().getLogicalType(OneOfType.class);
          Map<String, FieldValueTypeInformation> oneOfTypes = Maps.newHashMap();
          for (Field oneOfField : oneOfType.getOneOfSchema().getFields()) {
            Method method = getProtoGetter(methods, oneOfField.getName(), oneOfField.getType());
            oneOfTypes.put(
                oneOfField.getName(),
                FieldValueTypeInformation.forGetter(method).withName(field.getName()));
          }
          // Add an entry that encapsulates information about all possible getters.
          types.add(
              FieldValueTypeInformation.forOneOf(
                      field.getName(), field.getType().getNullable(), oneOfTypes)
                  .withName(field.getName()));
        } else {
          // This is a simple field. Add the getter.
          Method method = getProtoGetter(methods, field.getName(), field.getType());
          types.add(FieldValueTypeInformation.forGetter(method).withName(field.getName()));
        }
      }
      return types;
    }
  }

  @Nullable
  @Override
  public <T> Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
    checkForDynamicType(typeDescriptor);
    return ProtoSchemaTranslator.getSchema((Class<Message>) typeDescriptor.getRawType());
  }

  @Override
  public List<FieldValueGetter> fieldValueGetters(Class<?> targetClass, Schema schema) {
    return ProtoByteBuddyUtils.getGetters(
        targetClass,
        schema,
        new ProtoClassFieldValueTypeSupplier(),
        new ProtoTypeConversionsFactory());
  }

  @Override
  public List<FieldValueTypeInformation> fieldValueTypeInformations(
      Class<?> targetClass, Schema schema) {
    return JavaBeanUtils.getFieldTypes(targetClass, schema, new ProtoClassFieldValueTypeSupplier());
  }

  @Override
  public SchemaUserTypeCreator schemaTypeCreator(Class<?> targetClass, Schema schema) {
    SchemaUserTypeCreator creator =
        ProtoByteBuddyUtils.getBuilderCreator(
            targetClass, schema, new ProtoClassFieldValueTypeSupplier());
    if (creator == null) {
      throw new RuntimeException("Cannot create creator for " + targetClass);
    }
    return creator;
  }

  private <T> void checkForDynamicType(TypeDescriptor<T> typeDescriptor) {
    if (typeDescriptor.getRawType().equals(DynamicMessage.class)) {
      throw new RuntimeException(
          "DynamicMessage is not allowed for the standard ProtoSchemaProvider, use ProtoDynamicMessageSchema  instead.");
    }
  }
}
