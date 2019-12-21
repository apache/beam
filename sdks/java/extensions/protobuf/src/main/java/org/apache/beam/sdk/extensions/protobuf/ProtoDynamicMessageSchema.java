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

import com.google.protobuf.DynamicMessage;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueTypeInformation;
import org.apache.beam.sdk.schemas.GetterBasedSchemaProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaUserTypeCreator;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;

@Experimental(Kind.SCHEMAS)
public class ProtoDynamicMessageSchema extends GetterBasedSchemaProvider {
  private static final TypeDescriptor<DynamicMessage> DYNAMIC_MESSAGE_TYPE_DESCRIPTOR =
      TypeDescriptor.of(DynamicMessage.class);

  @Nullable
  @Override
  public <T> Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
    checkForDynamicType(typeDescriptor);
    return ProtoSchemaTranslator.getSchema((Class<DynamicMessage>) typeDescriptor.getRawType());
  }

  @Override
  public List<FieldValueGetter> fieldValueGetters(Class<?> targetClass, Schema schema) {
    return null;
  }

  @Override
  public List<FieldValueTypeInformation> fieldValueTypeInformations(
      Class<?> targetClass, Schema schema) {
    List<FieldValueTypeInformation> types = Lists.newArrayListWithCapacity(schema.getFieldCount());
    return null;
  }

  @Override
  public SchemaUserTypeCreator schemaTypeCreator(Class<?> targetClass, Schema schema) {
    return null;
  }

  private <T> void checkForDynamicType(TypeDescriptor<T> typeDescriptor) {
    if (!typeDescriptor.isSubtypeOf(DYNAMIC_MESSAGE_TYPE_DESCRIPTOR)) {
      throw new IllegalArgumentException("ProtoDynamicMessageSchema only handles DynamicMessages.");
    }
  }
}
