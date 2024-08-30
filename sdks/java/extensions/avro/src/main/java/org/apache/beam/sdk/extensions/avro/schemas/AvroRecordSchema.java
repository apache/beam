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
package org.apache.beam.sdk.extensions.avro.schemas;

import java.util.List;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueTypeInformation;
import org.apache.beam.sdk.schemas.GetterBasedSchemaProviderV2;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaProvider;
import org.apache.beam.sdk.schemas.SchemaUserTypeCreator;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link SchemaProvider} for AVRO generated SpecificRecords and POJOs.
 *
 * <p>This provider infers a schema from generated SpecificRecord objects, and creates schemas and
 * rows that bind to the appropriate fields. This provider also infers schemas from Java POJO
 * objects, creating a schema that matches that inferred by the AVRO libraries.
 */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class AvroRecordSchema extends GetterBasedSchemaProviderV2 {
  @Override
  public <T> Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
    return AvroUtils.toBeamSchema(typeDescriptor.getRawType());
  }

  @Override
  public List<FieldValueGetter> fieldValueGetters(
      TypeDescriptor<?> targetTypeDescriptor, Schema schema) {
    return AvroUtils.getGetters(targetTypeDescriptor, schema);
  }

  @Override
  public List<FieldValueTypeInformation> fieldValueTypeInformations(
      TypeDescriptor<?> targetTypeDescriptor, Schema schema) {
    return AvroUtils.getFieldTypes(targetTypeDescriptor, schema);
  }

  @Override
  public SchemaUserTypeCreator schemaTypeCreator(
      TypeDescriptor<?> targetTypeDescriptor, Schema schema) {
    return AvroUtils.getCreator(targetTypeDescriptor, schema);
  }
}
