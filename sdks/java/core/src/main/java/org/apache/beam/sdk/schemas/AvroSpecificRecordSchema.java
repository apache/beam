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
package org.apache.beam.sdk.schemas;

import org.apache.avro.specific.SpecificRecord;
import org.apache.beam.sdk.schemas.utils.AvroSpecificRecordTypeInformationFactory;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link SchemaProvider} for AVRO generated SpecificRecords.
 *
 * <p>This provider infers a schema from generates SpecificRecord objects, and creates schemas and
 * rows that bind to the appropriate fields.
 */
public class AvroSpecificRecordSchema extends GetterBasedSchemaProvider {
  @Override
  public <T> Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
    return AvroUtils.getSchema((Class<? extends SpecificRecord>) typeDescriptor.getRawType());
  }

  @Override
  public FieldValueGetterFactory fieldValueGetterFactory() {
    return new AvroSpecificRecordGetterFactory();
  }

  @Override
  public UserTypeCreatorFactory schemaTypeCreatorFactory() {
    return new AvroSpecificRecordUserTypeCreatorFactory();
  }

  @Override
  public FieldValueTypeInformationFactory fieldValueTypeInformationFactory() {
    return new AvroSpecificRecordTypeInformationFactory();
  }
}
