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
package org.apache.beam.sdk.extensions.avro.schemas.utils;

import com.google.auto.service.AutoService;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.utils.ConvertHelpers;
import org.apache.beam.sdk.schemas.utils.SchemaInformationProvider;
import org.apache.beam.sdk.values.TypeDescriptor;

@AutoService(SchemaInformationProvider.class)
public class AvroSchemaInformationProvider implements SchemaInformationProvider {

  @Override
  @Nullable
  public <T> ConvertHelpers.ConvertedSchemaInformation<T> getConvertedSchemaInformation(
      Schema inputSchema, TypeDescriptor<T> outputType) {
    if (outputType.equals(TypeDescriptor.of(GenericRecord.class))) {
      return new ConvertHelpers.ConvertedSchemaInformation<T>(
          (SchemaCoder<T>) AvroUtils.schemaCoder(AvroUtils.toAvroSchema(inputSchema)), null);
    }
    return null;
  }
}
