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
package org.apache.beam.sdk.io.fileschematransform;

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

@AutoService(FileReadSchemaTransformFormatProvider.class)
public class ParquetReadSchemaTransformFormatProvider
    implements FileReadSchemaTransformFormatProvider {
  @Override
  public String identifier() {
    return "parquet";
  }

  @Override
  public PTransform<PCollection<ReadableFile>, PCollection<Row>> buildTransform(
      FileReadSchemaTransformConfiguration configuration) {

    return new PTransform<PCollection<ReadableFile>, PCollection<Row>>() {
      @Override
      public PCollection<Row> expand(PCollection<ReadableFile> input) {
        org.apache.avro.Schema avroSchema =
            new org.apache.avro.Schema.Parser().parse(configuration.getSafeSchema());
        Schema beamSchema = AvroUtils.toBeamSchema(avroSchema);

        return input
            .apply(ParquetIO.readFiles(avroSchema).withBeamSchemas(true))
            .apply(
                MapElements.into(TypeDescriptors.rows())
                    .via(AvroUtils.getGenericRecordToRowFunction(beamSchema)))
            .setRowSchema(beamSchema);
      }
    };
  }
}
