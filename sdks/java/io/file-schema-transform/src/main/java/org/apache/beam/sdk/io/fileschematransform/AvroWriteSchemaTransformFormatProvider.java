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
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/** A {@link FileWriteSchemaTransformFormatProvider} for avro format. */
@AutoService(FileWriteSchemaTransformFormatProvider.class)
public class AvroWriteSchemaTransformFormatProvider
    implements FileWriteSchemaTransformFormatProvider {

  @Override
  public String identifier() {
    return FileWriteSchemaTransformFormatProviders.AVRO;
  }

  @Override
  public PTransform<PCollection<Row>, PCollection<String>> buildTransform(
      FileWriteSchemaTransformConfiguration configuration, Schema schema) {

    return new PTransform<PCollection<Row>, PCollection<String>>() {
      @Override
      public PCollection<String> expand(PCollection<Row> input) {

        org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(schema);
        AvroGenericCoder coder = AvroGenericCoder.of(avroSchema);

        PCollection<GenericRecord> avro =
            input
                .apply(
                    "Row To Avro Generic Record",
                    FileWriteSchemaTransformFormatProviders.mapRowsToGenericRecords(schema))
                .setCoder(coder);

        AvroIO.Write<GenericRecord> write =
            AvroIO.writeGenericRecords(avroSchema).to(configuration.getFilenamePrefix());

        if (configuration.getNumShards() != null) {
          write =
              write.withNumShards(
                  FileWriteSchemaTransformFormatProviders.getNumShards(configuration));
        }

        if (configuration.getShardNameTemplate() != null) {
          write =
              write.withShardNameTemplate(
                  FileWriteSchemaTransformFormatProviders.getShardNameTemplate(configuration));
        }

        return avro.apply("Write Avro", write.withOutputFilenames())
            .getPerDestinationOutputFilenames()
            .apply("perDestinationOutputFilenames", Values.create());
      }
    };
  }
}
