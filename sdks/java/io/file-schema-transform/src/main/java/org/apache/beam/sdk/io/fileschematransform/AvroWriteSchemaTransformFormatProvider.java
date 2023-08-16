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

import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.getNumShards;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.getShardNameTemplate;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformProvider.ERROR_SCHEMA;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformProvider.ERROR_TAG;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformProvider.RESULT_TAG;

import com.google.auto.service.AutoService;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.BeamRowMapperWithDlq;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;

/** A {@link FileWriteSchemaTransformFormatProvider} for avro format. */
@AutoService(FileWriteSchemaTransformFormatProvider.class)
public class AvroWriteSchemaTransformFormatProvider
    implements FileWriteSchemaTransformFormatProvider {
  static final TupleTag<GenericRecord> ERROR_FN_OUPUT_TAG = new TupleTag<GenericRecord>() {};

  @Override
  public String identifier() {
    return FileWriteSchemaTransformFormatProviders.AVRO;
  }

  /**
   * Builds a {@link PTransform} that transforms a {@link Row} {@link PCollection} into result
   * {@link PCollectionTuple} with two tags, one for file names written using {@link AvroIO.Write},
   * another for errored-out rows.
   */
  @Override
  public PTransform<PCollection<Row>, PCollectionTuple> buildTransform(
      FileWriteSchemaTransformConfiguration configuration, Schema schema) {

    return new PTransform<PCollection<Row>, PCollectionTuple>() {
      @Override
      public PCollectionTuple expand(PCollection<Row> input) {

        org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(schema);
        AvroCoder<GenericRecord> coder = AvroCoder.of(avroSchema);

        PCollectionTuple tuple =
            input.apply(
                "Row To Avro Generic Record",
                ParDo.of(
                        new BeamRowMapperWithDlq<GenericRecord>(
                            "Avro-write-error-counter",
                            AvroUtils.getRowToGenericRecordFunction(AvroUtils.toAvroSchema(schema)),
                            ERROR_FN_OUPUT_TAG))
                    .withOutputTags(ERROR_FN_OUPUT_TAG, TupleTagList.of(ERROR_TAG)));

        PCollection<GenericRecord> avro = tuple.get(ERROR_FN_OUPUT_TAG).setCoder(coder);

        AvroIO.Write<GenericRecord> write =
            AvroIO.writeGenericRecords(avroSchema).to(configuration.getFilenamePrefix());

        if (configuration.getNumShards() != null) {
          int numShards = getNumShards(configuration);
          // Python SDK external transforms do not support null values requiring additional check.
          if (numShards > 0) {
            write = write.withNumShards(numShards);
          }
        }

        if (!Strings.isNullOrEmpty(configuration.getShardNameTemplate())) {
          write = write.withShardNameTemplate(getShardNameTemplate(configuration));
        }

        PCollection<String> output =
            avro.apply("Write Avro", write.withOutputFilenames())
                .getPerDestinationOutputFilenames()
                .apply("perDestinationOutputFilenames", Values.create());

        return PCollectionTuple.of(RESULT_TAG, output)
            .and(ERROR_TAG, tuple.get(ERROR_TAG).setRowSchema(ERROR_SCHEMA));
      }
    };
  }
}
