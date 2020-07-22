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
package org.apache.beam.sdk.io;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/** A {@link PTransform} to convert {@link Row} to {@link GenericRecord}. */
@AutoValue
public abstract class GenericRecordWriteConverter
    extends PTransform<PCollection<Row>, PCollection<GenericRecord>> implements Serializable {

  public abstract Schema beamSchema();

  public static Builder builder() {
    return new AutoValue_GenericRecordWriteConverter.Builder();
  }

  @Override
  public PCollection<GenericRecord> expand(PCollection<Row> input) {
    return input
        .apply(
            "RowsToGenericRecord",
            ParDo.of(
                new DoFn<Row, GenericRecord>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    GenericRecord genericRecord =
                        AvroUtils.toGenericRecord(
                            c.element(), AvroUtils.toAvroSchema(beamSchema()));
                    c.output(genericRecord);
                  }
                }))
        .setCoder(AvroCoder.of(GenericRecord.class, AvroUtils.toAvroSchema(beamSchema())));
  }

  @AutoValue.Builder
  abstract static class Builder {
    public abstract Builder beamSchema(Schema beamSchema);

    public abstract GenericRecordWriteConverter build();
  }
}
