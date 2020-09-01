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
package org.apache.beam.sdk.io.parquet;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/** A {@link PTransform} to convert {@link GenericRecord} to {@link Row}. */
@AutoValue
abstract class GenericRecordReadConverter
    extends PTransform<PCollection<GenericRecord>, PCollection<Row>> implements Serializable {

  public abstract Schema beamSchema();

  public static Builder builder() {
    return new AutoValue_GenericRecordReadConverter.Builder();
  }

  @Override
  public PCollection<Row> expand(PCollection<GenericRecord> input) {
    return input
        .apply(
            "GenericRecordsToRows",
            ParDo.of(
                new DoFn<GenericRecord, Row>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    Row row = AvroUtils.toBeamRowStrict(c.element(), beamSchema());
                    c.output(row);
                  }
                }))
        .setRowSchema(beamSchema());
  }

  @AutoValue.Builder
  abstract static class Builder {
    public abstract Builder beamSchema(Schema beamSchema);

    public abstract GenericRecordReadConverter build();
  }
}
