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
package org.apache.beam.sdk.io.hcatalog;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.joda.time.Instant;

/** Utilities to convert {@link HCatRecord HCatRecords} to {@link Row Rows}. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class HCatToRow {

  /**
   * Creates a {@link PTransform} that converts incoming {@link HCatRecord HCatRecords} to {@link
   * Row Rows} using specified schema.
   *
   * <p>If there is a mismatch between the schema specified here and actual record schema, or
   * internal representation and schema, then runtime errors will happen.
   */
  private static PTransform<PCollection<? extends HCatRecord>, PCollection<Row>> forSchema(
      Schema schema) {
    return ParDo.of(new HCatToRowFn(schema));
  }

  /**
   * Wraps the {@link HCatalogIO#read()} to convert {@link HCatRecord HCatRecords} to {@link Row
   * Rows}.
   *
   * <p>Eventually this should become part of the IO, e.g. {@code HCatalogIO.readRows()}.
   */
  public static PTransform<PBegin, PCollection<Row>> fromSpec(HCatalogIO.Read readSpec) {
    return new PTransform<PBegin, PCollection<Row>>() {
      @Override
      public PCollection<Row> expand(PBegin input) {
        HCatalogBeamSchema hcatSchema = HCatalogBeamSchema.create(readSpec.getConfigProperties());
        Schema schema =
            hcatSchema.getTableSchema(readSpec.getDatabase(), readSpec.getTable()).get();
        return input
            .apply("ReadHCatRecords", readSpec)
            .apply("ConvertToRows", forSchema(schema))
            .setRowSchema(schema);
      }
    };
  }

  /**
   * {@link DoFn} to convert {@link HCatRecord HCatRecords} to {@link Row Rows}.
   *
   * <p>Gets all values from the records, uses them to create a row. Values/schema are validated in
   * the row builder.
   */
  private static class HCatToRowFn extends DoFn<HCatRecord, Row> {
    private final Schema schema;

    private Object maybeCastHDate(Object obj) {
      if (obj instanceof org.apache.hadoop.hive.common.type.Date) {
        return new Instant(((org.apache.hadoop.hive.common.type.Date) obj).toEpochMilli());
      }
      return obj;
    }

    /** Cast objects of the types that aren't supported by {@link Row}. */
    private List<Object> castTypes(List<Object> values) {
      return values.stream().map(this::maybeCastHDate).collect(Collectors.toList());
    }

    HCatToRowFn(Schema schema) {
      this.schema = schema;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      HCatRecord hCatRecord = c.element();
      c.output(Row.withSchema(schema).addValues(castTypes(hCatRecord.getAll())).build());
    }
  }
}
