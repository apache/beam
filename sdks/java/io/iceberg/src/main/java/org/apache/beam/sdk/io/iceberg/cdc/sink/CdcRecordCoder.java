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
package org.apache.beam.sdk.io.iceberg.cdc.sink;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.ValueKindCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueKind;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link CdcRecord} carries a {@link Row} field whose schema is known only at pipeline-construction
 * time. We need a custom coder because {@code AutoValueSchema} only infers schemas at the class
 * level and cannot infer a dynamic {@link Row} field, so {@code @DefaultSchema} alone cannot
 * produce a working coder for {@link CdcRecord}.
 */
final class CdcRecordCoder extends CustomCoder<CdcRecord> {

  private final RowCoder dataCoder;
  private final ValueKindCoder kindCoder = ValueKindCoder.of();
  private final VarLongCoder seqCoder = VarLongCoder.of();

  private CdcRecordCoder(Schema dataSchema) {
    this.dataCoder = RowCoder.of(dataSchema);
  }

  public static CdcRecordCoder of(Schema dataSchema) {
    return new CdcRecordCoder(dataSchema);
  }

  public Schema getDataSchema() {
    return dataCoder.getSchema();
  }

  @Override
  public void encode(CdcRecord value, OutputStream outStream) throws IOException {
    dataCoder.encode(value.getData(), outStream);
    kindCoder.encode(value.getKind(), outStream);
    seqCoder.encode(value.getSequenceNumber(), outStream);
  }

  @Override
  public CdcRecord decode(InputStream inStream) throws IOException {
    Row data = dataCoder.decode(inStream);
    ValueKind kind = kindCoder.decode(inStream);
    long seq = seqCoder.decode(inStream);
    return CdcRecord.of(data, kind, seq);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    Coder.verifyDeterministic(this, "Data coder must be deterministic", dataCoder);
  }

  @Override
  public boolean consistentWithEquals() {
    // decode() rebuilds the row with this coder's schema object, which may differ from the
    // original row's; false is always safe.
    return false;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return getDataSchema().equals(((CdcRecordCoder) o).getDataSchema());
  }

  @Override
  public int hashCode() {
    return getDataSchema().hashCode();
  }
}
