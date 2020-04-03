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
package org.apache.beam.sdk.io.gcp.bigquery;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.MimeTypes;

class AvroRowWriter<T> extends BigQueryRowWriter<T> {
  private final DataFileWriter<GenericRecord> writer;
  private final Schema schema;
  private final SerializableFunction<AvroWriteRequest<T>, GenericRecord> toAvroRecord;

  AvroRowWriter(
      String basename,
      Schema schema,
      SerializableFunction<AvroWriteRequest<T>, GenericRecord> toAvroRecord)
      throws Exception {
    super(basename, MimeTypes.BINARY);

    this.schema = schema;
    this.toAvroRecord = toAvroRecord;
    this.writer =
        new DataFileWriter<GenericRecord>(new GenericDatumWriter<>())
            .create(schema, getOutputStream());
  }

  @Override
  public void write(T element) throws IOException {
    AvroWriteRequest<T> writeRequest = new AvroWriteRequest<>(element, schema);
    writer.append(toAvroRecord.apply(writeRequest));
  }

  public Schema getSchema() {
    return this.schema;
  }

  @Override
  public void close() throws IOException {
    writer.close();
    super.close();
  }
}
