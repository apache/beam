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
package org.apache.beam.sdk.coders;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

/** AvroCoder specialisation for generated avro classes. */
public class AvroSpecificCoder<T> extends AvroCoder<T> {

  @SuppressWarnings("nullness") // new SpecificData(ClassLoader) is not annotated to accept null
  public AvroSpecificCoder(Class<T> type) {
    this(type, new SpecificData(type.getClassLoader()).getSchema(type));
  }

  public AvroSpecificCoder(Class<T> type, Schema schema) {
    super(type, schema);
  }

  @Override
  protected DatumReader<T> createReader() {
    // create the datum writer using the Class<T> api.
    // avro will load the proper class loader and when using avro 1.9
    // the proper data with conversions (SpecificData.getForClass)
    SpecificDatumReader<T> datumReader = new SpecificDatumReader<>(getType());
    Schema schema = getSchema();
    datumReader.setExpected(schema);
    datumReader.setSchema(schema);
    return datumReader;
  }

  @Override
  protected DatumWriter<T> createWriter() {
    // create the datum writer using the Class<T> api.
    // avro will load the proper class loader and when using avro 1.9
    // the proper data with conversions (SpecificData.getForClass)
    SpecificDatumWriter<T> datumWriter = new SpecificDatumWriter<>(getType());
    datumWriter.setSchema(getSchema());
    return datumWriter;
  }

  public static <T> AvroSpecificCoder<T> of(Class<T> type) {
    return new AvroSpecificCoder<>(type);
  }
}
