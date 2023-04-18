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
package org.apache.beam.sdk.extensions.avro.coders;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.beam.sdk.extensions.avro.io.AvroDatumFactory;

/** AvroCoder specialisation for generated avro classes. */
public class AvroSpecificCoder<T> extends AvroCoder<T> {

  @SuppressWarnings("nullness") // new SpecificData(ClassLoader) is not annotated to accept null
  AvroSpecificCoder(Class<T> type) {
    super(
        type,
        AvroDatumFactory.SpecificDatumFactory.of(type),
        new SpecificData(type.getClassLoader()).getSchema(type));
  }

  AvroSpecificCoder(Class<T> type, Schema schema) {
    super(type, AvroDatumFactory.SpecificDatumFactory.of(type), schema);
  }

  public static <T> AvroSpecificCoder<T> of(Class<T> type) {
    return new AvroSpecificCoder<>(type);
  }

  public static <T> AvroSpecificCoder<T> of(Class<T> type, Schema schema) {
    return new AvroSpecificCoder<>(type, schema);
  }
}
