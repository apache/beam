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
package org.apache.beam.sdk.schemas.io.payloads;

import java.io.Serializable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;

@Internal
public interface PayloadSerializer extends Serializable {
  long serialVersionUID = 5645783967169L;

  byte[] serialize(Row row);

  Row deserialize(byte[] bytes);

  static PayloadSerializer of(
      SerializableFunction<Row, byte[]> serializeFn,
      SerializableFunction<byte[], Row> deserializeFn) {
    return new PayloadSerializer() {
      @Override
      public byte[] serialize(Row row) {
        return serializeFn.apply(row);
      }

      @Override
      public Row deserialize(byte[] bytes) {
        return deserializeFn.apply(bytes);
      }
    };
  }
}
