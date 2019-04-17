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
package org.apache.beam.runners.core.construction;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.coders.Coder;

/** Coder translator for AvroGenericCoder. */
public class AvroGenericCoderTranslator implements CoderTranslator<AvroGenericCoder> {
  @Override
  public List<? extends Coder<?>> getComponents(AvroGenericCoder from) {
    return Collections.emptyList();
  }

  @Override
  public byte[] getPayload(AvroGenericCoder from) {
    try {
      return from.getSchema().toString().getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("failed to encode schema.");
    }
  }

  @Override
  public AvroGenericCoder fromComponents(List<Coder<?>> components, byte[] payload) {
    Schema schema;
    try {
      schema = new Schema.Parser().parse(new String(payload, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("failed to parse schema.");
    }
    return AvroGenericCoder.of(schema);
  }
}
