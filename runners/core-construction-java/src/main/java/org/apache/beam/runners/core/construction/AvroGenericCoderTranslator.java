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

import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.beam.runners.core.construction.CoderTranslation.TranslationContext;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;

/** Coder translator for AvroGenericCoder. */
public class AvroGenericCoderTranslator implements CoderTranslator<AvroGenericCoder> {
  @Override
  public List<? extends Coder<?>> getComponents(AvroGenericCoder from) {
    return Collections.emptyList();
  }

  @Override
  public byte[] getPayload(AvroGenericCoder from) {
    return from.getSchema().toString().getBytes(Charsets.UTF_8);
  }

  @Override
  public AvroGenericCoder fromComponents(
      List<Coder<?>> components, byte[] payload, TranslationContext context) {
    Schema schema = new Schema.Parser().parse(new String(payload, Charsets.UTF_8));
    return AvroGenericCoder.of(schema);
  }
}
