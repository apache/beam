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
package org.apache.beam.sdk.extensions.avro.schemas.io.payloads;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializerProvider;

@Internal
@AutoService(PayloadSerializerProvider.class)
public class AvroPayloadSerializerProvider implements PayloadSerializerProvider {
  @Override
  public String identifier() {
    return "avro";
  }

  @Override
  public PayloadSerializer getSerializer(Schema schema, Map<String, Object> tableParams) {
    return PayloadSerializer.of(
        AvroUtils.getRowToAvroBytesFunction(schema), AvroUtils.getAvroBytesToRowFunction(schema));
  }
}
