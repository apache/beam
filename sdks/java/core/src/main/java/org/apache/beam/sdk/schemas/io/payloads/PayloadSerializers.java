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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.Providers;

@Internal
public final class PayloadSerializers {
  private PayloadSerializers() {}

  private static final Map<String, PayloadSerializerProvider> PROVIDERS =
      Providers.loadProviders(PayloadSerializerProvider.class);

  @SuppressWarnings("dereference.of.nullable")
  public static PayloadSerializer getSerializer(
      String id, Schema schema, Map<String, Object> tableParams) {
    PayloadSerializerProvider provider = PROVIDERS.get(id);
    checkArgument(
        provider != null,
        "Invalid config, no serializer provider exists with identifier `%s`.",
        id);
    return provider.getSerializer(schema, tableParams);
  }
}
