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
package org.apache.beam.sdk.io;

import java.io.Serializable;
import org.apache.avro.Schema;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Suppliers;

/** Helpers for working with Avro. */
class AvroUtils {
  /** Helper to get around the fact that {@link Schema} itself is not serializable. */
  public static Supplier<Schema> serializableSchemaSupplier(String jsonSchema) {
    return Suppliers.memoize(
        Suppliers.compose(new JsonToSchema(), Suppliers.ofInstance(jsonSchema)));
  }

  private static class JsonToSchema implements Function<String, Schema>, Serializable {
    @Override
    public Schema apply(String input) {
      return new Schema.Parser().parse(input);
    }
  }
}
