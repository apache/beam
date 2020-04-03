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
package org.apache.beam.sdk.values;

import java.io.Serializable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.RowUtils.CapturingRowCases;
import org.apache.beam.sdk.values.RowUtils.FieldOverrides;
import org.apache.beam.sdk.values.RowUtils.RowFieldMatcher;
import org.apache.beam.sdk.values.RowUtils.RowPosition;

@Experimental
public abstract class SchemaVerification implements Serializable {
  public static Object verifyFieldValue(Object value, FieldType type, String fieldName) {
    Schema schema = Schema.builder().addField(fieldName, type).build();
    return new RowFieldMatcher()
        .match(
            new CapturingRowCases(schema, new FieldOverrides(schema)),
            type,
            new RowPosition(FieldAccessDescriptor.withFieldIds(0)),
            value);
  }
}
