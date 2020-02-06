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
package org.apache.beam.sdk.schemas.logicaltypes;

import java.time.Instant;
import org.apache.beam.sdk.values.Row;

/** A timestamp represented as nanoseconds since the epoch. */
public class NanosInstant extends NanosType<Instant> {
  public static final String IDENTIFIER = "beam:logical_type:nanos_instant:v1";

  public NanosInstant() {
    super(IDENTIFIER);
  }

  @Override
  public Row toBaseType(Instant input) {
    return Row.withSchema(schema).addValues(input.getEpochSecond(), input.getNano()).build();
  }

  @Override
  public Instant toInputType(Row row) {
    return Instant.ofEpochSecond(row.getInt64(0), row.getInt32(1));
  }
}
