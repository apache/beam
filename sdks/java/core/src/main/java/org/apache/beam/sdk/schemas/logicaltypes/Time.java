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

import java.time.LocalTime;
import org.apache.beam.sdk.schemas.Schema;

/**
 * A time without a time-zone.
 *
 * <p>It cannot represent an instant on the time-line without additional information such as an
 * offset or time-zone.
 *
 * <p>Its input type is a {@link LocalTime}, and base type is a {@link Long} that represents a count
 * of time in nanoseconds.
 */
public class Time implements Schema.LogicalType<LocalTime, Long> {

  @Override
  public String getIdentifier() {
    return "beam:logical_type:time:v1";
  }

  // unused
  @Override
  public Schema.FieldType getArgumentType() {
    return Schema.FieldType.STRING;
  }

  // unused
  @Override
  public String getArgument() {
    return "";
  }

  @Override
  public Schema.FieldType getBaseType() {
    return Schema.FieldType.INT64;
  }

  @Override
  public Long toBaseType(LocalTime input) {
    return input == null ? null : input.toNanoOfDay();
  }

  @Override
  public LocalTime toInputType(Long base) {
    return base == null ? null : LocalTime.ofNanoOfDay(base);
  }
}
