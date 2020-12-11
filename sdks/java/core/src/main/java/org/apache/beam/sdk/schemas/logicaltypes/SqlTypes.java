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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.values.Row;

/** Beam {@link org.apache.beam.sdk.schemas.Schema.LogicalType}s corresponding to SQL data types. */
public class SqlTypes {

  private SqlTypes() {}

  /** Beam LogicalType corresponding to ZetaSQL/CalciteSQL DATE type. */
  public static final LogicalType<LocalDate, Long> DATE = new Date();

  /** Beam LogicalType corresponding to ZetaSQL/CalciteSQL TIME type. */
  public static final LogicalType<LocalTime, Long> TIME = new Time();

  /** Beam LogicalType corresponding to ZetaSQL DATETIME type. */
  public static final LogicalType<LocalDateTime, Row> DATETIME = new DateTime();

  /** Beam LogicalType corresponding to ZetaSQL TIMESTAMP type. */
  public static final LogicalType<Instant, Row> TIMESTAMP = new MicrosInstant();
}
