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
package org.apache.beam.sdk.io.jdbc;

import java.sql.JDBCType;
import java.time.Instant;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.PassThroughLogicalType;

/** Beam {@link org.apache.beam.sdk.schemas.Schema.LogicalType} implementations of JDBC types. */
@Experimental(Kind.SCHEMAS)
class LogicalTypes {

  static final Schema.FieldType JDBC_DATE_TYPE =
      Schema.FieldType.logicalType(
          new PassThroughLogicalType<Instant>(
              JDBCType.DATE.getName(), FieldType.STRING, "", Schema.FieldType.DATETIME) {});

  static final Schema.FieldType JDBC_TIME_TYPE =
      Schema.FieldType.logicalType(
          new PassThroughLogicalType<Instant>(
              JDBCType.TIME.getName(), FieldType.STRING, "", Schema.FieldType.DATETIME) {});

  static final Schema.FieldType JDBC_TIMESTAMP_WITH_TIMEZONE_TYPE =
      Schema.FieldType.logicalType(
          new PassThroughLogicalType<Instant>(
              JDBCType.TIMESTAMP_WITH_TIMEZONE.getName(),
              FieldType.STRING,
              "",
              Schema.FieldType.DATETIME) {});
}
