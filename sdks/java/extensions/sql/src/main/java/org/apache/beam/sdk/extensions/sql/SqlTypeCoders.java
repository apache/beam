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

package org.apache.beam.sdk.extensions.sql;

import static org.apache.beam.sdk.extensions.sql.SqlTypeCoder.SqlBigIntCoder;
import static org.apache.beam.sdk.extensions.sql.SqlTypeCoder.SqlBooleanCoder;
import static org.apache.beam.sdk.extensions.sql.SqlTypeCoder.SqlCharCoder;
import static org.apache.beam.sdk.extensions.sql.SqlTypeCoder.SqlDateCoder;
import static org.apache.beam.sdk.extensions.sql.SqlTypeCoder.SqlDecimalCoder;
import static org.apache.beam.sdk.extensions.sql.SqlTypeCoder.SqlDoubleCoder;
import static org.apache.beam.sdk.extensions.sql.SqlTypeCoder.SqlFloatCoder;
import static org.apache.beam.sdk.extensions.sql.SqlTypeCoder.SqlSmallIntCoder;
import static org.apache.beam.sdk.extensions.sql.SqlTypeCoder.SqlTimeCoder;
import static org.apache.beam.sdk.extensions.sql.SqlTypeCoder.SqlTimestampCoder;
import static org.apache.beam.sdk.extensions.sql.SqlTypeCoder.SqlTinyIntCoder;
import static org.apache.beam.sdk.extensions.sql.SqlTypeCoder.SqlVarCharCoder;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.SqlTypeCoder.SqlArrayCoder;
import org.apache.beam.sdk.extensions.sql.SqlTypeCoder.SqlIntegerCoder;
import org.apache.beam.sdk.extensions.sql.SqlTypeCoder.SqlRowCoder;
import org.apache.beam.sdk.values.Schema;

/**
 * Coders for SQL types supported in Beam.
 *
 * <p>Currently SQL coders are subclasses of {@link SqlTypeCoder}.
 */
public class SqlTypeCoders {
  public static final SqlTypeCoder TINYINT = new SqlTinyIntCoder();
  public static final SqlTypeCoder SMALLINT = new SqlSmallIntCoder();
  public static final SqlTypeCoder INTEGER = new SqlIntegerCoder();
  public static final SqlTypeCoder BIGINT = new SqlBigIntCoder();
  public static final SqlTypeCoder FLOAT = new SqlFloatCoder();
  public static final SqlTypeCoder DOUBLE = new SqlDoubleCoder();
  public static final SqlTypeCoder DECIMAL = new SqlDecimalCoder();
  public static final SqlTypeCoder BOOLEAN = new SqlBooleanCoder();
  public static final SqlTypeCoder CHAR = new SqlCharCoder();
  public static final SqlTypeCoder VARCHAR = new SqlVarCharCoder();
  public static final SqlTypeCoder TIME = new SqlTimeCoder();
  public static final SqlTypeCoder DATE = new SqlDateCoder();
  public static final SqlTypeCoder TIMESTAMP = new SqlTimestampCoder();

  public static final Set<SqlTypeCoder> NUMERIC_TYPES =
      ImmutableSet.of(
          SqlTypeCoders.TINYINT,
          SqlTypeCoders.SMALLINT,
          SqlTypeCoders.INTEGER,
          SqlTypeCoders.BIGINT,
          SqlTypeCoders.FLOAT,
          SqlTypeCoders.DOUBLE,
          SqlTypeCoders.DECIMAL);

  public static SqlTypeCoder arrayOf(SqlTypeCoder elementCoder) {
    return SqlArrayCoder.of(elementCoder);
  }

  public static SqlTypeCoder arrayOf(RowType rowType) {
    return SqlArrayCoder.of(rowOf(rowType));
  }

  public static boolean isArray(SqlTypeCoder sqlTypeCoder) {
    return sqlTypeCoder instanceof SqlArrayCoder;
  }

  public static boolean isRow(SqlTypeCoder sqlTypeCoder) {
    return sqlTypeCoder instanceof SqlRowCoder;
  }

  public static SqlTypeCoder rowOf(Schema schema) {
    return SqlRowCoder.of(schema);
  }
}
