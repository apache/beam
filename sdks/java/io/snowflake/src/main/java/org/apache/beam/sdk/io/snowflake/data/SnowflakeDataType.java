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
package org.apache.beam.sdk.io.snowflake.data;

import java.io.Serializable;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.annotation.JsonSubTypes;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.annotation.JsonSubTypes.Type;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeDate;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeDateTime;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTime;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTimestamp;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTimestampLTZ;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTimestampNTZ;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTimestampTZ;
import org.apache.beam.sdk.io.snowflake.data.logical.SnowflakeBoolean;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeDecimal;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeDouble;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeFloat;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeInteger;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeNumber;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeNumeric;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeReal;
import org.apache.beam.sdk.io.snowflake.data.structured.SnowflakeArray;
import org.apache.beam.sdk.io.snowflake.data.structured.SnowflakeObject;
import org.apache.beam.sdk.io.snowflake.data.structured.SnowflakeVariant;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeBinary;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeChar;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeString;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeText;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeVarBinary;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeVarchar;

/** Interface for data types to provide SQLs for themselves. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
  @Type(value = SnowflakeDate.class, name = "date"),
  @Type(value = SnowflakeDateTime.class, name = "datetime"),
  @Type(value = SnowflakeTime.class, name = "time"),
  @Type(value = SnowflakeTimestamp.class, name = "timestamp"),
  @Type(value = SnowflakeTimestampLTZ.class, name = "timestamp_ltz"),
  @Type(value = SnowflakeTimestampNTZ.class, name = "timestamp_ntz"),
  @Type(value = SnowflakeTimestampTZ.class, name = "timestamp_tz"),
  @Type(value = SnowflakeBoolean.class, name = "boolean"),
  @Type(value = SnowflakeDecimal.class, name = "decimal"),
  @Type(value = SnowflakeDouble.class, name = "double"),
  @Type(value = SnowflakeFloat.class, name = "float"),
  @Type(value = SnowflakeInteger.class, name = "integer"),
  @Type(value = SnowflakeNumber.class, name = "number"),
  @Type(value = SnowflakeNumeric.class, name = "numeric"),
  @Type(value = SnowflakeReal.class, name = "real"),
  @Type(value = SnowflakeArray.class, name = "array"),
  @Type(value = SnowflakeObject.class, name = "object"),
  @Type(value = SnowflakeVariant.class, name = "variant"),
  @Type(value = SnowflakeBinary.class, name = "binary"),
  @Type(value = SnowflakeChar.class, name = "char"),
  @Type(value = SnowflakeString.class, name = "string"),
  @Type(value = SnowflakeText.class, name = "text"),
  @Type(value = SnowflakeVarBinary.class, name = "varbinary"),
  @Type(value = SnowflakeVarchar.class, name = "varchar"),
})
public interface SnowflakeDataType extends Serializable {
  String sql();
}
