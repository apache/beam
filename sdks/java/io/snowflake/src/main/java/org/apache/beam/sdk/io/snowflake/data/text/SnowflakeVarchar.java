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
package org.apache.beam.sdk.io.snowflake.data.text;

import java.io.Serializable;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeDataType;

public class SnowflakeVarchar implements SnowflakeDataType, Serializable {
  public static final Long MAX_LENGTH = 16777216L;
  private Long length;

  public static SnowflakeVarchar of() {
    return new SnowflakeVarchar();
  }

  public static SnowflakeVarchar of(long length) {
    return new SnowflakeVarchar(length);
  }

  public SnowflakeVarchar() {}

  public SnowflakeVarchar(long length) {
    if (length > MAX_LENGTH) {
      throw new IllegalArgumentException(
          String.format("Provided length %s is bigger than max length %s ", length, MAX_LENGTH));
    }
    this.length = length;
  }

  @Override
  public String sql() {
    if (length != null) {
      return String.format("VARCHAR(%d)", length);
    }
    return "VARCHAR";
  }

  public Long getLength() {
    return length;
  }

  public void setLength(Long length) {
    this.length = length;
  }
}
