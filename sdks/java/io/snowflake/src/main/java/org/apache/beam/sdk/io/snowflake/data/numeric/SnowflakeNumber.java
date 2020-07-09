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
package org.apache.beam.sdk.io.snowflake.data.numeric;

import java.io.Serializable;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeDataType;

public class SnowflakeNumber implements SnowflakeDataType, Serializable {
  private int precision = 38;
  private int scale = 0;

  public static SnowflakeNumber of() {
    return new SnowflakeNumber();
  }

  public static SnowflakeNumber of(int precision, int scale) {
    return new SnowflakeNumber(precision, scale);
  }

  public SnowflakeNumber() {}

  public SnowflakeNumber(int precision, int scale) {
    this.precision = precision;
    this.scale = scale;
  }

  @Override
  public String sql() {
    return String.format("NUMBER(%d,%d)", precision, scale);
  }

  public int getPrecision() {
    return precision;
  }

  public void setPrecision(int precision) {
    this.precision = precision;
  }

  public int getScale() {
    return scale;
  }

  public void setScale(int scale) {
    this.scale = scale;
  }
}
