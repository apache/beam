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

public class SnowflakeBinary implements SnowflakeDataType, Serializable {

  public static final Long MAX_SIZE = 8388608L;

  private Long size; // bytes

  public SnowflakeBinary() {}

  public static SnowflakeBinary of() {
    return new SnowflakeBinary();
  }

  public static SnowflakeBinary of(long size) {
    return new SnowflakeBinary(size);
  }

  public SnowflakeBinary(long size) {
    if (size > MAX_SIZE) {
      throw new IllegalArgumentException(
          String.format("Provided size %s is bigger than max size %s ", size, MAX_SIZE));
    }
    this.size = size;
  }

  @Override
  public String sql() {
    if (size != null) {
      return String.format("BINARY(%d)", size);
    }
    return "BINARY";
  }

  public Long getSize() {
    return size;
  }

  public void setSize(Long size) {
    this.size = size;
  }
}
