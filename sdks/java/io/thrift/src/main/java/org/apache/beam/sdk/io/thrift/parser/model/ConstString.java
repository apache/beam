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
package org.apache.beam.sdk.io.thrift.parser.model;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.Objects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;

public class ConstString extends ConstValue {
  private final String value;

  public ConstString(String value) {
    this.value = checkNotNull(value, "value");
  }

  @Override
  public String value() {
    return value;
  }

  @Override
  public String getValueString() {
    return "\"" + this.value + "\"";
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("value", value).toString();
  }

  @Override
  public boolean equals(Object o) {

    if (o == this) {
      return true;
    }

    if (!(o instanceof ConstString)) {
      return false;
    }

    ConstString c = (ConstString) o;

    return Objects.equals(value, c.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
