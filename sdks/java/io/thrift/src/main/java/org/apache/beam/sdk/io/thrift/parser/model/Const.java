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

public class Const extends Definition {
  private final String name;
  private final ThriftType type;
  private final ConstValue value;

  public Const(String name, ThriftType type, ConstValue value) {
    this.name = checkNotNull(name, "name");
    this.type = checkNotNull(type, "type");
    this.value = checkNotNull(value, "value");
  }

  @Override
  public String getName() {
    return name;
  }

  public ThriftType getType() {
    return type;
  }

  public ConstValue getValue() {
    return value;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("type", type)
        .add("value", value)
        .toString();
  }

  @Override
  public boolean equals(Object o) {

    if (o == this) {
      return true;
    }

    if (!(o instanceof Const)) {
      return false;
    }

    Const c = (Const) o;

    return Objects.equals(name, c.name)
        && Objects.equals(type, c.type)
        && Objects.equals(value, c.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, value);
  }
}
