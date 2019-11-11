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

import java.util.List;
import java.util.Objects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;

public class MapType extends ContainerType {
  private final ThriftType keyType;
  private final ThriftType valueType;

  public MapType(
      ThriftType keyType, ThriftType valueType, String cppType, List<TypeAnnotation> annotations) {
    super(cppType, annotations);
    this.keyType = checkNotNull(keyType, "keyType");
    this.valueType = checkNotNull(valueType, "valueType");
  }

  public ThriftType getKeyType() {
    return keyType;
  }

  public ThriftType getValueType() {
    return valueType;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("keyType", keyType)
        .add("valueType", valueType)
        .add("cppType", cppType)
        .add("annotations", annotations)
        .toString();
  }

  @Override
  public boolean equals(Object o) {

    if (o == this) {
      return true;
    }

    if (!(o instanceof MapType)) {
      return false;
    }

    MapType c = (MapType) o;

    return Objects.equals(keyType, c.keyType)
        && Objects.equals(valueType, c.valueType)
        && Objects.equals(cppType, c.cppType)
        && Objects.equals(annotations, c.annotations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyType, valueType, cppType, annotations);
  }
}
