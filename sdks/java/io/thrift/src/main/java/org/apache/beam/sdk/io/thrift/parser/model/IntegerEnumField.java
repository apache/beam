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

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;

public class IntegerEnumField implements Serializable {
  private final String name;
  private final Optional<Long> explicitValue;
  private final long effectiveValue;
  private final List<TypeAnnotation> annotations;

  public IntegerEnumField(
      String name, Long explicitValue, Long defaultValue, List<TypeAnnotation> annotations) {
    this.name = checkNotNull(name, "name");
    this.explicitValue = Optional.fromNullable(explicitValue);
    this.effectiveValue = this.explicitValue.isPresent() ? this.explicitValue.get() : defaultValue;
    this.annotations = annotations;
  }

  public String getName() {
    return name;
  }

  public Optional<Long> getExplicitValue() {
    return explicitValue;
  }

  public List<TypeAnnotation> getAnnotations() {
    return annotations;
  }

  public long getValue() {
    return effectiveValue;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("value", effectiveValue)
        .add("explicitValue", explicitValue)
        .toString();
  }

  @Override
  public boolean equals(Object o) {

    if (o == this) {
      return true;
    }

    if (!(o instanceof IntegerEnumField)) {
      return false;
    }

    IntegerEnumField c = (IntegerEnumField) o;

    return Objects.equals(name, c.name)
        && Objects.equals(explicitValue, c.explicitValue)
        && Objects.equals(effectiveValue, c.effectiveValue)
        && Objects.equals(annotations, c.annotations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, explicitValue, effectiveValue, annotations);
  }
}
