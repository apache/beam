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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

public class ConstList extends ConstValue {
  private final List<ConstValue> values;

  public ConstList(List<ConstValue> values) {
    this.values = ImmutableList.copyOf(checkNotNull(values, "values"));
  }

  @Override
  public List<ConstValue> value() {
    return values;
  }

  @Override
  public String getValueString() {
    StringBuilder stringBuilder = new StringBuilder("[");
    List<String> valueStrings = new ArrayList<>();
    for (ConstValue value : values) {
      valueStrings.add(value.getValueString());
    }
    stringBuilder.append(String.join(",", valueStrings)).append("]");
    return stringBuilder.toString();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("values", values).toString();
  }
}
