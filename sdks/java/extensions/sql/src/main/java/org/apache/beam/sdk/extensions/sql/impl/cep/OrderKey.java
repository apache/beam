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
package org.apache.beam.sdk.extensions.sql.impl.cep;

import java.io.Serializable;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelFieldCollation;

/**
 * The {@code OrderKey} class stores the information to sort a column. {@param fIndex} is the field
 * (column) index. {@param dir} is the direction (true for ascending). {@param nullFirst} states
 * whether to put null values first.
 *
 * <h3>Constraints</h3>
 *
 * <ul>
 *   <ui>Strict orders are not supported for now.
 * </ul>
 */
public class OrderKey implements Serializable {

  private final int fIndex;
  private final boolean dir;
  private final boolean nullFirst;

  private OrderKey(int fIndex, boolean dir, boolean nullFirst) {
    this.fIndex = fIndex;
    this.dir = dir;
    this.nullFirst = nullFirst;
  }

  public int getIndex() {
    return fIndex;
  }

  public boolean getDir() {
    return dir;
  }

  public boolean getNullFirst() {
    return nullFirst;
  }

  public static OrderKey of(RelFieldCollation orderKey) {
    int fieldIndex = orderKey.getFieldIndex();
    RelFieldCollation.Direction dir = orderKey.direction;
    RelFieldCollation.NullDirection nullDir = orderKey.nullDirection;
    if (!dir.isDescending()) {
      if (nullDir == RelFieldCollation.NullDirection.FIRST) {
        return new OrderKey(fieldIndex, true, true);
      } else {
        return new OrderKey(fieldIndex, true, false);
      }
    } else {
      if (nullDir == RelFieldCollation.NullDirection.FIRST) {
        return new OrderKey(fieldIndex, false, true);
      } else {
        return new OrderKey(fieldIndex, false, false);
      }
    }
  }
}
