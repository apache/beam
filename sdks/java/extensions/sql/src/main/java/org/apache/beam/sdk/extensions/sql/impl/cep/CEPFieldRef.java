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

import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexPatternFieldRef;

/**
 * A {@code CEPFieldRef} instance represents a node that points to a specified field in a {@code
 * Row}. It has similar functionality as Calcite's {@code RexPatternFieldRef}.
 */
public class CEPFieldRef extends CEPOperation {

  private final String alpha;
  private final int fieldIndex;

  CEPFieldRef(String alpha, int fieldIndex) {
    this.alpha = alpha;
    this.fieldIndex = fieldIndex;
  }

  public static CEPFieldRef of(RexPatternFieldRef rexFieldRef) {
    return new CEPFieldRef(rexFieldRef.getAlpha(), rexFieldRef.getIndex());
  }

  public String getAlpha() {
    return alpha;
  }

  public int getIndex() {
    return fieldIndex;
  }

  @Override
  public String toString() {
    return String.format("%s.$%d", alpha, fieldIndex);
  }
}
