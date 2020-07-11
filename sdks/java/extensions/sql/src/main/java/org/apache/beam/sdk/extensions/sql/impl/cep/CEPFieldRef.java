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

public class CEPFieldRef extends CEPOperation {
  private final int fieldIndex;

  private CEPFieldRef(int fieldIndex) {
    this.fieldIndex = fieldIndex;
  }

  public static CEPFieldRef of(RexPatternFieldRef rexFieldRef) {
    return new CEPFieldRef(rexFieldRef.getIndex());
  }

  public int getIndex() {
    return fieldIndex;
  }
}
