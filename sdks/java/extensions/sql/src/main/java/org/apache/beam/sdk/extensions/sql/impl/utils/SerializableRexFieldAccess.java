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
package org.apache.beam.sdk.extensions.sql.impl.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexFieldAccess;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexInputRef;

/** SerializableRexFieldAccess. */
public class SerializableRexFieldAccess extends SerializableRexNode {
  private List<Integer> indexes = new ArrayList<>();

  public SerializableRexFieldAccess(RexFieldAccess rexFieldAccess) {
    indexes.add(rexFieldAccess.getField().getIndex());
    RexFieldAccess curr = rexFieldAccess;
    while (curr.getReferenceExpr() instanceof RexFieldAccess) {
      curr = (RexFieldAccess) curr.getReferenceExpr();
      indexes.add(rexFieldAccess.getField().getIndex());
    }

    // curr.getReferenceExpr() is not a RexFieldAccess. Check if it is RexInputRef, which is only
    // allowed RexNode type in RexFieldAccess.
    if (!(curr.getReferenceExpr() instanceof RexInputRef)) {
      throw new UnsupportedOperationException(
          "Does not support " + curr.getReferenceExpr().getType());
    }

    // curr.getReferenceExpr() is a RexInputRef.
    RexInputRef inputRef = (RexInputRef) curr.getReferenceExpr();
    indexes.add(inputRef.getIndex());

    Collections.reverse(indexes);
  }

  public List<Integer> getIndexes() {
    return indexes;
  }
}
