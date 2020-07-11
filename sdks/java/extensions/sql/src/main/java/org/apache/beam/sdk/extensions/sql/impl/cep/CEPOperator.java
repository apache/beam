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
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlOperator;

public class CEPOperator implements Serializable {
  private final CEPKind cepKind;

  private CEPOperator(CEPKind cepKind) {
    this.cepKind = cepKind;
  }

  public CEPKind getCepKind() {
    return cepKind;
  }

  public static CEPOperator of(SqlOperator op) {
    switch (op.getKind()) {
      case LAST:
        return new CEPOperator(CEPKind.LAST);
      case PREV:
        return new CEPOperator(CEPKind.PREV);
      case NEXT:
        return new CEPOperator(CEPKind.NEXT);
      case EQUALS:
        return new CEPOperator(CEPKind.EQUALS);
      case GREATER_THAN:
        return new CEPOperator(CEPKind.GREATER_THAN);
      case GREATER_THAN_OR_EQUAL:
        return new CEPOperator(CEPKind.GREATER_THAN_OR_EQUAL);
      case LESS_THAN:
        return new CEPOperator(CEPKind.LESS_THAN);
      case LESS_THAN_OR_EQUAL:
        return new CEPOperator(CEPKind.LESS_THAN_OR_EQUAL);
      default:
        return new CEPOperator(CEPKind.NONE);
    }
  }
}
