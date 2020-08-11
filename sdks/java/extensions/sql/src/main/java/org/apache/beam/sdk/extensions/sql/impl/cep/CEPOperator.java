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
import java.util.Map;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * The {@code CEPOperator} records the operators (i.e. functions) in the {@code DEFINE} clause of
 * {@code MATCH_RECOGNIZE}.
 */
public class CEPOperator implements Serializable {
  private final CEPKind cepKind;
  private static final Map<SqlKind, CEPKind> CEPKindTable =
      ImmutableMap.<SqlKind, CEPKind>builder()
          .put(SqlKind.SUM, CEPKind.SUM)
          .put(SqlKind.COUNT, CEPKind.COUNT)
          .put(SqlKind.AVG, CEPKind.AVG)
          .put(SqlKind.FIRST, CEPKind.FIRST)
          .put(SqlKind.LAST, CEPKind.LAST)
          .put(SqlKind.PREV, CEPKind.PREV)
          .put(SqlKind.NEXT, CEPKind.NEXT)
          .put(SqlKind.EQUALS, CEPKind.EQUALS)
          .put(SqlKind.NOT_EQUALS, CEPKind.NOT_EQUALS)
          .put(SqlKind.GREATER_THAN, CEPKind.GREATER_THAN)
          .put(SqlKind.GREATER_THAN_OR_EQUAL, CEPKind.GREATER_THAN_OR_EQUAL)
          .put(SqlKind.LESS_THAN, CEPKind.LESS_THAN)
          .put(SqlKind.LESS_THAN_OR_EQUAL, CEPKind.LESS_THAN_OR_EQUAL)
          .build();

  private CEPOperator(CEPKind cepKind) {
    this.cepKind = cepKind;
  }

  public CEPKind getCepKind() {
    return cepKind;
  }

  public static CEPOperator of(SqlOperator op) {
    SqlKind opKind = op.getKind();
    return new CEPOperator(CEPKindTable.getOrDefault(opKind, CEPKind.NONE));
  }

  @Override
  public String toString() {
    return cepKind.name();
  }
}
