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
import org.apache.beam.sdk.schemas.Schema;

/**
 * The {@code CEPMeasure} class represents the Measures clause and contains information about output
 * columns.
 */
public class CEPMeasure implements Serializable {

  private final String outTableName;
  private final CEPOperation opr;
  private final CEPFieldRef fieldRef;
  private final Schema.FieldType fieldType;

  public CEPMeasure(Schema streamSchema, String outTableName, CEPOperation opr) {
    this.outTableName = outTableName;
    this.opr = opr;
    this.fieldRef = CEPUtil.getFieldRef(opr);
    this.fieldType = CEPUtil.getFieldType(streamSchema, opr);
  }

  // return the out column name
  public String getName() {
    return outTableName;
  }

  public CEPOperation getOperation() {
    return opr;
  }

  public CEPFieldRef getField() {
    return fieldRef;
  }

  public Schema.FieldType getType() {
    return fieldType;
  }
}
