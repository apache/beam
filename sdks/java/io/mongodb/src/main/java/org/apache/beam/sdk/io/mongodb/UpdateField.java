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
package org.apache.beam.sdk.io.mongodb;

import java.io.Serializable;

public class UpdateField implements Serializable {

  private String updateOperator;

  private String sourceField;

  private String destField;

  public UpdateField(String updateOperator, String sourceField, String destField) {
    this.updateOperator = updateOperator;
    this.sourceField = sourceField;
    this.destField = destField;
  }

  /** for updating field by field. */
  public static UpdateField of(String updateOperator, String sourceField, String destField) {
    return new UpdateField(updateOperator, sourceField, destField);
  }

  /** for updating with entire input document. */
  public static UpdateField of(String updateOperator, String destField) {
    return new UpdateField(updateOperator, null, destField);
  }

  public String getSourceField() {
    return sourceField;
  }

  public String getUpdateOperator() {
    return updateOperator;
  }

  public String getDestField() {
    return destField;
  }
}
