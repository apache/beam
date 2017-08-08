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
package org.apache.beam.sdk.values;

import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.BeamRecordCoder;
import org.apache.beam.sdk.coders.Coder;

/**
 * The default type provider used in {@link BeamRecord}.
 */
@Experimental
public class BeamRecordType implements Serializable{
  private List<String> fieldNames;
  private List<Coder> fieldCoders;

  public BeamRecordType(List<String> fieldNames, List<Coder> fieldCoders) {
    this.fieldNames = fieldNames;
    this.fieldCoders = fieldCoders;
  }

  /**
   * Validate input fieldValue for a field.
   * @throws IllegalArgumentException throw exception when the validation fails.
   */
   public void validateValueType(int index, Object fieldValue)
      throws IllegalArgumentException{
     //do nothing by default.
   }

   /**
    * Get the coder for {@link BeamRecordCoder}.
    */
   public BeamRecordCoder getRecordCoder(){
     return BeamRecordCoder.of(this, fieldCoders);
   }

   public List<String> getFieldNames(){
     return fieldNames;
   }

   public String getFieldByIndex(int index){
     return fieldNames.get(index);
   }

   public int findIndexOfField(String fieldName){
     return fieldNames.indexOf(fieldName);
   }

  public int size(){
    return fieldNames.size();
  }
}
