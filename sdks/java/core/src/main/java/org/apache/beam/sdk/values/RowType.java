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

import static com.google.common.base.MoreObjects.toStringHelper;

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.Coder;

/**
 * {@link RowType} describes the fields in {@link Row}, extra checking can be added
 * by overwriting {@link RowType#validateValueType(int, Object)}.
 */
@Experimental
public class RowType implements Serializable{
  private List<String> fieldNames;
  private List<Coder> fieldCoders;

  /**
   * Create a {@link RowType} with a name and Coder for each field.
   */
  public RowType(List<String> fieldNames, List<Coder> fieldCoders) {
    if (fieldNames.size() != fieldCoders.size()) {
      throw new IllegalStateException(
          "the size of fieldNames and fieldCoders need to be the same.");
    }
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
   * Return the coder for {@link Row}, which wraps {@link #fieldCoders} for each field.
   */
  public RowCoder getRecordCoder(){
    return RowCoder.of(this, fieldCoders);
  }

  /**
   * Return the field coder for {@code index}.
   */
  public Coder getFieldCoder(int index){
    return fieldCoders.get(index);
  }

  /**
   * Returns an immutable list of field names.
   */
  public List<String> getFieldNames(){
    return ImmutableList.copyOf(fieldNames);
  }

  /**
   * Return the name of field by index.
   */
  public String getFieldNameByIndex(int index){
    return fieldNames.get(index);
  }

  /**
   * Find the index of a given field.
   */
  public int findIndexOfField(String fieldName){
    return fieldNames.indexOf(fieldName);
  }

  /**
   * Return the count of fields.
   */
  public int getFieldCount(){
    return fieldNames.size();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof RowType)) {
      return false;
    }

    RowType that = (RowType) o;
    return Objects.equals(fieldNames, that.fieldNames)
        && Objects.equals(fieldCoders, that.fieldCoders);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldNames, fieldCoders);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("fieldNames", fieldNames)
        .add("fieldCoders", fieldCoders)
        .toString();
  }
}
