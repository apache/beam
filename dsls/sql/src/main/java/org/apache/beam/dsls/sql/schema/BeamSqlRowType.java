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
package org.apache.beam.dsls.sql.schema;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.List;

/**
 * Field type information in {@link BeamSqlRow}.
 *
 */
@AutoValue
public abstract class BeamSqlRowType implements Serializable {
  public abstract List<String> getFieldsName();
  public abstract List<Integer> getFieldsType();

  public static BeamSqlRowType create(List<String> fieldNames, List<Integer> fieldTypes) {
    return new AutoValue_BeamSqlRowType(fieldNames, fieldTypes);
  }

  public int size() {
    return getFieldsName().size();
  }
}
