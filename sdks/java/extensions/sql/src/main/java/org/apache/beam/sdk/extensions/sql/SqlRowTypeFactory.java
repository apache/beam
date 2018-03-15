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

package org.apache.beam.sdk.extensions.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.values.Schema;
import org.apache.beam.sdk.values.reflect.FieldValueGetter;
import org.apache.beam.sdk.values.reflect.RowTypeFactory;

/**
 * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
 *
 * <p>Implementation of the {@link RowTypeFactory} to return instances
 * of {@link Schema} with coders specific for SQL types, see {@link SqlTypeCoders}.
 */
@Internal
public class SqlRowTypeFactory implements RowTypeFactory {

  static final ImmutableMap<Class, Coder> SQL_CODERS = ImmutableMap
      .<Class, Coder>builder()
      .put(Byte.class, SqlTypeCoders.TINYINT)
      .put(Short.class, SqlTypeCoders.SMALLINT)
      .put(Integer.class, SqlTypeCoders.INTEGER)
      .put(Long.class, SqlTypeCoders.BIGINT)
      .put(Float.class, SqlTypeCoders.FLOAT)
      .put(Double.class, SqlTypeCoders.DOUBLE)
      .put(BigDecimal.class, SqlTypeCoders.DECIMAL)
      .put(Boolean.class, SqlTypeCoders.BOOLEAN)
      .put(String.class, SqlTypeCoders.VARCHAR)
      .put(GregorianCalendar.class, SqlTypeCoders.TIME)
      .put(Date.class, SqlTypeCoders.TIMESTAMP)
      .build();

  @Override
  public Schema createRowType(Iterable<FieldValueGetter> getters) {
    return
        Schema
            .fromNamesAndCoders(
                fieldNames(getters),
                sqlCoders(getters));
  }

  private List<String> fieldNames(Iterable<FieldValueGetter> getters) {
    ImmutableList.Builder<String> names = ImmutableList.builder();

    for (FieldValueGetter fieldValueGetter : getters) {
      names.add(fieldValueGetter.name());
    }

    return names.build();
  }

  private List<Coder> sqlCoders(Iterable<FieldValueGetter> getters) {
    ImmutableList.Builder<Coder> sqlCoders = ImmutableList.builder();

    for (FieldValueGetter fieldValueGetter : getters) {
      if (!SQL_CODERS.containsKey(fieldValueGetter.type())) {
        throw new UnsupportedOperationException(
            "Field type " + fieldValueGetter.type().getSimpleName() + " is not supported yet");
      }

      sqlCoders.add(SQL_CODERS.get(fieldValueGetter.type()));
    }

    return sqlCoders.build();
  }
}
