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

package org.apache.beam.sdk.values.reflect;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.schemas.Schema;

/**
 * A default implementation of the {@link RowTypeFactory} interface. The purpose of
 * the factory is to create a row types given a list of getters.
 *
 * <p>Row type is represented by {@link Schema} which essentially is a
 * {@code List<Pair<FieldName, Coder>>}.
 *
 * <p>Getters (e.g. pojo field getters) are represented by {@link FieldValueGetter} interface,
 * which exposes the field's name (see {@link FieldValueGetter#name()})
 * and java type (see {@link FieldValueGetter#type()}).
 *
 * <p>This factory then uses the default {@link CoderRegistry} to map java types of
 * the getters to coders, and then creates an instance of {@link Schema} using those coders.
 *
 * <p>If there is no coder in the default {@link CoderRegistry} for the java type of the getter,
 * then the factory throws {@link UnsupportedOperationException}.
 *
 * <p>This is the default factory implementation used in {@link RowFactory}.
 * It should work for regular java pipelines where coder mapping via default {@link CoderRegistry}
 * is enough.
 *
 * <p>In other cases, when mapping requires extra logic, another implentation of the
 * {@link RowTypeFactory} should be used instead of this class.
 *
 * <p>For example, Beam SQL uses {@link java.sql.Types} as an intermediate type representation
 * instead of using plain java types. The mapping between {@link java.sql.Types} and coders
 * is not available in the default {@link CoderRegistry}, so custom SQL-specific implementation of
 * {@link RowTypeFactory} is used with SQL infrastructure instead of this class.
 * See {@code SqlRecordTypeFactory}.
 */
class DefaultRowTypeFactory implements RowTypeFactory {

  private static final CoderRegistry CODER_REGISTRY = CoderRegistry.createDefault();

  /**
   * Uses {@link FieldValueGetter#name()} as field names.
   * Uses {@link CoderRegistry#createDefault()} to get coders for {@link FieldValueGetter#type()}.
   */
  @Override
  public Schema createRowType(Iterable<FieldValueGetter> fieldValueGetters) {
    return
        Schema
            .fromNamesAndCoders(
                getFieldNames(fieldValueGetters),
                getFieldCoders(fieldValueGetters));
  }

  private static List<String> getFieldNames(Iterable<FieldValueGetter> fieldValueGetters) {
    ImmutableList.Builder<String> names = ImmutableList.builder();

    for (FieldValueGetter fieldValueGetter : fieldValueGetters) {
      names.add(fieldValueGetter.name());
    }

    return names.build();
  }

  private static List<Coder> getFieldCoders(Iterable<FieldValueGetter> fieldValueGetters) {
    ImmutableList.Builder<Coder> coders = ImmutableList.builder();

    for (FieldValueGetter fieldValueGetter : fieldValueGetters) {
      try {
        coders.add(CODER_REGISTRY.getCoder(fieldValueGetter.type()));
      } catch (CannotProvideCoderException e) {
        throw new UnsupportedOperationException(
            "Fields of type "
                + fieldValueGetter.type().getSimpleName() + " are not supported yet", e);
      }
    }

    return coders.build();
  }
}
