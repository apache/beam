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

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.UdafImpl;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.FunctionParameter;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TypedCombineFnDelegateTest {

  @Rule public ExpectedException exceptions = ExpectedException.none();

  @Test
  public void testParameterExtractionFromCombineFn_CombineFnDelegate() {
    Combine.BinaryCombineFn<String> max =
        Max.of(
            (Comparator<String> & Serializable) (a, b) -> Integer.compare(a.length(), b.length()));
    UdafImpl<String, Combine.Holder<String>, String> udaf =
        new UdafImpl<>(new TypedCombineFnDelegate<String, Combine.Holder<String>, String>(max) {});
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    List<FunctionParameter> parameters = udaf.getParameters();
    assertEquals(1, parameters.size());
    assertEquals(SqlTypeName.VARCHAR, parameters.get(0).getType(typeFactory).getSqlTypeName());
  }

  @Test
  public void testParameterExtractionFromCombineFn_CombineFnDelegate_WithGenericArray() {
    Combine.BinaryCombineFn<List<String>[]> max =
        Max.of(
            (Comparator<List<String>[]> & Serializable)
                (a, b) -> Integer.compare(a[0].get(0).length(), b[0].get(0).length()));
    UdafImpl<List<String>[], Combine.Holder<List<String>[]>, List<String>[]> udaf =
        new UdafImpl<>(
            new TypedCombineFnDelegate<
                List<String>[], Combine.Holder<List<String>[]>, List<String>[]>(max) {});
    exceptions.expect(IllegalArgumentException.class);
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    udaf.getParameters().get(0).getType(typeFactory);
  }

  @Test
  public void testParameterExtractionFromCombineFn_CombineFnDelegate_WithListInsteadOfArray() {
    Combine.BinaryCombineFn<List<List<String>>> max =
        Max.of(
            (Comparator<List<List<String>>> & Serializable)
                (a, b) -> Integer.compare(a.get(0).get(0).length(), b.get(0).get(0).length()));
    UdafImpl<List<List<String>>, Combine.Holder<List<List<String>>>, List<List<String>>> udaf =
        new UdafImpl<>(
            new TypedCombineFnDelegate<
                List<List<String>>, Combine.Holder<List<List<String>>>, List<List<String>>>(
                max) {});
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    List<FunctionParameter> parameters = udaf.getParameters();
    assertEquals(1, parameters.size());
    assertEquals(SqlTypeName.ARRAY, parameters.get(0).getType(typeFactory).getSqlTypeName());
  }
}
