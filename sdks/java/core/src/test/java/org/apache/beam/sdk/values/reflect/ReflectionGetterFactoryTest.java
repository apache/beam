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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;

/**
 * Unit tests for {@link ReflectionGetterFactory}.
 */
public class ReflectionGetterFactoryTest {

  /**
   * Test pojo.
   */
  private static class Pojo {
    private String privateStringField = "privateStringValue";
    private Integer privateIntegerField = 15;

    public String publicStringField = "publicStringField";

    public String getPrivateStringField() {
      return privateStringField;
    }

    public Integer getPrivateIntegerField() {
      return privateIntegerField;
    }
  }

  @Test
  public void testGettersHaveCorrectNames() throws Exception {
    List<FieldValueGetter> getters = new ReflectionGetterFactory().generateGetters(Pojo.class);

    assertEquals(
        ImmutableSet.of("privateStringField", "privateIntegerField"),
        getNames(getters));
  }

  @Test
  public void testGettersHaveCorrectTypes() throws Exception {
    List<FieldValueGetter> getters = new ReflectionGetterFactory().generateGetters(Pojo.class);

    assertEquals(
        ImmutableSet.of(String.class, Integer.class),
        getTypes(getters));
  }

  @Test
  public void testGettersReturnCorrectValues() throws Exception {
    List<FieldValueGetter> getters = new ReflectionGetterFactory().generateGetters(Pojo.class);

    assertEquals(
        ImmutableSet.<Object>of("privateStringValue", 15),
        getValues(getters, new Pojo()));
  }

  private Set<String> getNames(List<FieldValueGetter> getters) {
    ImmutableSet.Builder<String> names = ImmutableSet.builder();

    for (FieldValueGetter getter : getters) {
      names.add(getter.name());
    }

    return names.build();
  }

  private Set<Class> getTypes(List<FieldValueGetter> getters) {
    ImmutableSet.Builder<Class> types = ImmutableSet.builder();

    for (FieldValueGetter getter : getters) {
      types.add(getter.type());
    }

    return types.build();
  }

  private ImmutableSet<Object> getValues(List<FieldValueGetter> getters, Pojo pojo) {
    ImmutableSet.Builder<Object> values = ImmutableSet.builder();

    for (FieldValueGetter getter : getters) {
      values.add(getter.get(pojo));
    }

    return values.build();
  }
}
