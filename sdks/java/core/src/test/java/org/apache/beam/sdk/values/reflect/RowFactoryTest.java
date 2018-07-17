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

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.values.reflect.ReflectionUtils.getPublicGetters;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Unit tests for {@link RowFactory}. */
@RunWith(Parameterized.class)
public class RowFactoryTest {

  /** Test pojo. */
  public static final class SomePojo {
    private String someStringField;
    private Integer someIntegerField;

    public SomePojo(String someStringField, Integer someIntegerField) {
      this.someStringField = someStringField;
      this.someIntegerField = someIntegerField;
    }

    public String getSomeStringField() {
      return someStringField;
    }

    public Integer getSomeIntegerField() {
      return someIntegerField;
    }
  }

  /** Getters factories to test the record factory with. */
  @Parameterized.Parameters
  public static Iterable<GetterFactory> gettersFactories() {
    return ImmutableList.of(
        new GeneratedGetterFactory(),
        new ReflectionGetterFactory(),
        clazz -> getPublicGetters(clazz).stream().map(ReflectionGetter::new).collect(toList()));
  }

  private GetterFactory getterFactory;

  public RowFactoryTest(GetterFactory getterFactory) {
    this.getterFactory = getterFactory;
  }

  @Test
  public void testNewRecordFieldValues() throws Exception {
    SomePojo pojo = new SomePojo("someString", 42);
    RowFactory factory = newFactory();

    Row row = factory.create(pojo);

    assertEquals(2, row.getFieldCount());
    assertThat(row.getValues(), containsInAnyOrder((Object) "someString", 42));
  }

  @Test
  public void testNewRecordFieldNames() throws Exception {
    SomePojo pojo = new SomePojo("someString", 42);
    RowFactory factory = newFactory();

    Row row = factory.create(pojo);

    assertThat(
        row.getSchema().getFieldNames(), containsInAnyOrder("someStringField", "someIntegerField"));
  }

  @Test
  public void testCreatesNewInstanceEachTime() throws Exception {
    SomePojo pojo = new SomePojo("someString", 42);
    RowFactory factory = newFactory();

    Row row1 = factory.create(pojo);
    Row row2 = factory.create(pojo);

    assertNotSame(row1, row2);
  }

  @Test
  public void testCachesRecordType() throws Exception {
    SomePojo pojo = new SomePojo("someString", 42);
    RowFactory factory = newFactory();

    Row row1 = factory.create(pojo);
    Row row2 = factory.create(pojo);

    assertSame(row1.getSchema(), row2.getSchema());
  }

  @Test
  public void testCopiesValues() throws Exception {
    SomePojo pojo = new SomePojo("someString", 42);
    RowFactory factory = newFactory();

    Row row = factory.create(pojo);

    assertThat(row.getValues(), containsInAnyOrder((Object) "someString", 42));

    pojo.someIntegerField = 23;
    pojo.someStringField = "hello";

    assertThat(row.getValues(), containsInAnyOrder((Object) "someString", 42));
  }

  private RowFactory newFactory() {
    return new RowFactory(new DefaultSchemaFactory(), getterFactory);
  }
}
