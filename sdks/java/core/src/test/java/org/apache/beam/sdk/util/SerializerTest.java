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
package org.apache.beam.sdk.util;

import static org.apache.beam.sdk.util.Structs.addBoolean;
import static org.apache.beam.sdk.util.Structs.addDouble;
import static org.apache.beam.sdk.util.Structs.addLong;
import static org.apache.beam.sdk.util.Structs.addString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests Serializer implementation.
 */
@RunWith(JUnit4.class)
public class SerializerTest {
  /**
   * A POJO to use for testing serialization.
   */
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
      property = PropertyNames.OBJECT_TYPE_NAME)
  public static class TestRecord {
    // TODO: When we apply property name typing to all non-final classes, the
    // annotation on this class should be removed.
    public String name;
    public boolean ok;
    public int value;
    public double dValue;
  }

  @Test
  public void testStatefulDeserialization() {
    CloudObject object = CloudObject.forClass(TestRecord.class);

    addString(object, "name", "foobar");
    addBoolean(object, "ok", true);
    addLong(object, "value", 42L);
    addDouble(object, "dValue", .25);

    TestRecord record = Serializer.deserialize(object, TestRecord.class);
    Assert.assertEquals("foobar", record.name);
    Assert.assertEquals(true, record.ok);
    Assert.assertEquals(42L, record.value);
    Assert.assertEquals(0.25, record.dValue, 0.0001);
  }

  private static class InjectedTestRecord {
    private final String n;
    private final int v;

    @SuppressWarnings("unused")  // used for JSON serialization
    public InjectedTestRecord(
        @JsonProperty("name") String name,
        @JsonProperty("value") int value) {
      this.n = name;
      this.v = value;
    }

    public String getName() {
      return n;
    }
    public int getValue() {
      return v;
    }
  }

  @Test
  public void testDeserializationInjection() {
    CloudObject object = CloudObject.forClass(InjectedTestRecord.class);
    addString(object, "name", "foobar");
    addLong(object, "value", 42L);

    InjectedTestRecord record =
        Serializer.deserialize(object, InjectedTestRecord.class);

    Assert.assertEquals("foobar", record.getName());
    Assert.assertEquals(42L, record.getValue());
  }

  private static class FactoryInjectedTestRecord {
    @JsonCreator
    public static FactoryInjectedTestRecord of(
        @JsonProperty("name") String name,
        @JsonProperty("value") int value) {
      return new FactoryInjectedTestRecord(name, value);
    }

    private final String n;
    private final int v;

    private FactoryInjectedTestRecord(String name, int value) {
      this.n = name;
      this.v = value;
    }

    public String getName() {
      return n;
    }
    public int getValue() {
      return v;
    }
  }

  @Test
  public void testDeserializationFactoryInjection() {
    CloudObject object = CloudObject.forClass(FactoryInjectedTestRecord.class);
    addString(object, "name", "foobar");
    addLong(object, "value", 42L);

    FactoryInjectedTestRecord record =
        Serializer.deserialize(object, FactoryInjectedTestRecord.class);
    Assert.assertEquals("foobar", record.getName());
    Assert.assertEquals(42L, record.getValue());
  }

  private static class DerivedTestRecord extends TestRecord {
    public String derived;
  }

  @Test
  public void testSubclassDeserialization() {
    CloudObject object = CloudObject.forClass(DerivedTestRecord.class);

    addString(object, "name", "foobar");
    addBoolean(object, "ok", true);
    addLong(object, "value", 42L);
    addDouble(object, "dValue", .25);
    addString(object, "derived", "baz");

    TestRecord result = Serializer.deserialize(object, TestRecord.class);
    Assert.assertThat(result, Matchers.instanceOf(DerivedTestRecord.class));

    DerivedTestRecord record = (DerivedTestRecord) result;
    Assert.assertEquals("foobar", record.name);
    Assert.assertEquals(true, record.ok);
    Assert.assertEquals(42L, record.value);
    Assert.assertEquals(0.25, record.dValue, 0.0001);
    Assert.assertEquals("baz", record.derived);
  }
}
