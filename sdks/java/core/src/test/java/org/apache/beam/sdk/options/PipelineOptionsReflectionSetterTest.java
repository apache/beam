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
package org.apache.beam.sdk.options;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link org.apache.beam.sdk.options.PipelineOptionsReflectionSetter}. */
@RunWith(JUnit4.class)
public class PipelineOptionsReflectionSetterTest {

  /** This is an object that can be used in PipelineOptions. */
  public static class ObjectOption {
    private int a;

    public void setA(int a) {
      this.a = a;
    }

    public int getA() {
      return a;
    }

    /** A value Factory for ObjectOption to be used in Default annotation. */
    public static class ObjectOptionFactory implements DefaultValueFactory<ObjectOption> {

      @Override
      public ObjectOption create(PipelineOptions options) {
        ObjectOption o = new ObjectOption();
        o.setA(123);
        return o;
      }
    }
  }

  /** A test interface containing some of the primitives with default value. */
  public interface Primitives extends PipelineOptions {
    @Default.Boolean(true)
    boolean getBoolean();

    void setBoolean(boolean value);

    @Default.Integer(123)
    int getInt();

    void setInt(int value);

    @Default.Long(1234)
    long getLong();

    void setLong(long value);
  }

  /** A test interface containing previous types and arrays and objects. */
  public interface ComplexOptions extends Primitives {

    @Default.InstanceFactory(ObjectOption.ObjectOptionFactory.class)
    ObjectOption getObject();

    void setObject(ObjectOption object);

    void setStringArray(String[] strings);

    String[] getStringArray();

    ObjectOption getObject2();

    void setObject2(ObjectOption object);
  }

  @Test
  public void testGetInterfaceForPipelineOptions() {
    PipelineOptions options = PipelineOptionsFactory.create();
    Class klass = PipelineOptionsReflectionSetter.getPipelineOptionsInterface(options);
    assertEquals(PipelineOptions.class, klass);
  }

  @Test
  public void testSettingExistingPrimitive() {
    Primitives options = PipelineOptionsFactory.as(Primitives.class);
    PipelineOptionsReflectionSetter.setOption(options, "boolean", "false");
    PipelineOptionsReflectionSetter.setOption(options, "int", "321");
    PipelineOptionsReflectionSetter.setOption(options, "long", "4321");
    PipelineOptionsReflectionSetter.setOption(options, "jobName", "someName");

    assertEquals(false, options.getBoolean());
    assertEquals(321, options.getInt());
    assertEquals(4321, options.getLong());
    assertEquals("someName", options.getJobName());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSettingNonExistingOption() {
    Primitives options = PipelineOptionsFactory.as(Primitives.class);
    PipelineOptionsReflectionSetter.setOption(options, "boolean2", "false");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSettingWithoutSetter() {
    Primitives options = PipelineOptionsFactory.as(Primitives.class);
    PipelineOptionsReflectionSetter.setOption(
        options, "class", "org.apache.beam.sdk.options.PipelineOptionsReflectionSetterTest.class");
  }

  @Test
  public void testRemovingPrimitivesWithDefault() {
    Primitives options = PipelineOptionsFactory.as(Primitives.class);
    PipelineOptionsReflectionSetter.setOption(options, "boolean", "false");
    PipelineOptionsReflectionSetter.setOption(options, "int", "321");
    PipelineOptionsReflectionSetter.setOption(options, "long", "4321");

    PipelineOptionsReflectionSetter.removeOption(options, "boolean");
    PipelineOptionsReflectionSetter.removeOption(options, "int");
    PipelineOptionsReflectionSetter.removeOption(options, "long");

    assertTrue(options.getBoolean());
    assertEquals(123, options.getInt());
    assertEquals(1234, options.getLong());
  }

  @Test
  public void testSettingExistingObjects() {
    ComplexOptions options = PipelineOptionsFactory.as(ComplexOptions.class);
    PipelineOptionsReflectionSetter.setOption(options, "object", "{\"a\":321}");
    PipelineOptionsReflectionSetter.setOption(options, "object2", "{\"a\":321}");
    PipelineOptionsReflectionSetter.setOption(options, "stringArray", "str1,str2");

    String[] strArray = {"str1", "str2"};
    assertEquals(321, options.getObject().getA());
    assertEquals(321, options.getObject2().getA());
    assertArrayEquals(strArray, options.getStringArray());
  }

  @Test
  public void testRemovingObject() {
    ComplexOptions options = PipelineOptionsFactory.as(ComplexOptions.class);
    PipelineOptionsReflectionSetter.setOption(options, "object", "{\"a\":321}");
    PipelineOptionsReflectionSetter.setOption(options, "object2", "{\"a\":321}");
    PipelineOptionsReflectionSetter.removeOption(options, "object");
    PipelineOptionsReflectionSetter.removeOption(options, "object2");

    assertEquals(123, options.getObject().getA());
    assertNull(options.getObject2());
  }
}
