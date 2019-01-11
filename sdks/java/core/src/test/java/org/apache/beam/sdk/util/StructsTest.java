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
import static org.apache.beam.sdk.util.Structs.addList;
import static org.apache.beam.sdk.util.Structs.addLong;
import static org.apache.beam.sdk.util.Structs.addLongs;
import static org.apache.beam.sdk.util.Structs.addNull;
import static org.apache.beam.sdk.util.Structs.addString;
import static org.apache.beam.sdk.util.Structs.addStringList;
import static org.apache.beam.sdk.util.Structs.getBoolean;
import static org.apache.beam.sdk.util.Structs.getDictionary;
import static org.apache.beam.sdk.util.Structs.getInt;
import static org.apache.beam.sdk.util.Structs.getListOfMaps;
import static org.apache.beam.sdk.util.Structs.getLong;
import static org.apache.beam.sdk.util.Structs.getObject;
import static org.apache.beam.sdk.util.Structs.getString;
import static org.apache.beam.sdk.util.Structs.getStrings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for Structs.
 */
@RunWith(JUnit4.class)
public class StructsTest {
  private List<Map<String, Object>> makeCloudObjects() {
    List<Map<String, Object>> objects = new ArrayList<>();
    {
      CloudObject o = CloudObject.forClassName("string");
      addString(o, "singletonStringKey", "stringValue");
      objects.add(o);
    }
    {
      CloudObject o = CloudObject.forClassName("long");
      addLong(o, "singletonLongKey", 42L);
      objects.add(o);
    }
    return objects;
  }

  private Map<String, Object> makeCloudDictionary() {
    Map<String, Object> o = new HashMap<>();
    addList(o, "emptyKey", Collections.<Map<String, Object>>emptyList());
    addNull(o, "noStringsKey");
    addString(o, "singletonStringKey", "stringValue");
    addStringList(o, "multipleStringsKey", Arrays.asList("hi", "there", "bob"));
    addLongs(o, "multipleLongsKey", 47L, 1L << 42, -5L);
    addLong(o, "singletonLongKey", 42L);
    addDouble(o, "singletonDoubleKey", 3.14);
    addBoolean(o, "singletonBooleanKey", true);
    addNull(o, "noObjectsKey");
    addList(o, "multipleObjectsKey", makeCloudObjects());
    return o;
  }

  @Test
  public void testGetStringParameter() throws Exception {
    Map<String, Object> o = makeCloudDictionary();

    Assert.assertEquals(
        "stringValue",
        getString(o, "singletonStringKey"));
    Assert.assertEquals(
        "stringValue",
        getString(o, "singletonStringKey", "defaultValue"));
    Assert.assertEquals(
        "defaultValue",
        getString(o, "missingKey", "defaultValue"));

    try {
      getString(o, "missingKey");
      Assert.fail("should have thrown an exception");
    } catch (Exception exn) {
      Assert.assertThat(exn.toString(),
                        Matchers.containsString(
                            "didn't find required parameter missingKey"));
    }

    try {
      getString(o, "noStringsKey");
      Assert.fail("should have thrown an exception");
    } catch (Exception exn) {
      Assert.assertThat(exn.toString(),
                        Matchers.containsString("not a string"));
    }

    Assert.assertThat(getStrings(o, "noStringsKey", null), Matchers.<String>emptyIterable());
    Assert.assertThat(getObject(o, "noStringsKey").keySet(), Matchers.<String>emptyIterable());
    Assert.assertThat(getDictionary(o, "noStringsKey").keySet(), Matchers.<String>emptyIterable());
    Assert.assertThat(getDictionary(o, "noStringsKey", null).keySet(),
        Matchers.<String>emptyIterable());

    try {
      getString(o, "multipleStringsKey");
      Assert.fail("should have thrown an exception");
    } catch (Exception exn) {
      Assert.assertThat(exn.toString(),
                        Matchers.containsString("not a string"));
    }

    try {
      getString(o, "emptyKey");
      Assert.fail("should have thrown an exception");
    } catch (Exception exn) {
      Assert.assertThat(exn.toString(),
                        Matchers.containsString("not a string"));
    }
  }

  @Test
  public void testGetBooleanParameter() throws Exception {
    Map<String, Object> o = makeCloudDictionary();

    Assert.assertEquals(
        true,
        getBoolean(o, "singletonBooleanKey", false));
    Assert.assertEquals(
        false,
        getBoolean(o, "missingKey", false));

    try {
      getBoolean(o, "emptyKey", false);
      Assert.fail("should have thrown an exception");
    } catch (Exception exn) {
      Assert.assertThat(exn.toString(),
                        Matchers.containsString("not a boolean"));
    }
  }

  @Test
  public void testGetLongParameter() throws Exception {
    Map<String, Object> o = makeCloudDictionary();

    Assert.assertEquals(
        (Long) 42L,
        getLong(o, "singletonLongKey", 666L));
    Assert.assertEquals(
        (Integer) 42,
        getInt(o, "singletonLongKey", 666));
    Assert.assertEquals(
        (Long) 666L,
        getLong(o, "missingKey", 666L));

    try {
      getLong(o, "emptyKey", 666L);
      Assert.fail("should have thrown an exception");
    } catch (Exception exn) {
      Assert.assertThat(exn.toString(),
                        Matchers.containsString("not a long"));
    }
    try {
      getInt(o, "emptyKey", 666);
      Assert.fail("should have thrown an exception");
    } catch (Exception exn) {
      Assert.assertThat(exn.toString(),
                        Matchers.containsString("not an int"));
    }
  }

  @Test
  public void testGetListOfMaps() throws Exception {
    Map<String, Object> o = makeCloudDictionary();

    Assert.assertEquals(
        makeCloudObjects(),
        getListOfMaps(o, "multipleObjectsKey", null));

    try {
      getListOfMaps(o, "singletonLongKey", null);
      Assert.fail("should have thrown an exception");
    } catch (Exception exn) {
      Assert.assertThat(exn.toString(),
                        Matchers.containsString("not a list"));
    }
  }

  // TODO: Test builder operations.
}
