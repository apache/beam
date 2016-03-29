/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.values;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link TupleTag}.
 */
@RunWith(JUnit4.class)
public class TupleTagTest {

  private static TupleTag<Object> staticTag = new TupleTag<>();
  private static TupleTag<Object> staticBlockTag;
  private static TupleTag<Object> staticMethodTag = createTag();
  private static TupleTag<Object> instanceMethodTag = new AnotherClass().createAnotherTag();

  static {
    staticBlockTag = new TupleTag<>();
  }

  private static TupleTag<Object> createTag() {
    return new TupleTag<>();
  }

  private static class AnotherClass {
    private static TupleTag<Object> anotherTag = new TupleTag<>();
    private TupleTag<Object> createAnotherTag() {
      return new TupleTag<>();
    }
  }

  @Test
  public void testStaticTupleTag() {
    assertEquals("com.google.cloud.dataflow.sdk.values.TupleTagTest#0", staticTag.getId());
    assertEquals("com.google.cloud.dataflow.sdk.values.TupleTagTest#3", staticBlockTag.getId());
    assertEquals("com.google.cloud.dataflow.sdk.values.TupleTagTest#1", staticMethodTag.getId());
    assertEquals("com.google.cloud.dataflow.sdk.values.TupleTagTest#2", instanceMethodTag.getId());
    assertEquals(
        "com.google.cloud.dataflow.sdk.values.TupleTagTest$AnotherClass#0",
        AnotherClass.anotherTag.getId());
  }

  private TupleTag<Object> createNonstaticTupleTag() {
    return new TupleTag<Object>();
  }

  @Test
  public void testNonstaticTupleTag() {
    assertNotEquals(new TupleTag<Object>().getId(), new TupleTag<Object>().getId());
    assertNotEquals(createNonstaticTupleTag(), createNonstaticTupleTag());

    TupleTag<Object> tag = createNonstaticTupleTag();

    // Check that the name is derived from the method it is created in.
    assertThat(tag.getId().split("#")[0],
        startsWith("com.google.cloud.dataflow.sdk.values.TupleTagTest.createNonstaticTupleTag"));

    // Check that after the name there is a ':' followed by a line number, and just make
    // sure the line number is big enough to be reasonable, so superficial changes don't break
    // the test.
    assertThat(Integer.parseInt(tag.getId().split("#")[0].split(":")[1]),
        greaterThan(15));
  }
}
