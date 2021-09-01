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
package org.apache.beam.runners.dataflow.worker.util.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.util.common.Reiterator;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link TaggedReiteratorList}. */
@RunWith(JUnit4.class)
@SuppressWarnings({"keyfor"})
public class TaggedReiteratorListTest {

  @Test
  public void testSingleIterator() {
    TaggedReiteratorList iter = create(new String[] {"a", "b", "c"});
    assertEquals(iter.get(0), "a", "b", "c");
    assertEquals(iter.get(0), "a", "b", "c");
    assertEquals(iter.get(1) /*empty*/);
    assertEquals(iter.get(0), "a", "b", "c");
  }

  @Test
  public void testSequentialAccess() {
    TaggedReiteratorList iter = create(3, new String[] {"a", "b", "c"});
    for (int i = 0; i < 2; i++) {
      assertEquals(iter.get(0), "a0", "b0", "c0");
      assertEquals(iter.get(1), "a1", "b1", "c1");
      assertEquals(iter.get(2), "a2", "b2", "c2");
    }
    for (int i = 0; i < 2; i++) {
      assertEquals(iter.get(2), "a2", "b2", "c2");
      assertEquals(iter.get(1), "a1", "b1", "c1");
      assertEquals(iter.get(0), "a0", "b0", "c0");
    }
  }

  @Test
  public void testRandomAccess() {
    TaggedReiteratorList iter = create(6, new String[] {"a", "b"});
    assertEquals(iter.get(3), "a3", "b3");
    assertEquals(iter.get(1), "a1", "b1");
    assertEquals(iter.get(5), "a5", "b5");
    assertEquals(iter.get(0), "a0", "b0");
    assertEquals(iter.get(4), "a4", "b4");
    assertEquals(iter.get(4), "a4", "b4");
    assertEquals(iter.get(1), "a1", "b1");
  }

  @Test
  public void testPartialIteration() {
    TaggedReiteratorList iter = create(6, new String[] {"a", "b", "c"});
    Iterator<?> get0 = iter.get(0);
    Iterator<?> get1 = iter.get(1);
    Iterator<?> get3 = iter.get(3);
    assertEquals(asList(get0, 1), "a0");
    assertEquals(asList(get1, 2), "a1", "b1");
    assertEquals(asList(get3, 3), "a3", "b3", "c3");
    Iterator<?> get2 = iter.get(2);
    Iterator<?> get0Again = iter.get(0);
    assertEquals(asList(get0, 1), "b0");
    assertEquals(get2, "a2", "b2", "c2");
    assertEquals(get0Again, "a0", "b0", "c0");
    assertEquals(asList(get0), "c0");
    Iterator<?> get4 = iter.get(4);
    assertEquals(get4, "a4", "b4", "c4");
    assertEquals(get4 /*empty*/);
    assertEquals(iter.get(4), "a4", "b4", "c4");
  }

  @Test
  public void testNextIteration() {
    TaggedReiteratorList iter = create(2, new String[] {"a", "b", "c"});
    Reiterator<?> get0 = iter.get(0);
    assertEquals(get0, "a0", "b0", "c0");
    Iterator<?> get1 = iter.get(1);
    Assert.assertEquals("a1", get1.next());
    assertEquals(get0.copy() /*empty*/);
    Assert.assertEquals("b1", get1.next());
    assertEquals(iter.get(1), "a1", "b1", "c1");
  }

  @Test
  public void testEmpties() {
    TaggedReiteratorList iter =
        create(
            new String[] {},
            new String[] {"a", "b", "c"},
            new String[] {},
            new String[] {},
            new String[] {"d"});
    assertEquals(iter.get(2) /*empty*/);
    assertEquals(iter.get(1), "a", "b", "c");
    assertEquals(iter.get(2) /*empty*/);
    assertEquals(iter.get(0) /*empty*/);
    assertEquals(iter.get(2) /*empty*/);
    assertEquals(iter.get(4), "d");
    assertEquals(iter.get(3) /*empty*/);
  }

  /////////////////////////////////////////////////////////////////////////////
  // Helpers

  private TaggedReiteratorList create(String[]... values) {
    ArrayList<TaggedValue> taggedValues = new ArrayList<>();
    for (int tag = 0; tag < values.length; tag++) {
      for (String value : values[tag]) {
        taggedValues.add(new TaggedValue(tag, value));
      }
    }
    return new TaggedReiteratorList(
        new TestReiterator(taggedValues.toArray(new TaggedValue[0])), new TaggedValueExtractor());
  }

  private TaggedReiteratorList create(int repeat, String... values) {
    ArrayList<TaggedValue> taggedValues = new ArrayList<>();
    for (int tag = 0; tag < repeat; tag++) {
      for (String value : values) {
        taggedValues.add(new TaggedValue(tag, value + tag));
      }
    }
    return new TaggedReiteratorList(
        new TestReiterator(taggedValues.toArray(new TaggedValue[0])), new TaggedValueExtractor());
  }

  private <T> List<T> asList(Iterator<T> iter) {
    return asList(iter, Integer.MAX_VALUE);
  }

  private <T> List<T> asList(Iterator<T> iter, int limit) {
    List<T> list = new ArrayList<>();
    for (int i = 0; i < limit && iter.hasNext(); i++) {
      list.add(iter.next());
    }
    return list;
  }

  private void assertEquals(Iterator<?> actual, Object... expected) {
    assertEquals(asList(actual), expected);
  }

  private void assertEquals(List<?> actual, Object... expected) {
    Assert.assertEquals(Arrays.asList(expected), actual);
  }

  private static class TestReiterator implements Reiterator<TaggedValue> {
    private final TaggedValue[] values;
    private int pos = 0;

    public TestReiterator(TaggedValue... values) {
      this(values, 0);
    }

    private TestReiterator(TaggedValue[] values, int pos) {
      this.values = values;
      this.pos = pos;
    }

    @Override
    public boolean hasNext() {
      return pos < values.length;
    }

    @Override
    public TaggedValue next() {
      return values[pos++];
    }

    @Override
    public void remove() {
      throw new IllegalArgumentException();
    }

    @Override
    public TestReiterator copy() {
      return new TestReiterator(values, pos);
    }
  }

  private static class TaggedValueExtractor
      implements TaggedReiteratorList.TagExtractor<TaggedValue> {
    @Override
    public int getTag(TaggedValue elem) {
      return elem.tag;
    }

    @Override
    public String getValue(TaggedValue elem) {
      return elem.value;
    }
  }

  private static class TaggedValue {
    public final int tag;
    public final String value;

    public TaggedValue(int tag, String value) {
      this.tag = tag;
      this.value = value;
    }
  }
}
