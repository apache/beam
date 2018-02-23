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
package org.apache.beam.sdk.transforms.display;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasNamespace;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasPath;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasType;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasValue;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.includesDisplayDataFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link DisplayDataMatchers}.
 */
@RunWith(JUnit4.class)
public class DisplayDataMatchersTest {
  @Test
  public void testHasDisplayItem() {
    Matcher<DisplayData> matcher = hasDisplayItem();

    assertFalse(matcher.matches(DisplayData.none()));
    assertThat(createDisplayDataWithItem("foo", "bar"), matcher);
  }

  @Test
  public void testHasDisplayItemDescription() {
    Matcher<DisplayData> matcher = hasDisplayItem();
    Description desc = new StringDescription();
    Description mismatchDesc = new StringDescription();

    matcher.describeTo(desc);
    matcher.describeMismatch(DisplayData.none(), mismatchDesc);

    assertEquals("DisplayData not an empty collection", desc.toString());
    assertEquals("DisplayData was <[]>", mismatchDesc.toString());
  }

  @Test
  public void testHasKey() {
    Matcher<DisplayData> matcher = hasDisplayItem("foo");

    assertFalse(matcher.matches(createDisplayDataWithItem("fooz", "bar")));

    assertThat(createDisplayDataWithItem("foo", "bar"), matcher);
  }

  @Test
  public void testHasType() {
    Matcher<DisplayData> matcher = hasDisplayItem(hasType(DisplayData.Type.JAVA_CLASS));

    DisplayData data = DisplayData.from(new PTransform<PCollection<String>, PCollection<String>>() {
      @Override
      public PCollection<String> expand(PCollection<String> input) {
        throw new IllegalArgumentException("Should never be applied");
      }

      @Override
      public void populateDisplayData(Builder builder) {
        builder.add(DisplayData.item("foo", DisplayDataMatchersTest.class));
      }
    });

    assertFalse(matcher.matches(createDisplayDataWithItem("fooz", "bar")));
    assertThat(data, matcher);
  }

  @Test
  public void testHasValue() {
    Matcher<DisplayData> matcher = hasDisplayItem(hasValue("bar"));

    assertFalse(matcher.matches(createDisplayDataWithItem("foo", "baz")));
    assertThat(createDisplayDataWithItem("foo", "bar"), matcher);
  }

  @Test
  public void testHasPath() {
    Matcher<DisplayData> matcher = hasDisplayItem(hasPath("a", "b"));

    final HasDisplayData subComponent =
        builder ->
            builder.include(
                "b",
                (HasDisplayData)
                    builder1 -> {
                      builder1.add(DisplayData.item("foo", "bar"));
                    });

    assertFalse(matcher.matches(DisplayData.from(subComponent)));

    assertThat(DisplayData.from(builder -> builder.include("a", subComponent)), matcher);
  }

  @Test
  public void testHasNamespace() {
    Matcher<DisplayData> matcher = hasDisplayItem(hasNamespace(SampleTransform.class));

    assertFalse(matcher.matches(DisplayData.from(
        new PTransform<PCollection<String>, PCollection<String>>() {
          @Override
          public PCollection<String> expand(PCollection<String> input) {
            throw new IllegalArgumentException("Should never be applied");
          }
        })));
    assertThat(createDisplayDataWithItem("foo", "bar"), matcher);
  }

  @Test
  public void testIncludes() {
    final HasDisplayData subComponent = builder -> builder.add(DisplayData.item("foo", "bar"));
    HasDisplayData hasSubcomponent = builder -> builder.include("p", subComponent);

    HasDisplayData wrongPath = builder -> builder.include("q", subComponent);

    HasDisplayData deeplyNested =
        builder -> builder.include("p", builder1 -> builder1.include("p", subComponent));

    HasDisplayData sameDisplayItemDifferentComponent =
        builder -> builder.add(DisplayData.item("foo", "bar"));

    Matcher<DisplayData> matcher = includesDisplayDataFor("p", subComponent);

    assertFalse("should not match sub-component at different path",
        matcher.matches(DisplayData.from(wrongPath)));
    assertFalse("should not match deeply nested sub-component",
        matcher.matches(DisplayData.from(deeplyNested)));
    assertFalse("should not match identical display data from different component",
        matcher.matches(DisplayData.from(sameDisplayItemDifferentComponent)));
    assertThat(DisplayData.from(hasSubcomponent), matcher);
  }

  private DisplayData createDisplayDataWithItem(final String key, final String value) {
    return DisplayData.from(new SampleTransform(key, value));
  }

  static class SampleTransform extends PTransform<PCollection<String>, PCollection<String>> {
    private final String key;
    private final String value;

    SampleTransform(String key, String value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
      throw new IllegalArgumentException("Should never be applied");
    }

    @Override
    public void populateDisplayData(Builder builder) {
      builder.add(DisplayData.item(key, value));
    }
  }
}
