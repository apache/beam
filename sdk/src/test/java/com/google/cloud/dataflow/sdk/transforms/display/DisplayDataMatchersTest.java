/*
 * Copyright (C) 2016 Google Inc.
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

package com.google.cloud.dataflow.sdk.transforms.display;

import static com.google.cloud.dataflow.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static com.google.cloud.dataflow.sdk.transforms.display.DisplayDataMatchers.hasKey;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.display.DisplayData.Builder;
import com.google.cloud.dataflow.sdk.values.PCollection;

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
    assertTrue(matcher.matches(createDisplayDataWithItem("foo", "bar")));
  }

  @Test
  public void testHasDisplayItemDescription() {
    Matcher<DisplayData> matcher = hasDisplayItem();
    Description desc = new StringDescription();
    Description mismatchDesc = new StringDescription();

    matcher.describeTo(desc);
    matcher.describeMismatch(DisplayData.none(), mismatchDesc);

    assertThat(desc.toString(), startsWith("display data with item: "));
    assertThat(mismatchDesc.toString(), containsString("found 0 non-matching items"));
  }

  @Test
  public void testHasKey() {
    Matcher<DisplayData> matcher = hasDisplayItem(hasKey("foo"));

    assertTrue(matcher.matches(createDisplayDataWithItem("foo", "bar")));
    assertFalse(matcher.matches(createDisplayDataWithItem("fooz", "bar")));
  }

  private DisplayData createDisplayDataWithItem(final String key, final String value) {
    return DisplayData.from(
        new PTransform<PCollection<String>, PCollection<String>>() {
          @Override
          public void populateDisplayData(Builder builder) {
            builder.add(key, value);
          }
        });
  }
}
