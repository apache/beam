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

import com.google.cloud.dataflow.sdk.transforms.display.DisplayData.Item;

import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.Collection;

/**
 * Hamcrest matcher for making assertions on {@link DisplayData} instances.
 */
public class DisplayDataMatchers {
  /**
   * Do not instantiate.
   */
  private DisplayDataMatchers() {}

  /**
   * Creates a matcher that matches if the examined {@link DisplayData} contains any items.
   */
  public static Matcher<DisplayData> hasDisplayItem() {
    return hasDisplayItem(Matchers.any(DisplayData.Item.class));
  }

  /**
   * Creates a matcher that matches if the examined {@link DisplayData} contains any item
   * matching the specified {@code itemMatcher}.
   */
  public static Matcher<DisplayData> hasDisplayItem(Matcher<DisplayData.Item> itemMatcher) {
    return new HasDisplayDataItemMatcher(itemMatcher);
  }

  private static class HasDisplayDataItemMatcher extends TypeSafeDiagnosingMatcher<DisplayData> {
    private final Matcher<Item> itemMatcher;

    private HasDisplayDataItemMatcher(Matcher<DisplayData.Item> itemMatcher) {
      this.itemMatcher = itemMatcher;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("display data with item: ");
      itemMatcher.describeTo(description);
    }

    @Override
    protected boolean matchesSafely(DisplayData data, Description mismatchDescription) {
      Collection<Item> items = data.items();
      boolean isMatch = Matchers.hasItem(itemMatcher).matches(items);
      if (!isMatch) {
        mismatchDescription.appendText("found " + items.size() + " non-matching items");
      }

      return isMatch;
    }
  }

  /**
   * Creates a matcher that matches if the examined {@link DisplayData.Item} contains a key
   * with the specified value.
   */
  public static Matcher<DisplayData.Item> hasKey(String key) {
    return hasKey(Matchers.is(key));
  }

  /**
   * Creates a matcher that matches if the examined {@link DisplayData.Item} contains a key
   * matching the specified key matcher.
   */
  public static Matcher<DisplayData.Item> hasKey(Matcher<String> keyMatcher) {
    return new FeatureMatcher<DisplayData.Item, String>(keyMatcher, "with key", "key") {
      @Override
      protected String featureValueOf(DisplayData.Item actual) {
        return actual.getKey();
      }
    };
  }
}
