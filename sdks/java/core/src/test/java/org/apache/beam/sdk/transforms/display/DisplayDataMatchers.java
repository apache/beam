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

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.Collection;
import org.apache.beam.sdk.transforms.display.DisplayData.Item;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** Hamcrest matcher for making assertions on {@link DisplayData} instances. */
public class DisplayDataMatchers {
  /** Do not instantiate. */
  private DisplayDataMatchers() {}

  /** Creates a matcher that matches if the examined {@link DisplayData} contains any items. */
  public static Matcher<DisplayData> hasDisplayItem() {
    return new FeatureMatcher<DisplayData, Collection<DisplayData.Item>>(
        Matchers.not(Matchers.empty()), "DisplayData", "DisplayData") {
      @Override
      protected Collection<Item> featureValueOf(DisplayData actual) {
        return actual.items();
      }
    };
  }

  /**
   * Creates a matcher that matches if the examined {@link DisplayData} contains an item with the
   * specified key.
   */
  public static Matcher<DisplayData> hasDisplayItem(String key) {
    return hasDisplayItem(hasKey(key));
  }

  /**
   * Create a matcher that matches if the examined {@link DisplayData} contains an item with the
   * specified key and String value.
   */
  public static Matcher<DisplayData> hasDisplayItem(String key, String value) {
    return hasDisplayItem(key, DisplayData.Type.STRING, value);
  }

  /**
   * Create a matcher that matches if the examined {@link DisplayData} contains an item with the
   * specified key and Boolean value.
   */
  public static Matcher<DisplayData> hasDisplayItem(String key, Boolean value) {
    return hasDisplayItem(key, DisplayData.Type.BOOLEAN, value);
  }

  /**
   * Create a matcher that matches if the examined {@link DisplayData} contains an item with the
   * specified key and Duration value.
   */
  public static Matcher<DisplayData> hasDisplayItem(String key, Duration value) {
    return hasDisplayItem(key, DisplayData.Type.DURATION, value);
  }

  /**
   * Create a matcher that matches if the examined {@link DisplayData} contains an item with the
   * specified key and Float value.
   */
  public static Matcher<DisplayData> hasDisplayItem(String key, double value) {
    return hasDisplayItem(key, DisplayData.Type.FLOAT, value);
  }

  /**
   * Create a matcher that matches if the examined {@link DisplayData} contains an item with the
   * specified key and Integer value.
   */
  public static Matcher<DisplayData> hasDisplayItem(String key, long value) {
    return hasDisplayItem(key, DisplayData.Type.INTEGER, value);
  }

  /**
   * Create a matcher that matches if the examined {@link DisplayData} contains an item with the
   * specified key and Class value.
   */
  public static Matcher<DisplayData> hasDisplayItem(String key, Class<?> value) {
    return hasDisplayItem(key, DisplayData.Type.JAVA_CLASS, value);
  }

  /**
   * Create a matcher that matches if the examined {@link DisplayData} contains an item with the
   * specified key and Timestamp value.
   */
  public static Matcher<DisplayData> hasDisplayItem(String key, Instant value) {
    return hasDisplayItem(key, DisplayData.Type.TIMESTAMP, value);
  }

  private static Matcher<DisplayData> hasDisplayItem(
      String key, DisplayData.Type type, Object value) {
    DisplayData.FormattedItemValue formattedValue = type.format(value);
    return hasDisplayItem(
        allOf(hasKey(key), hasType(type), hasValue(formattedValue.getLongValue())));
  }

  /**
   * Creates a matcher that matches if the examined {@link DisplayData} contains any item matching
   * the specified {@code itemMatcher}.
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
        mismatchDescription.appendText("found " + items.size() + " non-matching item(s):\n");
        mismatchDescription.appendValue(data);
      }

      return isMatch;
    }
  }

  /**
   * Create a matcher that matches if the examined {@link DisplayData} contains all display data
   * registered from the specified subcomponent and namespace.
   */
  public static Matcher<DisplayData> includesDisplayDataFor(
      final String path, final HasDisplayData subComponent) {
    return new CustomTypeSafeMatcher<DisplayData>("includes subcomponent") {
      @Override
      protected boolean matchesSafely(DisplayData displayData) {
        DisplayData subComponentData = subComponentData(path);
        if (subComponentData.items().isEmpty()) {
          throw new UnsupportedOperationException(
              "subComponent contains no display data; " + "cannot verify whether it is included");
        }

        DisplayDataComparison comparison = checkSubset(displayData, subComponentData, path);
        return comparison.missingItems.isEmpty();
      }

      @Override
      protected void describeMismatchSafely(
          DisplayData displayData, Description mismatchDescription) {
        DisplayData subComponentDisplayData = subComponentData(path);
        DisplayDataComparison comparison = checkSubset(displayData, subComponentDisplayData, path);

        mismatchDescription
            .appendText("did not include:\n")
            .appendValue(comparison.missingItems)
            .appendText("\nNon-matching items:\n")
            .appendValue(comparison.unmatchedItems);
      }

      private DisplayData subComponentData(final String path) {
        return DisplayData.from(builder -> builder.include(path, subComponent));
      }

      private DisplayDataComparison checkSubset(
          DisplayData displayData, DisplayData included, String path) {
        DisplayDataComparison comparison = new DisplayDataComparison(displayData.items());
        for (Item item : included.items()) {
          Item matchedItem =
              displayData
                  .asMap()
                  .get(
                      DisplayData.Identifier.of(
                          DisplayData.Path.absolute(path), item.getNamespace(), item.getKey()));

          if (matchedItem != null) {
            comparison.matched(matchedItem);
          } else {
            comparison.missing(item);
          }
        }

        return comparison;
      }

      class DisplayDataComparison {
        Collection<Item> missingItems;
        Collection<Item> unmatchedItems;

        DisplayDataComparison(Collection<Item> superset) {
          missingItems = Sets.newHashSet();
          unmatchedItems = Sets.newHashSet(superset);
        }

        void matched(Item supersetItem) {
          unmatchedItems.remove(supersetItem);
        }

        void missing(Item subsetItem) {
          missingItems.add(subsetItem);
        }
      }
    };
  }

  /**
   * Creates a matcher that matches if the examined {@link DisplayData.Item} contains a key with the
   * specified value.
   */
  public static Matcher<DisplayData.Item> hasKey(String key) {
    return hasKey(is(key));
  }

  /**
   * Creates a matcher that matches if the examined {@link DisplayData.Item} contains a key matching
   * the specified key matcher.
   */
  public static Matcher<DisplayData.Item> hasKey(Matcher<String> keyMatcher) {
    return new FeatureMatcher<DisplayData.Item, String>(keyMatcher, "with key", "key") {
      @Override
      protected String featureValueOf(DisplayData.Item actual) {
        return actual.getKey();
      }
    };
  }

  /**
   * Creates a matcher that matches if the examined {@link DisplayData.Item} contains a path
   * matching the specified namespace.
   */
  public static Matcher<DisplayData.Item> hasPath(String... paths) {
    DisplayData.Path path =
        (paths.length == 0)
            ? DisplayData.Path.root()
            : DisplayData.Path.absolute(paths[0], Arrays.copyOfRange(paths, 1, paths.length));
    return new FeatureMatcher<DisplayData.Item, DisplayData.Path>(
        is(path), " with namespace", "namespace") {
      @Override
      protected DisplayData.Path featureValueOf(DisplayData.Item actual) {
        return actual.getPath();
      }
    };
  }

  /**
   * Creates a matcher that matches if the examined {@link DisplayData.Item} contains the specified
   * namespace.
   */
  public static Matcher<DisplayData.Item> hasNamespace(Class<?> namespace) {
    return hasNamespace(Matchers.<Class<?>>is(namespace));
  }

  /**
   * Creates a matcher that matches if the examined {@link DisplayData.Item} contains a namespace
   * matching the specified namespace matcher.
   */
  public static Matcher<DisplayData.Item> hasNamespace(Matcher<Class<?>> namespaceMatcher) {
    return new FeatureMatcher<DisplayData.Item, Class<?>>(
        namespaceMatcher, " with namespace", "namespace") {
      @Override
      protected Class<?> featureValueOf(DisplayData.Item actual) {
        return actual.getNamespace();
      }
    };
  }

  /**
   * Creates a matcher that matches if the examined {@link DisplayData.Item} matches the specified
   * type.
   */
  public static Matcher<DisplayData.Item> hasType(DisplayData.Type type) {
    return hasType(is(type));
  }

  /**
   * Creates a matcher that matches if the examined {@link DisplayData.Item} has a type matching the
   * specified type matcher.
   */
  public static Matcher<DisplayData.Item> hasType(Matcher<DisplayData.Type> typeMatcher) {
    return new FeatureMatcher<DisplayData.Item, DisplayData.Type>(
        typeMatcher, "with type", "type") {
      @Override
      protected DisplayData.Type featureValueOf(DisplayData.Item actual) {
        return actual.getType();
      }
    };
  }

  /**
   * Creates a matcher that matches if the examined {@link DisplayData.Item} has the specified
   * value.
   */
  public static Matcher<DisplayData.Item> hasValue(Object value) {
    return hasValue(is(value));
  }

  /**
   * Creates a matcher that matches if the examined {@link DisplayData.Item} contains a value
   * matching the specified value matcher.
   */
  public static <T> Matcher<DisplayData.Item> hasValue(Matcher<T> valueMatcher) {
    return new FeatureMatcher<DisplayData.Item, T>(valueMatcher, "with value", "value") {
      @Override
      protected T featureValueOf(DisplayData.Item actual) {
        @SuppressWarnings("unchecked")
        T value = (T) actual.getValue();
        return value;
      }
    };
  }

  /**
   * Creates a matcher that matches if the examined {@link DisplayData.Item} has the specified
   * label.
   */
  public static Matcher<DisplayData.Item> hasLabel(String label) {
    return hasLabel(is(label));
  }

  /**
   * Creates a matcher that matches if the examined {@link DisplayData.Item} has a label matching
   * the specified label matcher.
   */
  public static Matcher<DisplayData.Item> hasLabel(Matcher<String> labelMatcher) {
    return new FeatureMatcher<DisplayData.Item, String>(
        labelMatcher, "display item with label", "label") {
      @Override
      protected String featureValueOf(DisplayData.Item actual) {
        return actual.getLabel();
      }
    };
  }
}
