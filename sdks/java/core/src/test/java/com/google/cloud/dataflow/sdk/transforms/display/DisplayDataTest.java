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

package com.google.cloud.dataflow.sdk.transforms.display;

import static com.google.cloud.dataflow.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static com.google.cloud.dataflow.sdk.transforms.display.DisplayDataMatchers.hasKey;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.display.DisplayData.Builder;
import com.google.cloud.dataflow.sdk.transforms.display.DisplayData.Item;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.testing.EqualsTester;

import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Tests for {@link DisplayData} class.
 */
@RunWith(JUnit4.class)
public class DisplayDataTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  private static final DateTimeFormatter ISO_FORMATTER = ISODateTimeFormat.dateTime();

  @Test
  public void testTypicalUsage() {
    final HasDisplayData subComponent1 =
        new HasDisplayData() {
          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder.add("ExpectedAnswer", 42);
          }
        };

    final HasDisplayData subComponent2 =
        new HasDisplayData() {
          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder.add("Location", "Seattle").add("Forecast", "Rain");
          }
        };

    PTransform<?, ?> transform =
        new PTransform<PCollection<String>, PCollection<String>>() {
          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder
                .include(subComponent1)
                .include(subComponent2)
                .add("MinSproggles", 200)
                .withLabel("Mimimum Required Sproggles")
                .add("LazerOrientation", "NORTH")
                .add("TimeBomb", Instant.now().plus(Duration.standardDays(1)))
                .add("FilterLogic", subComponent1.getClass())
                .add("ServiceUrl", "google.com/fizzbang")
                .withLinkUrl("http://www.google.com/fizzbang");
          }
        };

    DisplayData data = DisplayData.from(transform);

    assertThat(data.items(), not(empty()));
    assertThat(
        data.items(),
        everyItem(
            allOf(
                hasKey(not(isEmptyOrNullString())),
                hasNamespace(
                    Matchers.<Class<?>>isOneOf(
                        transform.getClass(), subComponent1.getClass(), subComponent2.getClass())),
                hasType(notNullValue(DisplayData.Type.class)),
                hasValue(not(isEmptyOrNullString())))));
  }

  @Test
  public void testDefaultInstance() {
    DisplayData none = DisplayData.none();
    assertThat(none.items(), empty());
  }

  @Test
  public void testCanBuild() {
    DisplayData data =
        DisplayData.from(new HasDisplayData() {
              @Override
              public void populateDisplayData(DisplayData.Builder builder) {
                builder.add("Foo", "bar");
              }
            });

    assertThat(data.items(), hasSize(1));
    assertThat(data, hasDisplayItem(hasKey("Foo")));
  }

  @Test
  public void testAsMap() {
    DisplayData data =
        DisplayData.from(
            new HasDisplayData() {
              @Override
              public void populateDisplayData(DisplayData.Builder builder) {
                builder.add("foo", "bar");
              }
            });

    Map<DisplayData.Identifier, DisplayData.Item> map = data.asMap();
    assertEquals(map.size(), 1);
    assertThat(data, hasDisplayItem(hasKey("foo")));
    assertEquals(map.values(), data.items());
  }

  @Test
  public void testItemProperties() {
    final Instant value = Instant.now();
    DisplayData data = DisplayData.from(new ConcreteComponent(value));

    @SuppressWarnings("unchecked")
    DisplayData.Item item = (DisplayData.Item) data.items().toArray()[0];
    assertThat(
        item,
        allOf(
            hasNamespace(Matchers.<Class<?>>is(ConcreteComponent.class)),
            hasKey("now"),
            hasType(is(DisplayData.Type.TIMESTAMP)),
            hasValue(is(ISO_FORMATTER.print(value))),
            hasShortValue(nullValue(String.class)),
            hasLabel(is("the current instant")),
            hasUrl(is("http://time.gov"))));
  }

  static class ConcreteComponent implements HasDisplayData {
    private Instant value;

    ConcreteComponent(Instant value) {
      this.value = value;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.add("now", value).withLabel("the current instant").withLinkUrl("http://time.gov");
    }
  }

  @Test
  public void testUnspecifiedOptionalProperties() {
    DisplayData data =
        DisplayData.from(
            new HasDisplayData() {
              @Override
              public void populateDisplayData(DisplayData.Builder builder) {
                builder.add("foo", "bar");
              }
            });

    assertThat(
        data,
        hasDisplayItem(allOf(hasLabel(nullValue(String.class)), hasUrl(nullValue(String.class)))));
  }

  @Test
  public void testIncludes() {
    final HasDisplayData subComponent =
        new HasDisplayData() {
          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder.add("foo", "bar");
          }
        };

    DisplayData data =
        DisplayData.from(
            new HasDisplayData() {
              @Override
              public void populateDisplayData(DisplayData.Builder builder) {
                builder.include(subComponent);
              }
            });

    assertThat(
        data,
        hasDisplayItem(
            allOf(
                hasKey("foo"),
                hasNamespace(Matchers.<Class<?>>is(subComponent.getClass())))));
  }

  @Test
  public void testIdentifierEquality() {
    new EqualsTester()
        .addEqualityGroup(
            DisplayData.Identifier.of(DisplayDataTest.class, "1"),
            DisplayData.Identifier.of(DisplayDataTest.class, "1"))
        .addEqualityGroup(DisplayData.Identifier.of(Object.class, "1"))
        .addEqualityGroup(DisplayData.Identifier.of(DisplayDataTest.class, "2"))
        .testEquals();
  }

  @Test
  public void testAnonymousClassNamespace() {
    DisplayData data =
        DisplayData.from(
            new HasDisplayData() {
              @Override
              public void populateDisplayData(DisplayData.Builder builder) {
                builder.add("foo", "bar");
              }
            });

    DisplayData.Item item = (DisplayData.Item) data.items().toArray()[0];
    final Pattern anonClassRegex = Pattern.compile(
        Pattern.quote(DisplayDataTest.class.getName()) + "\\$\\d+$");
    assertThat(item.getNamespace(), new CustomTypeSafeMatcher<String>(
        "anonymous class regex: " + anonClassRegex) {
      @Override
      protected boolean matchesSafely(String item) {
        java.util.regex.Matcher m = anonClassRegex.matcher(item);
        return m.matches();
      }
    });
  }

  @Test
  public void testAcceptsKeysWithDifferentNamespaces() {
    DisplayData data =
        DisplayData.from(
            new HasDisplayData() {
              @Override
              public void populateDisplayData(DisplayData.Builder builder) {
                builder
                    .add("foo", "bar")
                    .include(
                        new HasDisplayData() {
                          @Override
                          public void populateDisplayData(DisplayData.Builder builder) {
                            builder.add("foo", "bar");
                          }
                        });
              }
            });

    assertThat(data.items(), hasSize(2));
  }

  @Test
  public void testDuplicateKeyThrowsException() {
    thrown.expect(IllegalArgumentException.class);
    DisplayData.from(
        new HasDisplayData() {
          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder
              .add("foo", "bar")
              .add("foo", "baz");
          }
        });
  }

  @Test
  public void testToString() {
    HasDisplayData component = new HasDisplayData() {
      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder.add("foo", "bar");
      }
    };

    DisplayData data = DisplayData.from(component);
    assertEquals(String.format("%s:foo=bar", component.getClass().getName()), data.toString());
  }

  @Test
  public void testHandlesIncludeCycles() {

    final IncludeSubComponent componentA =
        new IncludeSubComponent() {
          @Override
          String getId() {
            return "componentA";
          }
        };
    final IncludeSubComponent componentB =
        new IncludeSubComponent() {
          @Override
          String getId() {
            return "componentB";
          }
        };

    HasDisplayData component =
        new HasDisplayData() {
          @Override
          public void populateDisplayData(Builder builder) {
            builder.include(componentA);
          }
        };

    componentA.subComponent = componentB;
    componentB.subComponent = componentA;

    DisplayData data = DisplayData.from(component);
    assertThat(data.items(), hasSize(2));
  }

  @Test
  public void testIncludesSubcomponentsWithObjectEquality() {
    DisplayData data = DisplayData.from(new HasDisplayData() {
      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder
          .include(new EqualsEverything("foo1", "bar1"))
          .include(new EqualsEverything("foo2", "bar2"));
      }
    });

    assertThat(data.items(), hasSize(2));
  }

  private static class EqualsEverything implements HasDisplayData {
    private final String value;
    private final String key;
    EqualsEverything(String key, String value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.add(key, value);
    }

    @Override
    public int hashCode() {
      return 1;
    }

    @Override
    public boolean equals(Object obj) {
      return true;
    }
  }

  abstract static class IncludeSubComponent implements HasDisplayData {
    HasDisplayData subComponent;

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.add("id", getId()).include(subComponent);
    }

    abstract String getId();
  }

  @Test
  public void testTypeMappings() {
    DisplayData data =
        DisplayData.from(
            new HasDisplayData() {
              @Override
              public void populateDisplayData(DisplayData.Builder builder) {
                builder
                    .add("string", "foobar")
                    .add("integer", 123)
                    .add("float", 3.14)
                    .add("java_class", DisplayDataTest.class)
                    .add("timestamp", Instant.now())
                    .add("duration", Duration.standardHours(1));
              }
            });

    Collection<Item> items = data.items();
    assertThat(
        items, hasItem(allOf(hasKey("string"), hasType(is(DisplayData.Type.STRING)))));
    assertThat(
        items, hasItem(allOf(hasKey("integer"), hasType(is(DisplayData.Type.INTEGER)))));
    assertThat(items, hasItem(allOf(hasKey("float"), hasType(is(DisplayData.Type.FLOAT)))));
    assertThat(
        items,
        hasItem(allOf(hasKey("java_class"), hasType(is(DisplayData.Type.JAVA_CLASS)))));
    assertThat(
        items,
        hasItem(allOf(hasKey("timestamp"), hasType(is(DisplayData.Type.TIMESTAMP)))));
    assertThat(
        items, hasItem(allOf(hasKey("duration"), hasType(is(DisplayData.Type.DURATION)))));
  }

  @Test
  public void testStringFormatting() throws IOException {
    final Instant now = Instant.now();
    final Duration oneHour = Duration.standardHours(1);

    HasDisplayData component = new HasDisplayData() {
      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder
          .add("string", "foobar")
          .add("integer", 123)
          .add("float", 3.14)
          .add("java_class", DisplayDataTest.class)
          .add("timestamp", now)
          .add("duration", oneHour);
      }
    };
    DisplayData data = DisplayData.from(component);

    Collection<Item> items = data.items();
    assertThat(items, hasItem(allOf(hasKey("string"), hasValue(is("foobar")))));
    assertThat(items, hasItem(allOf(hasKey("integer"), hasValue(is("123")))));
    assertThat(items, hasItem(allOf(hasKey("float"), hasValue(is("3.14")))));
    assertThat(items, hasItem(allOf(hasKey("java_class"),
            hasValue(is(DisplayDataTest.class.getName())),
            hasShortValue(is(DisplayDataTest.class.getSimpleName())))));
    assertThat(items, hasItem(allOf(hasKey("timestamp"),
            hasValue(is(ISO_FORMATTER.print(now))))));
    assertThat(items, hasItem(allOf(hasKey("duration"),
            hasValue(is(Long.toString(oneHour.getMillis()))))));
  }

  @Test
  public void testContextProperlyReset() {
    final HasDisplayData subComponent =
        new HasDisplayData() {
          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder.add("foo", "bar");
          }
        };

    HasDisplayData component =
        new HasDisplayData() {
          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder
              .include(subComponent)
              .add("alpha", "bravo");
          }
        };

    DisplayData data = DisplayData.from(component);
    assertThat(
        data.items(),
        hasItem(
            allOf(
                hasKey("alpha"),
                hasNamespace(Matchers.<Class<?>>is(component.getClass())))));
  }

  @Test
  public void testFromNull() {
    thrown.expect(NullPointerException.class);
    DisplayData.from(null);
  }

  @Test
  public void testIncludeNull() {
    thrown.expect(NullPointerException.class);
    DisplayData.from(
        new HasDisplayData() {
          @Override
          public void populateDisplayData(Builder builder) {
            builder.include(null);
          }
        });
  }

  @Test
  public void testNullKey() {
    thrown.expect(NullPointerException.class);
    DisplayData.from(
        new HasDisplayData() {
          @Override
          public void populateDisplayData(Builder builder) {
            builder.add(null, "foo");
          }
        });
  }

  @Test
  public void testRejectsNullValues() {
    DisplayData.from(
      new HasDisplayData() {
        @Override
        public void populateDisplayData(Builder builder) {
          try {
            builder.add("key", (String) null);
            throw new RuntimeException("Should throw on null string value");
          } catch (NullPointerException ex) {
            // Expected
          }

          try {
            builder.add("key", (Class<?>) null);
            throw new RuntimeException("Should throw on null class value");
          } catch (NullPointerException ex) {
            // Expected
          }

          try {
            builder.add("key", (Duration) null);
            throw new RuntimeException("Should throw on null duration value");
          } catch (NullPointerException ex) {
            // Expected
          }

          try {
            builder.add("key", (Instant) null);
            throw new RuntimeException("Should throw on null instant value");
          } catch (NullPointerException ex) {
            // Expected
          }
        }
      });
  }

  public void testAcceptsNullOptionalValues() {
    DisplayData.from(
      new HasDisplayData() {
        @Override
        public void populateDisplayData(Builder builder) {
          builder.add("key", "value")
                  .withLabel(null)
                  .withLinkUrl(null);
        }
      });

    // Should not throw
  }

  private static Matcher<DisplayData.Item> hasNamespace(Matcher<Class<?>> nsMatcher) {
    return new FeatureMatcher<DisplayData.Item, Class<?>>(
        nsMatcher, "display item with namespace", "namespace") {
      @Override
      protected Class<?> featureValueOf(DisplayData.Item actual) {
        try {
          return Class.forName(actual.getNamespace());
        } catch (ClassNotFoundException e) {
          return null;
        }
      }
    };
  }

  private static Matcher<DisplayData.Item> hasType(Matcher<DisplayData.Type> typeMatcher) {
    return new FeatureMatcher<DisplayData.Item, DisplayData.Type>(
        typeMatcher, "display item with type", "type") {
      @Override
      protected DisplayData.Type featureValueOf(DisplayData.Item actual) {
        return actual.getType();
      }
    };
  }

  private static Matcher<DisplayData.Item> hasLabel(Matcher<String> labelMatcher) {
    return new FeatureMatcher<DisplayData.Item, String>(
        labelMatcher, "display item with label", "label") {
      @Override
      protected String featureValueOf(DisplayData.Item actual) {
        return actual.getLabel();
      }
    };
  }

  private static Matcher<DisplayData.Item> hasUrl(Matcher<String> urlMatcher) {
    return new FeatureMatcher<DisplayData.Item, String>(
        urlMatcher, "display item with url", "URL") {
      @Override
      protected String featureValueOf(DisplayData.Item actual) {
        return actual.getLinkUrl();
      }
    };
  }

  private static Matcher<DisplayData.Item> hasValue(Matcher<String> valueMatcher) {
    return new FeatureMatcher<DisplayData.Item, String>(
        valueMatcher, "display item with value", "value") {
      @Override
      protected String featureValueOf(DisplayData.Item actual) {
        return actual.getValue();
      }
    };
  }

  private static Matcher<DisplayData.Item> hasShortValue(Matcher<String> valueStringMatcher) {
    return new FeatureMatcher<DisplayData.Item, String>(
        valueStringMatcher, "display item with short value", "short value") {
      @Override
      protected String featureValueOf(DisplayData.Item actual) {
        return actual.getShortValue();
      }
    };
  }
}
