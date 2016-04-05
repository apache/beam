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
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasKey;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasNamespace;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasType;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasValue;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.includes;

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
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.transforms.display.DisplayData.Item;
import org.apache.beam.sdk.values.PCollection;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.testing.EqualsTester;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
  private static final ObjectMapper MAPPER = new ObjectMapper();

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
          final Instant defaultStartTime = new Instant(0);
          Instant startTime = defaultStartTime;

          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder
                .include(subComponent1)
                .include(subComponent2)
                .add("minSproggles", 200)
                  .withLabel("Mimimum Required Sproggles")
                .add("fireLazers", true)
                .addIfNotDefault("startTime", startTime, defaultStartTime)
                .add("timeBomb", Instant.now().plus(Duration.standardDays(1)))
                .add("filterLogic", subComponent1.getClass())
                .add("serviceUrl", "google.com/fizzbang")
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
                builder.add("foo", "bar");
              }
            });

    assertThat(data.items(), hasSize(1));
    assertThat(data, hasDisplayItem("foo", "bar"));
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
    assertThat(data, hasDisplayItem("foo", "bar"));
    assertEquals(map.values(), data.items());
  }

  @Test
  public void testItemProperties() {
    final Instant value = Instant.now();
    DisplayData data = DisplayData.from(new HasDisplayData() {
      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder.add("now", value)
            .withLabel("the current instant")
            .withLinkUrl("http://time.gov")
            .withNamespace(DisplayDataTest.class);
      }
    });

    @SuppressWarnings("unchecked")
    DisplayData.Item item = (DisplayData.Item) data.items().toArray()[0];
    assertThat(
        item,
        Matchers.allOf(
            hasNamespace(DisplayDataTest.class),
            hasKey("now"),
            hasType(DisplayData.Type.TIMESTAMP),
            hasValue(ISO_FORMATTER.print(value)),
            hasShortValue(nullValue(String.class)),
            hasLabel(is("the current instant")),
            hasUrl(is("http://time.gov"))));
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
  public void testAddIfNotDefault() {
    DisplayData data = DisplayData.from(new HasDisplayData() {
      @Override
      public void populateDisplayData(Builder builder) {
        builder
            .addIfNotDefault("defaultString", "foo", "foo")
            .addIfNotDefault("notDefaultString", "foo", "notFoo")
            .addIfNotDefault("defaultInteger", 1, 1)
            .addIfNotDefault("notDefaultInteger", 1, 2)
            .addIfNotDefault("defaultDouble", 123.4, 123.4)
            .addIfNotDefault("notDefaultDouble", 123.4, 234.5)
            .addIfNotDefault("defaultBoolean", true, true)
            .addIfNotDefault("notDefaultBoolean", true, false)
            .addIfNotDefault("defaultInstant", new Instant(0), new Instant(0))
            .addIfNotDefault("notDefaultInstant", new Instant(0), Instant.now())
            .addIfNotDefault("defaultDuration", Duration.ZERO, Duration.ZERO)
            .addIfNotDefault("notDefaultDuration", Duration.millis(1234), Duration.ZERO)
            .addIfNotDefault("defaultClass", DisplayDataTest.class, DisplayDataTest.class)
            .addIfNotDefault("notDefaultClass", DisplayDataTest.class, null);
      }
    });

    assertThat(data.items(), hasSize(7));
    assertThat(data.items(), everyItem(hasKey(startsWith("notDefault"))));
  }

  @Test
  public void testAddIfNotNull() {
    DisplayData data = DisplayData.from(new HasDisplayData() {
      @Override
      public void populateDisplayData(Builder builder) {
        builder
            .addIfNotNull("nullString", (String) null)
            .addIfNotNull("notNullString", "foo")
            .addIfNotNull("nullLong", (Long) null)
            .addIfNotNull("notNullLong", 1234L)
            .addIfNotNull("nullDouble", (Double) null)
            .addIfNotNull("notNullDouble", 123.4)
            .addIfNotNull("nullBoolean", (Boolean) null)
            .addIfNotNull("notNullBoolean", true)
            .addIfNotNull("nullInstant", (Instant) null)
            .addIfNotNull("notNullInstant", Instant.now())
            .addIfNotNull("nullDuration", (Duration) null)
            .addIfNotNull("notNullDuration", Duration.ZERO)
            .addIfNotNull("nullClass", (Class<?>) null)
            .addIfNotNull("notNullClass", DisplayDataTest.class);
      }
    });

    assertThat(data.items(), hasSize(7));
    assertThat(data.items(), everyItem(hasKey(startsWith("notNull"))));
  }

  @Test
  public void testModifyingConditionalItemIsSafe() {
    HasDisplayData component = new HasDisplayData() {
      @Override
      public void populateDisplayData(Builder builder) {
        builder.addIfNotNull("nullItem", (Class<?>) null)
            .withLinkUrl("http://abc")
            .withNamespace(DisplayDataTest.class)
            .withLabel("Null item shoudl be safe");
      }
    };

    DisplayData.from(component); // should not throw
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

    assertThat(data, includes(subComponent));
  }

  @Test
  public void testIncludesNamespaceOverride() {
    final HasDisplayData subComponent = new HasDisplayData() {
        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
          builder.add("foo", "bar");
        }
    };

    final HasDisplayData namespaceOverride = new HasDisplayData(){
      @Override
      public void populateDisplayData(Builder builder) {
      }
    };

    DisplayData data = DisplayData.from(new HasDisplayData() {
      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder.include(subComponent, namespaceOverride.getClass());
      }
    });

    assertThat(data, includes(subComponent, namespaceOverride.getClass()));
  }

  @Test
  public void testNullNamespaceOverride() {
    thrown.expect(NullPointerException.class);

    DisplayData.from(new HasDisplayData() {
      @Override
      public void populateDisplayData(Builder builder) {
        builder.add("foo", "bar")
            .withNamespace((Class<?>) null);
      }
    });
  }

  @Test
  public void testIdentifierEquality() {
    new EqualsTester()
        .addEqualityGroup(
            DisplayData.Identifier.of(ClassForDisplay.of(DisplayDataTest.class), "1"),
            DisplayData.Identifier.of(ClassForDisplay.of(DisplayDataTest.class), "1"))
        .addEqualityGroup(DisplayData.Identifier.of(ClassForDisplay.of(Object.class), "1"))
        .addEqualityGroup(DisplayData.Identifier.of(ClassForDisplay.of(DisplayDataTest.class), "2"))
        .testEquals();
  }

  @Test
  public void testItemEquality() {
    HasDisplayData component1 = new HasDisplayData() {
      @Override
      public void populateDisplayData(Builder builder) {
        builder.add("foo", "bar");
      }
    };
    HasDisplayData component2 = new HasDisplayData() {
      @Override
      public void populateDisplayData(Builder builder) {
        builder.add("foo", "bar");
      }
    };

    DisplayData component1DisplayData1 = DisplayData.from(component1);
    DisplayData component1DisplayData2 = DisplayData.from(component1);
    DisplayData component2DisplayData = DisplayData.from(component2);

    new EqualsTester()
        .addEqualityGroup(
            component1DisplayData1.items().toArray()[0],
            component1DisplayData2.items().toArray()[0])
        .addEqualityGroup(component2DisplayData.items().toArray()[0])
        .testEquals();
  }

  @Test
  public void testDisplayDataEquality() {
    HasDisplayData component1 = new HasDisplayData() {
      @Override
      public void populateDisplayData(Builder builder) {
        builder.add("foo", "bar");
      }
    };
    HasDisplayData component2 = new HasDisplayData() {
      @Override
      public void populateDisplayData(Builder builder) {
        builder.add("foo", "bar");
      }
    };

    DisplayData component1DisplayData1 = DisplayData.from(component1);
    DisplayData component1DisplayData2 = DisplayData.from(component1);
    DisplayData component2DisplayData = DisplayData.from(component2);

    new EqualsTester()
        .addEqualityGroup(component1DisplayData1, component1DisplayData2)
        .addEqualityGroup(component2DisplayData)
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
  public void testDuplicateKeyWithNamespaceOverrideDoesntThrow() {
    DisplayData displayData = DisplayData.from(
        new HasDisplayData() {
          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder
                .add("foo", "bar")
                .add("foo", "baz")
                  .withNamespace(DisplayDataTest.class);
          }
        });

    assertThat(displayData.items(), hasSize(2));
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
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
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
                    .add("boolean", true)
                    .add("java_class", DisplayDataTest.class)
                    .add("java_class2", ClassForDisplay.of(DisplayDataTest.class))
                    .add("timestamp", Instant.now())
                    .add("duration", Duration.standardHours(1));
              }
            });

    Collection<Item> items = data.items();
    assertThat(
        items, hasItem(allOf(hasKey("string"), hasType(DisplayData.Type.STRING))));
    assertThat(
        items, hasItem(allOf(hasKey("integer"), hasType(DisplayData.Type.INTEGER))));
    assertThat(items, hasItem(allOf(hasKey("float"), hasType(DisplayData.Type.FLOAT))));
    assertThat(items, hasItem(allOf(hasKey("boolean"), hasType(DisplayData.Type.BOOLEAN))));
    assertThat(
        items,
        hasItem(allOf(hasKey("java_class"), hasType(DisplayData.Type.JAVA_CLASS))));
    assertThat(
        items,
        hasItem(allOf(hasKey("java_class2"), hasType(DisplayData.Type.JAVA_CLASS))));
    assertThat(
        items,
        hasItem(allOf(hasKey("timestamp"), hasType(DisplayData.Type.TIMESTAMP))));
    assertThat(
        items, hasItem(allOf(hasKey("duration"), hasType(DisplayData.Type.DURATION))));
  }

  @Test
  public void testExplicitItemType() {
    DisplayData data = DisplayData.from(new HasDisplayData() {
      @Override
      public void populateDisplayData(Builder builder) {
        builder
            .add("integer", DisplayData.Type.INTEGER, 1234L)
            .add("string", DisplayData.Type.STRING, "foobar");
      }
    });

    assertThat(data, hasDisplayItem("integer", 1234L));
    assertThat(data, hasDisplayItem("string", "foobar"));
  }

  @Test
  public void testFormatIncompatibleTypes() {
    Map<DisplayData.Type, Object> invalidPairs = ImmutableMap.<DisplayData.Type, Object>builder()
        .put(DisplayData.Type.STRING, 1234)
        .put(DisplayData.Type.INTEGER, "string value")
        .put(DisplayData.Type.FLOAT, "string value")
        .put(DisplayData.Type.BOOLEAN, "string value")
        .put(DisplayData.Type.TIMESTAMP, "string value")
        .put(DisplayData.Type.DURATION, "string value")
        .put(DisplayData.Type.JAVA_CLASS, "string value")
        .build();

    for (Map.Entry<DisplayData.Type, Object> pair : invalidPairs.entrySet()) {
      try {
        DisplayData.Type type = pair.getKey();
        Object invalidValue = pair.getValue();

        type.format(invalidValue);
        fail(String.format(
            "Expected exception not thrown for invalid %s value: %s", type, invalidValue));
      } catch (ClassCastException e) {
        // Expected
      }
    }
  }

  @Test
  public void testFormatCompatibleTypes() {
    Multimap<DisplayData.Type, Object> validPairs = ImmutableMultimap
        .<DisplayData.Type, Object>builder()
        .put(DisplayData.Type.INTEGER, 1234)
        .put(DisplayData.Type.INTEGER, 1234L)
        .put(DisplayData.Type.FLOAT, 123.4f)
        .put(DisplayData.Type.FLOAT, 123.4)
        .put(DisplayData.Type.FLOAT, 1234)
        .put(DisplayData.Type.FLOAT, 1234L)
        .build();

    for (Map.Entry<DisplayData.Type, Object> pair : validPairs.entries()) {
      DisplayData.Type type = pair.getKey();
      Object value = pair.getValue();

      try {
        type.format(value);
      } catch (ClassCastException e) {
        fail(String.format("Failed to format %s for DisplayData.%s",
            value.getClass().getSimpleName(), type));
      }
    }
  }

  @Test
  public void testInvalidExplicitItemType() {
    HasDisplayData component = new HasDisplayData() {
      @Override
      public void populateDisplayData(Builder builder) {
        builder.add("integer", DisplayData.Type.INTEGER, "foobar");
      }
    };

    thrown.expect(ClassCastException.class);
    DisplayData.from(component);
  }

  @Test
  public void testKnownTypeInference() {
    assertEquals(DisplayData.Type.INTEGER, DisplayData.inferType(1234));
    assertEquals(DisplayData.Type.INTEGER, DisplayData.inferType(1234L));
    assertEquals(DisplayData.Type.FLOAT, DisplayData.inferType(12.3));
    assertEquals(DisplayData.Type.FLOAT, DisplayData.inferType(12.3f));
    assertEquals(DisplayData.Type.BOOLEAN, DisplayData.inferType(true));
    assertEquals(DisplayData.Type.TIMESTAMP, DisplayData.inferType(Instant.now()));
    assertEquals(DisplayData.Type.DURATION, DisplayData.inferType(Duration.millis(1234)));
    assertEquals(DisplayData.Type.JAVA_CLASS,
        DisplayData.inferType(ClassForDisplay.of(DisplayDataTest.class)));
    assertEquals(DisplayData.Type.JAVA_CLASS, DisplayData.inferType(DisplayDataTest.class));
    assertEquals(DisplayData.Type.STRING, DisplayData.inferType("hello world"));

    assertEquals(null, DisplayData.inferType(null));
    assertEquals(null, DisplayData.inferType(new Object() {}));
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
          .add("boolean", true)
          .add("java_class", DisplayDataTest.class)
          .add("timestamp", now)
          .add("duration", oneHour);
      }
    };
    DisplayData data = DisplayData.from(component);

    assertThat(data, hasDisplayItem("string", "foobar"));
    assertThat(data, hasDisplayItem("integer", 123));
    assertThat(data, hasDisplayItem("float", 3.14));
    assertThat(data, hasDisplayItem("boolean", true));
    assertThat(data, hasDisplayItem("java_class", DisplayDataTest.class));
    assertThat(data, hasDisplayItem("timestamp", now));
    assertThat(data, hasDisplayItem("duration", oneHour));
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
                hasNamespace(component.getClass()))));
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
  public void testIncludeNullNamespace() {
    final HasDisplayData subComponent = new HasDisplayData() {
      @Override
      public void populateDisplayData(Builder builder) {
      }
    };

    thrown.expect(NullPointerException.class);
    DisplayData.from(new HasDisplayData() {
        @Override
        public void populateDisplayData(Builder builder) {
          builder.include(subComponent, (ClassForDisplay) null);
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

  @Test
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

  @Test
  public void testJsonSerialization() throws IOException {
    final String stringValue = "foobar";
    final int intValue = 1234;
    final double floatValue = 123.4;
    final boolean boolValue = true;
    final int durationMillis = 1234;

    HasDisplayData component = new HasDisplayData() {
      @Override
      public void populateDisplayData(Builder builder) {
        builder
            .add("string", stringValue)
            .add("long", intValue)
            .add("double", floatValue)
            .add("boolean", boolValue)
            .add("instant", new Instant(0))
            .add("duration", Duration.millis(durationMillis))
            .add("class", DisplayDataTest.class)
              .withLinkUrl("http://abc")
              .withLabel("baz")
        ;
      }
    };
    DisplayData data = DisplayData.from(component);

    JsonNode json = MAPPER.readTree(MAPPER.writeValueAsBytes(data));
    assertThat(json, hasExpectedJson(component, "STRING", "string", quoted(stringValue)));
    assertThat(json, hasExpectedJson(component, "INTEGER", "long", intValue));
    assertThat(json, hasExpectedJson(component, "FLOAT", "double", floatValue));
    assertThat(json, hasExpectedJson(component, "BOOLEAN", "boolean", boolValue));
    assertThat(json, hasExpectedJson(component, "DURATION", "duration", durationMillis));
    assertThat(json, hasExpectedJson(
        component, "TIMESTAMP", "instant", quoted("1970-01-01T00:00:00.000Z")));
    assertThat(json, hasExpectedJson(
        component, "JAVA_CLASS", "class", quoted(DisplayDataTest.class.getName()),
        quoted("DisplayDataTest"), "baz", "http://abc"));
  }

  private String quoted(Object obj) {
    return String.format("\"%s\"", obj);
  }

  private Matcher<Iterable<? super JsonNode>> hasExpectedJson(
      HasDisplayData component, String type, String key, Object value)
      throws IOException {
    return hasExpectedJson(component, type, key, value, null, null, null);
  }

  private Matcher<Iterable<? super JsonNode>> hasExpectedJson(
      HasDisplayData component,
      String type,
      String key,
      Object value,
      Object shortValue,
      String label,
      String linkUrl) throws IOException {
    Class<?> nsClass = component.getClass();

    StringBuilder builder = new StringBuilder();
    builder.append("{");
    builder.append(String.format("\"namespace\":\"%s\",", nsClass.getName()));
    builder.append(String.format("\"type\":\"%s\",", type));
    builder.append(String.format("\"key\":\"%s\",", key));
    builder.append(String.format("\"value\":%s", value));

    if (shortValue != null) {
      builder.append(String.format(",\"shortValue\":%s", shortValue));
    }
    if (label != null) {
      builder.append(String.format(",\"label\":\"%s\"", label));
    }
    if (linkUrl != null) {
      builder.append(String.format(",\"linkUrl\":\"%s\"", linkUrl));
    }

    builder.append("}");

    JsonNode jsonNode = MAPPER.readTree(builder.toString());
    return hasItem(jsonNode);
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

  private static  <T> Matcher<DisplayData.Item> hasShortValue(Matcher<T> valueStringMatcher) {
    return new FeatureMatcher<DisplayData.Item, T>(
        valueStringMatcher, "display item with short value", "short value") {
      @Override
      protected T featureValueOf(DisplayData.Item actual) {
        @SuppressWarnings("unchecked")
        T shortValue = (T) actual.getShortValue();
        return shortValue;
      }
    };
  }
}
