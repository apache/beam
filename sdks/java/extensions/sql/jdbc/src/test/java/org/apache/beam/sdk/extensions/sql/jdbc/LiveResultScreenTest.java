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
package org.apache.beam.sdk.extensions.sql.jdbc;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.jline.utils.AttributedString;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link LiveResultScreen}. */
@RunWith(JUnit4.class)
public class LiveResultScreenTest {
  private static final Schema SCHEMA =
      Schema.builder().addInt64Field("id").addStringField("name").build();

  private static final LiveResultScreen.Status RUNNING =
      new LiveResultScreen.Status(State.RUNNING, 3_000, null);

  @Test
  public void showsTheHeaderAndEachChangeWithItsKind() {
    LiveResultScreen screen = new LiveResultScreen("Live results", SCHEMA, 100);
    screen.add(ValueKind.INSERT, row(1, "a"));
    screen.add(ValueKind.UPDATE_BEFORE, row(1, "a"));
    screen.add(ValueKind.UPDATE_AFTER, row(1, "bb"));
    screen.add(ValueKind.DELETE, row(22, "x"));

    List<AttributedString> lines = screen.render(80, 10, RUNNING);

    assertEquals(10, lines.size());
    assertThat(lines.get(0).toString(), startsWith(" Live results"));
    assertThat(lines.get(0).toString(), containsString("RUNNING  00:03"));
    assertEquals(ImmutableList.of("op", "id", "name"), cells(lines.get(1)));
    assertEquals(ImmutableList.of("+I", "1", "a"), cells(lines.get(3)));
    assertEquals(ImmutableList.of("-U", "1", "a"), cells(lines.get(4)));
    assertEquals(ImmutableList.of("+U", "1", "bb"), cells(lines.get(5)));
    assertEquals(ImmutableList.of("-D", "22", "x"), cells(lines.get(6)));
    assertEquals("", lines.get(7).toString());
    assertThat(lines.get(9).toString(), startsWith(" 4 rows · live"));

    // Numbers line up on the right. Added rows are green, and retracted ones red.
    assertThat(lines.get(3).toString(), startsWith("+I   │    1 │ a"));
    assertThat(lines.get(6).toString(), startsWith("-D   │   22 │ x"));
    assertThat(lines.get(3).toAnsi(), startsWith("\033[32m+I"));
    assertThat(lines.get(4).toAnsi(), startsWith("\033[31m-U"));
  }

  @Test
  public void followsTheNewestRowsUntilScrolledUp() {
    LiveResultScreen screen = screenWithRows(20, 100);
    // Four lines of rows fit in eight.
    assertEquals(ImmutableList.of(16L, 17L, 18L, 19L), ids(screen.render(40, 8, RUNNING)));

    screen.lineUp();
    assertFalse(screen.isFollowing());
    for (int i = 20; i < 25; i++) {
      screen.add(ValueKind.INSERT, row(i, "r"));
    }
    List<AttributedString> lines = screen.render(40, 8, RUNNING);
    assertEquals(ImmutableList.of(15L, 16L, 17L, 18L), ids(lines));
    assertThat(lines.get(7).toString(), startsWith(" 25 rows · paused, 5 new"));

    screen.end();
    lines = screen.render(40, 8, RUNNING);
    assertTrue(screen.isFollowing());
    assertEquals(ImmutableList.of(21L, 22L, 23L, 24L), ids(lines));
    assertThat(lines.get(7).toString(), startsWith(" 25 rows · live"));
  }

  @Test
  public void pagesThroughTheRows() {
    LiveResultScreen screen = screenWithRows(10, 100);
    screen.render(40, 8, RUNNING);

    screen.home();
    assertEquals(ImmutableList.of(0L, 1L, 2L, 3L), ids(screen.render(40, 8, RUNNING)));
    screen.pageDown();
    assertEquals(ImmutableList.of(4L, 5L, 6L, 7L), ids(screen.render(40, 8, RUNNING)));
    screen.lineDown();
    assertEquals(ImmutableList.of(5L, 6L, 7L, 8L), ids(screen.render(40, 8, RUNNING)));
    // Reaching the newest rows follows them again.
    screen.pageDown();
    assertTrue(screen.isFollowing());
    assertEquals(ImmutableList.of(6L, 7L, 8L, 9L), ids(screen.render(40, 8, RUNNING)));
    screen.pageUp();
    assertEquals(ImmutableList.of(2L, 3L, 4L, 5L), ids(screen.render(40, 8, RUNNING)));
  }

  @Test
  public void keepsOnlyTheNewestRows() {
    List<AttributedString> lines = screenWithRows(5, 3).render(40, 8, RUNNING);

    assertEquals(ImmutableList.of(2L, 3L, 4L), ids(lines));
    // Two rows fell out of the view.
    assertThat(lines.get(7).toString(), startsWith(" 5 rows · 2 dropped · live"));
  }

  @Test
  public void cutsLongValuesAndScrollsSideways() {
    Schema wide =
        Schema.builder()
            .addStringField("first")
            .addStringField("second")
            .addStringField("third")
            .build();
    LiveResultScreen screen = new LiveResultScreen("Live results", wide, 100);
    screen.add(ValueKind.INSERT, Row.withSchema(wide).addValues("x".repeat(50), "b", "c").build());

    List<AttributedString> lines = screen.render(40, 6, RUNNING);
    String cut = "x".repeat(LiveResultScreen.MAX_COLUMN_WIDTH - 1) + "…";
    assertThat(lines.get(3).toString(), containsString(cut));
    assertThat(lines.get(1).toString(), startsWith("op"));

    screen.right();
    lines = screen.render(40, 6, RUNNING);
    assertFalse(lines.get(1).toString().startsWith("op"));
    assertThat(lines.get(1).toString(), containsString("second"));

    screen.left();
    assertThat(screen.render(40, 6, RUNNING).get(1).toString(), startsWith("op"));
  }

  @Test
  public void showsThatThePipelineFailed() {
    LiveResultScreen screen = screenWithRows(1, 100);

    List<AttributedString> lines =
        screen.render(80, 6, new LiveResultScreen.Status(State.FAILED, 65_000, "failed: boom"));

    assertThat(lines.get(0).toString(), containsString("FAILED  01:05"));
    assertThat(lines.get(5).toString(), containsString("failed: boom"));
  }

  @Test
  public void asksForABiggerTerminal() {
    List<AttributedString> lines = screenWithRows(1, 100).render(80, 3, RUNNING);

    assertEquals(3, lines.size());
    assertThat(lines.get(0).toString(), containsString("bigger"));
  }

  @Test
  public void formatsValuesOnOneLine() {
    Schema nested = Schema.builder().addInt32Field("a").addStringField("b").build();

    assertEquals("NULL", LiveResultScreen.format(null));
    assertEquals("a b c", LiveResultScreen.format("a\nb\tc"));
    assertEquals("0x01ab", LiveResultScreen.format(new byte[] {1, (byte) 0xab}));
    assertEquals("1970-01-01 00:00:01.500", LiveResultScreen.format(new Instant(1_500)));
    assertEquals("1000", LiveResultScreen.format(new BigDecimal("1E+3")));
    assertEquals(
        "{1, x}", LiveResultScreen.format(Row.withSchema(nested).addValues(1, "x").build()));
    assertEquals("[1, NULL]", LiveResultScreen.format(Arrays.asList(1, null)));
  }

  private static LiveResultScreen screenWithRows(int count, int maxRows) {
    LiveResultScreen screen = new LiveResultScreen("Live results", SCHEMA, maxRows);
    for (int i = 0; i < count; i++) {
      screen.add(ValueKind.INSERT, row(i, "r"));
    }
    return screen;
  }

  private static Row row(long id, String name) {
    return Row.withSchema(SCHEMA).addValues(id, name).build();
  }

  /** The values of a line, split at the column separators. */
  private static List<String> cells(AttributedString line) {
    return Arrays.stream(line.toString().split("│")).map(String::trim).collect(toList());
  }

  /** The ids of the rows on the screen. */
  private static List<Long> ids(List<AttributedString> lines) {
    return lines.subList(3, lines.size() - 1).stream()
        .map(AttributedString::toString)
        .filter(line -> !line.isEmpty())
        .map(line -> Long.parseLong(cells(new AttributedString(line)).get(1)))
        .collect(toList());
  }
}
