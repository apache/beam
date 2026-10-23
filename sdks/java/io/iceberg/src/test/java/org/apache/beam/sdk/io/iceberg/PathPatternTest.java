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
package org.apache.beam.sdk.io.iceberg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PathPatternTest {

  @Test
  public void severalPlaceholdersInPathOrder() {
    assertEquals(
        Arrays.asList("db", "tbl", "y"), PathPattern.placeholders("gs://lake/{db}/{tbl}/y={y}/**"));
  }

  /** Lazily, so the first {@code }} wins; greedy would give one name {@code db}.{tbl}. */
  @Test
  public void adjacentPlaceholdersStaySeparate() {
    assertEquals(Arrays.asList("db", "tbl"), PathPattern.placeholders("{db}.{tbl}"));
  }

  @Test
  public void noPlaceholders() {
    assertEquals(Collections.emptyList(), PathPattern.placeholders("gs://lake/export/orders/"));
    assertEquals(Collections.emptyList(), PathPattern.placeholders(""));
  }

  /** {@code .+?} needs a character, so these are literal text rather than a malformed name. */
  @Test
  public void emptyOrUnterminatedBracesAreNotPlaceholders() {
    assertEquals(Collections.emptyList(), PathPattern.placeholders("{}"));
    assertEquals(Collections.emptyList(), PathPattern.placeholders("gs://lake/{table"));
  }

  @Test
  public void strayClosingBraceIsSkipped() {
    assertEquals(Arrays.asList("a"), PathPattern.placeholders("table}/{a}"));
  }

  /** Kept, not collapsed, so compiling the pattern can refuse the duplicate. */
  @Test
  public void repeatedNameIsReportedTwice() {
    assertEquals(Arrays.asList("t", "t"), PathPattern.placeholders("{t}/{t}"));
  }

  /**
   * Nothing here validates a name: {@code .} spans {@code /}, {@code .} and {@code {}, and there is
   * no escape syntax. Compiling the pattern is what refuses these.
   */
  @Test
  public void namesAreNotValidatedHere() {
    assertEquals(Arrays.asList("a/b"), PathPattern.placeholders("{a/b}"));
    assertEquals(Arrays.asList("a.b"), PathPattern.placeholders("{a.b}"));
    assertEquals(Arrays.asList("a{b"), PathPattern.placeholders("{a{b}"));
    assertEquals(Arrays.asList(" "), PathPattern.placeholders("{ }"));
  }

  private static PathPattern.Match matched(String pathPattern, String path) {
    PathPattern.Match match = PathPattern.compile(pathPattern).match(path);
    assertNotNull(pathPattern + " did not match " + path, match);
    return match;
  }

  @Test
  public void compileWithNoPlaceholders() {
    PathPattern pattern = PathPattern.compile("gs://lake/orders/**");
    assertEquals(Collections.emptyList(), pattern.captureNames());
    PathPattern.Match match = pattern.match("gs://lake/orders/part-0.parquet");
    assertNotNull(match);
    assertEquals(Collections.emptyMap(), match.captures());
    assertEquals("", match.remainder());
  }

  @Test
  public void capturesTheFoldersAndTheRemainderBelowThem() {
    PathPattern.Match match =
        matched("gs://lake/{db}/{tbl}/**", "gs://lake/sales/orders/y=2024/part-0.parquet");
    assertEquals("sales", match.captures().get("db"));
    assertEquals("orders", match.captures().get("tbl"));
    assertEquals("y=2024/part-0.parquet", match.remainder());
  }

  /** The file name is part of the remainder, because _SUCCESS and .crc are files. */
  @Test
  public void theRemainderIncludesTheFileName() {
    PathPattern.Match match = matched("gs://lake/{tbl}/**", "gs://lake/orders/_SUCCESS");
    assertEquals("orders", match.captures().get("tbl"));
    assertEquals("_SUCCESS", match.remainder());
  }

  /** Captures come back in the order the placeholders appear, so messages read like the pattern. */
  @Test
  public void capturesAreInPatternOrder() {
    PathPattern.Match match =
        matched("gs://lake/{db}/{tbl}/**", "gs://lake/sales/orders/f.parquet");
    assertEquals(Arrays.asList("db", "tbl"), new ArrayList<>(match.captures().keySet()));
  }

  /** A deeper path binds the shallower folders rather than failing; '**' absorbs the rest. */
  @Test
  public void placeholderNeverSpansADirectoryLevel() {
    PathPattern.Match match =
        matched("gs://lake/{db}/{tbl}/**", "gs://lake/sales/eu/orders/f.parquet");
    assertEquals("sales", match.captures().get("db"));
    assertEquals("eu", match.captures().get("tbl"));
    assertEquals("orders/f.parquet", match.remainder());
  }

  /** '*' is one level, so the depth is pinned where '**' would have absorbed it. */
  @Test
  public void singleStarPinsTheDepth() {
    PathPattern pattern = PathPattern.compile("gs://lake/export/{table}/*");
    assertNotNull(pattern.match("gs://lake/export/orders/part-0.parquet"));
    assertNull(pattern.match("gs://lake/export/orders/y=2024/part-0.parquet"));
  }

  /** The pattern is the only place on this transform that can say which files belong. */
  @Test
  public void aSelectorExcludesOtherFiles() {
    PathPattern pattern = PathPattern.compile("gs://lake/export/{table}/*.parquet");
    assertNotNull(pattern.match("gs://lake/export/orders/f.parquet"));
    assertNull(pattern.match("gs://lake/export/orders/f.json"));
  }

  /** A literal folder keeps a sibling's files out of the table, even when they are parquet. */
  @Test
  public void aLiteralFolderExcludesItsSiblings() {
    PathPattern pattern = PathPattern.compile("gs://lake/export/{tbl}/data/**");
    assertNotNull(pattern.match("gs://lake/export/orders/data/part-0.parquet"));
    assertNull(pattern.match("gs://lake/export/orders/rejected/bad.parquet"));
  }

  /** One file per table is a layout too, and it is unambiguous under a whole-path match. */
  @Test
  public void theFileNameCanBeTheTable() {
    PathPattern.Match match =
        matched("gs://lake/export/{table}.parquet", "gs://lake/export/orders.parquet");
    assertEquals("orders", match.captures().get("table"));
    assertEquals("", match.remainder());
  }

  @Test
  public void anchoredAtTheStart() {
    assertNull(PathPattern.compile("gs://lake/{tbl}/**").match("gs://other/orders/f.parquet"));
  }

  /** Unquoted, {@code a.b+c} would also match {@code axbbc}. */
  @Test
  public void literalTextIsQuoted() {
    PathPattern pattern = PathPattern.compile("gs://lake/a.b+c/{tbl}/**");
    assertNotNull(pattern.match("gs://lake/a.b+c/orders/f.parquet"));
    assertNull(pattern.match("gs://lake/axbbc/orders/f.parquet"));
  }

  /** Globs must not capture, or the names would bind to the wrong folders. */
  @Test
  public void globsDoNotBindToPlaceholders() {
    PathPattern.Match match =
        matched("gs://lake/{db}/*/v?/{tbl}/**", "gs://lake/sales/skip/v2/orders/f.parquet");
    assertEquals("sales", match.captures().get("db"));
    assertEquals("orders", match.captures().get("tbl"));
    assertEquals(2, match.captures().size());
  }

  /** Ending at a folder would match a directory marker and never a file. */
  @Test
  public void patternEndingWithASlashIsRefused() {
    assertThrows(IllegalArgumentException.class, () -> PathPattern.compile("gs://lake/{tbl}/"));
  }

  /** A '**' above a placeholder would swallow the folder the placeholder is meant to name. */
  @Test
  public void doubleStarAboveAPlaceholderIsRefused() {
    assertThrows(
        IllegalArgumentException.class, () -> PathPattern.compile("gs://lake/**/{tbl}/**"));
    assertThrows(
        IllegalArgumentException.class, () -> PathPattern.compile("gs://lake/{db}/**/{tbl}/**"));
  }

  @Test
  public void namesThatCannotBeBoundAreRefused() {
    assertThrows(IllegalArgumentException.class, () -> PathPattern.compile("gs://lake/{a/b}/**"));
    assertThrows(IllegalArgumentException.class, () -> PathPattern.compile("gs://lake/{a.b}/**"));
    assertThrows(IllegalArgumentException.class, () -> PathPattern.compile("gs://lake/{a{b}/**"));
    assertThrows(IllegalArgumentException.class, () -> PathPattern.compile("gs://lake/{ }/**"));
  }

  @Test
  public void repeatedNameIsRefused() {
    assertThrows(IllegalArgumentException.class, () -> PathPattern.compile("gs://lake/{t}/{t}/**"));
  }

  /** What A1 reports as literal text stays literal once compiled. */
  @Test
  public void emptyOrUnterminatedBracesCompileAsLiteralText() {
    PathPattern empty = PathPattern.compile("gs://lake/{}/**");
    assertEquals(Collections.emptyList(), empty.captureNames());
    assertNotNull(empty.match("gs://lake/{}/f.parquet"));
    assertEquals(Collections.emptyList(), PathPattern.compile("gs://lake/{tbl/**").captureNames());
  }

  @Test
  public void renderFillsTheTemplateFromTheCaptures() {
    PathPattern.Match match =
        matched("gs://lake/{db}/{tbl}/**", "gs://lake/sales/orders/f.parquet");
    assertEquals(
        "warehouse.sales.orders", PathPattern.render("warehouse.{db}.{tbl}", match.captures()));
  }

  /** Templates at both extremes: no placeholder at all, and nothing but a placeholder. */
  @Test
  public void renderHandlesTemplatesWithoutAnyLiteralOrAnyPlaceholder() {
    PathPattern.Match match = matched("gs://lake/{tbl}/**", "gs://lake/orders/f.parquet");
    assertEquals("lake.events", PathPattern.render("lake.events", match.captures()));
    assertEquals("orders", PathPattern.render("{tbl}", match.captures()));
  }

  /** A name may repeat in the template, even though the path pattern refuses a repeat. */
  @Test
  public void renderRepeatsANameAsOftenAsTheTemplateUsesIt() {
    PathPattern.Match match = matched("gs://lake/{tbl}/**", "gs://lake/orders/f.parquet");
    assertEquals("orders.orders_v2", PathPattern.render("{tbl}.{tbl}_v2", match.captures()));
  }

  /** Folder names can hold {@code $} and {@code \}, which appendReplacement would have eaten. */
  @Test
  public void renderTreatsCaptureValuesAsLiteralText() {
    PathPattern.Match match = matched("gs://lake/{tbl}/**", "gs://lake/q$1/f.parquet");
    assertEquals("q$1", match.captures().get("tbl"));
    assertEquals("lake.q$1", PathPattern.render("lake.{tbl}", match.captures()));
  }

  /** The per-element backstop; checkRenderable is meant to catch this at construction. */
  @Test
  public void renderRefusesAnUnboundPlaceholder() {
    PathPattern.Match match = matched("gs://lake/{db}/**", "gs://lake/sales/f.parquet");
    assertThrows(
        IllegalArgumentException.class,
        () -> PathPattern.render("warehouse.{db}.{tbl}", match.captures()));
  }

  /** Every template placeholder captured, only some captured, and none -- all legal. */
  @Test
  public void checkRenderableAcceptsAnyTemplateThePatternCanFill() {
    PathPattern pattern = PathPattern.compile("gs://lake/{db}/{tbl}/**");
    pattern.checkRenderable("warehouse.{db}.{tbl}");
    pattern.checkRenderable("warehouse.{tbl}");
    pattern.checkRenderable("lake.events");
  }

  @Test
  public void checkRenderableRefusesAnUncapturedName() {
    PathPattern pattern = PathPattern.compile("gs://lake/{db}/{tbl}/**");
    assertThrows(
        IllegalArgumentException.class, () -> pattern.checkRenderable("warehouse.{db}.{table}"));
  }
}
