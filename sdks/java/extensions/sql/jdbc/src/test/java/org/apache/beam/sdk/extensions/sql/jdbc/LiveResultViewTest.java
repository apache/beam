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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.beam.sdk.extensions.sql.impl.JdbcDriver;
import org.apache.beam.sdk.extensions.sql.impl.rel.LiveResult;
import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalogManager;
import org.apache.beam.sdk.extensions.sql.meta.provider.seqgen.GenerateSequenceTableProvider;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.jline.terminal.Size;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.ExternalTerminal;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Runs unbounded queries through JDBC and shows them in a {@link LiveResultView}. */
@RunWith(JUnit4.class)
public class LiveResultViewTest {
  private final List<List<String>> frames = new CopyOnWriteArrayList<>();
  private final ByteArrayOutputStream screen = new ByteArrayOutputStream();
  private final PipedOutputStream keys = new PipedOutputStream();
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private int firstNewFrame;
  private Terminal terminal;
  private Connection connection;
  private Statement statement;

  @Before
  public void setUp() throws Exception {
    terminal =
        new ExternalTerminal("test", "xterm-256color", new PipedInputStream(keys), screen, UTF_8);
    terminal.setSize(new Size(80, 12));
    LiveResult.setViewer(new LiveResultView(() -> terminal, frames::add));
    InMemoryCatalogManager catalogManager = new InMemoryCatalogManager();
    catalogManager.registerTableProvider(new GenerateSequenceTableProvider());
    connection = JdbcDriver.connect(catalogManager, PipelineOptionsFactory.create());
    statement = connection.createStatement();
    statement.execute(
        "CREATE EXTERNAL TABLE seq (sequence BIGINT, event_time TIMESTAMP) TYPE 'sequence'"
            + " TBLPROPERTIES '{\"elementsPerSecond\": 20}'");
  }

  @After
  public void tearDown() throws Exception {
    LiveResult.setViewer(null);
    executor.shutdownNow();
    connection.close();
  }

  @Test
  public void showsRowsAndScrollsUntilTheUserQuits() throws Exception {
    Future<ResultSet> query = executor.submit(() -> statement.executeQuery("SELECT * FROM seq"));
    // Eight rows fill the screen.
    List<String> frame = awaitFrame(lines -> rows(lines).size() == 8);
    assertEquals("[op, sequence, event_time]", cells(frame.get(1)).toString());
    assertThat(rows(frame), everyItem(startsWith("+I|")));

    // The arrow keys and vi's keys both scroll. Scrolling up pauses, and End follows again.
    press("\033[A");
    awaitFrame(lines -> footer(lines).contains("paused"));
    press("G");
    awaitFrame(lines -> footer(lines).contains("· live"));
    press("k");
    awaitFrame(lines -> footer(lines).contains("paused"));
    press("\033[F");
    awaitFrame(lines -> footer(lines).contains("· live"));
    press("q");

    // The view showed the rows, so the query returns none.
    assertFalse(query.get(30, TimeUnit.SECONDS).next());
    String output = screen.toString(UTF_8.name());
    assertThat(output, containsString("\033[?1049l")); // left the alternate screen
    assertThat(output, containsString("Live view closed after"));
  }

  @Test
  public void cancellingTheStatementClosesTheView() throws Exception {
    Future<ResultSet> query = executor.submit(() -> statement.executeQuery("SELECT * FROM seq"));
    awaitFrame(lines -> !rows(lines).isEmpty());

    // What Ctrl+C does in the shell.
    statement.cancel();

    query.get(30, TimeUnit.SECONDS).close();
    assertThat(screen.toString(UTF_8.name()), containsString("Live view closed after"));
  }

  @Test
  public void drawsEachRowOnItsOwnLine() throws Exception {
    Future<ResultSet> query = executor.submit(() -> statement.executeQuery("SELECT * FROM seq"));
    awaitFrame(lines -> rows(lines).size() == 8);
    // A resize redraws the whole screen at the new size.
    terminal.setSize(new Size(100, 16));
    awaitFrame(lines -> lines.size() == 16 && rows(lines).size() == 12);
    press("q");
    query.get(30, TimeUnit.SECONDS).close();

    // Rows that wrap into each other look like one long line to terminals that keep track of
    // wrapped lines.
    String output = screen.toString(UTF_8.name());
    int resized = output.lastIndexOf("\033[2J");
    assertEquals(0, wraps(output.substring(output.indexOf("\033[2J"), resized), 80));
    assertEquals(0, wraps(output.substring(resized, output.indexOf("\033[?1049l")), 100));
  }

  @Test
  public void loadsTheLineDrawingCharactersBeforeDrawingLines() throws Exception {
    // Like tmux, screen draws lines from a second character set, which has to be loaded first.
    PipedOutputStream screenKeys = new PipedOutputStream();
    ByteArrayOutputStream screenOutput = new ByteArrayOutputStream();
    Terminal screenTerminal =
        new ExternalTerminal(
            "test", "screen-256color", new PipedInputStream(screenKeys), screenOutput, UTF_8);
    screenTerminal.setSize(new Size(80, 12));
    LiveResult.setViewer(new LiveResultView(() -> screenTerminal, frames::add));
    Future<ResultSet> query = executor.submit(() -> statement.executeQuery("SELECT * FROM seq"));
    awaitFrame(lines -> !rows(lines).isEmpty());
    screenKeys.write('q');
    screenKeys.flush();
    query.get(30, TimeUnit.SECONDS).close();

    // ESC ) 0 loads the line-drawing set, and SO switches to it to draw a line.
    String output = screenOutput.toString(UTF_8.name());
    int loaded = output.indexOf("\033)0");
    assertThat(loaded, greaterThanOrEqualTo(0));
    assertThat(loaded, lessThan(output.indexOf('\016')));
  }

  private void press(String key) throws IOException {
    firstNewFrame = frames.size();
    keys.write(key.getBytes(UTF_8));
    keys.flush();
  }

  /**
   * Waits up to a minute for the view to draw a frame that matches {@code condition}, among the
   * frames since the last key press.
   */
  private List<String> awaitFrame(Predicate<List<String>> condition) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
    for (int checked = firstNewFrame; System.nanoTime() < deadline; Thread.sleep(20)) {
      for (; checked < frames.size(); checked++) {
        if (condition.test(frames.get(checked))) {
          return frames.get(checked);
        }
      }
    }
    throw new AssertionError(
        "No frame matched. The last one was "
            + (frames.isEmpty() ? "none" : frames.get(frames.size() - 1)));
  }

  /** The rows on screen, as their cells joined by "|". */
  private static List<String> rows(List<String> frame) {
    return frame.subList(3, frame.size() - 1).stream()
        .filter(line -> !line.isEmpty())
        .map(line -> String.join("|", cells(line)))
        .collect(toList());
  }

  private static String footer(List<String> frame) {
    return frame.get(frame.size() - 1);
  }

  private static List<String> cells(String line) {
    return Arrays.stream(line.split("│")).map(String::trim).collect(toList());
  }

  /**
   * Counts the characters that {@code output} writes past the end of a row {@code width} columns
   * wide, which the terminal wraps onto the next row. It follows the cursor's column through the
   * kinds of sequences that the view writes.
   */
  private static int wraps(String output, int width) {
    int wraps = 0;
    // At width, the cursor is past the last column, and the next character wraps.
    int column = 0;
    for (int i = 0; i < output.length(); ) {
      char c = output.charAt(i++);
      if (c == '\033' && output.charAt(i) == '[') {
        int end = i + 1;
        while (output.charAt(end) < '@' || output.charAt(end) > '~') {
          end++;
        }
        String params = output.substring(i + 1, end);
        int n = params.matches("\\d+") ? Integer.parseInt(params) : 1;
        switch (output.charAt(end)) {
          case 'C':
            column = Math.min(width - 1, column + n);
            break;
          case 'D':
            column = Math.max(0, Math.min(column, width - 1) - n);
            break;
          case 'G':
            column = n - 1;
            break;
          case 'H':
            column =
                params.contains(";")
                    ? Integer.parseInt(params.substring(params.indexOf(';') + 1)) - 1
                    : 0;
            break;
          case 'L':
          case 'M':
            column = 0;
            break;
          default:
            // Moves up or down, clears, or sets the style.
        }
        i = end + 1;
      } else if (c == '\033') {
        // ESC ( B picks a character set. The view's other escapes are two characters long.
        i += output.charAt(i) == '(' ? 2 : 1;
      } else if (c == '\r') {
        column = 0;
      } else if (c == '\b') {
        column = Math.max(0, Math.min(column, width - 1) - 1);
      } else if (c >= ' ' && !Character.isLowSurrogate(c)) {
        if (column == width) {
          wraps++;
          column = 0;
        }
        column++;
      }
    }
    return wraps;
  }
}
