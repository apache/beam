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

import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.sql.impl.rel.LiveResult;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jline.keymap.BindingReader;
import org.jline.keymap.KeyMap;
import org.jline.terminal.Attributes;
import org.jline.terminal.Size;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedString;
import org.jline.utils.Display;
import org.jline.utils.InfoCmp.Capability;
import org.jline.utils.NonBlockingReader;

/**
 * Shows the rows of an unbounded query in a full-screen terminal view while its pipeline runs.
 *
 * <p>The column names stay at the top and new rows come in at the bottom. The first column, {@code
 * op}, shows each row's kind of change: +I, -U, +U or -D. The arrow keys, PgUp, PgDn, Home and End
 * scroll, and so do vi's h, j, k, l, g and G. Scrolling up pauses the view on the rows you're
 * looking at, and End follows the newest rows again. q or Esc closes the view. Ctrl+C belongs to
 * the shell: it cancels the statement, which closes the view too. The pipeline is cancelled once
 * the view closes.
 *
 * <p>The view opens its own JLine 3 terminal while it's on screen, next to the shell's jline2 line
 * editor. That terminal leaves the JVM's signal handlers alone, so the shell keeps handling Ctrl+C.
 * Console logging is held back while the view is open, and warnings and errors print once it
 * closes.
 */
public class LiveResultView implements LiveResult.Viewer {
  /** Opens the terminal that the view draws on. The view closes it when it's done. */
  public interface TerminalFactory {
    Terminal open() throws IOException;
  }

  /** How many of the newest rows the view keeps for scrolling back. */
  private static final int MAX_ROWS = 10_000;

  private static final String TITLE = "Beam SQL live results";
  private static final long FRAME_MILLIS = 100;
  private static final long KEY_WAIT_MILLIS = 50;
  private static final long SIZE_CHECK_MILLIS = 250;
  private static final long ESCAPE_TIMEOUT_MILLIS = 100;
  private static final Size DEFAULT_SIZE = new Size(80, 24);

  private final TerminalFactory terminals;
  private final Consumer<List<String>> frames;

  /** Shows the rows on the terminal that the shell runs in. */
  public LiveResultView() {
    this(
        () ->
            TerminalBuilder.builder()
                .system(true)
                // Leave the JVM's signal handlers alone, so the shell's Ctrl+C handler works.
                .nativeSignals(false)
                .build(),
        frame -> {});
  }

  /**
   * Shows the rows on the terminal that {@code terminals} opens, and passes the text of each frame
   * it draws to {@code frames}. Meant for tests.
   */
  public LiveResultView(TerminalFactory terminals, Consumer<List<String>> frames) {
    this.terminals = terminals;
    this.frames = frames;
  }

  @Override
  public void show(LiveResult result) throws IOException {
    LiveResultScreen screen = new LiveResultScreen(TITLE, result.getSchema(), MAX_ROWS);
    Runnable quit = () -> {};
    long start = System.currentTimeMillis();
    long end = 0;
    // Log lines would draw over the view, so hold them back until it closes.
    HeldLogs logs = HeldLogs.hold();
    try (Terminal terminal = terminals.open()) {
      KeyMap<Runnable> keys = keys(terminal, screen, quit);
      BindingReader bindings = new BindingReader(terminal.reader());
      Display display = new Display(terminal, true);
      Attributes original = terminal.enterRawMode();
      terminal.puts(Capability.enter_ca_mode);
      terminal.puts(Capability.keypad_xmit);
      terminal.puts(Capability.cursor_invisible);
      try {
        Size size = new Size(0, 0);
        List<LiveResult.Change> changes = new ArrayList<>();
        @Nullable State shownState = null;
        long shownSecond = -1;
        long lastFrame = 0;
        long lastSizeCheck = 0;
        boolean dirty = true;
        while (!result.isCancelRequested()) {
          changes.clear();
          result.drainTo(changes);
          for (LiveResult.Change change : changes) {
            screen.add(change.getKind(), change.getRow());
          }
          long now = System.currentTimeMillis();
          if (end == 0 && result.isFinished()) {
            end = now;
          }
          long elapsed = (end == 0 ? now : end) - start;
          State state = result.getState();
          // Redraw for new rows, a new pipeline state, and every second for the clock.
          if (!changes.isEmpty() || state != shownState || elapsed / 1000 != shownSecond) {
            dirty = true;
          }

          if (now - lastSizeCheck >= SIZE_CHECK_MILLIS) {
            lastSizeCheck = now;
            Size current = terminal.getSize();
            if (current.getRows() <= 0 || current.getColumns() <= 0) {
              current = DEFAULT_SIZE;
            }
            if (current.getRows() != size.getRows() || current.getColumns() != size.getColumns()) {
              size = new Size(current.getColumns(), current.getRows());
              display.clear();
              display.resize(size.getRows(), size.getColumns());
              dirty = true;
            }
          }

          if (dirty && now - lastFrame >= FRAME_MILLIS) {
            List<AttributedString> lines =
                screen.render(size.getColumns(), size.getRows(), status(result, state, elapsed));
            display.update(lines, size.cursorPos(size.getRows() - 1, 0));
            frames.accept(lines.stream().map(AttributedString::toString).collect(toList()));
            lastFrame = now;
            shownState = state;
            shownSecond = elapsed / 1000;
            dirty = false;
          }

          int next = terminal.reader().peek(KEY_WAIT_MILLIS);
          if (next == NonBlockingReader.EOF) {
            break;
          } else if (next != NonBlockingReader.READ_EXPIRED) {
            Runnable action = bindings.readBinding(keys);
            if (action == null || action == quit) {
              break;
            }
            action.run();
            dirty = true;
            // Answer keys right away.
            lastFrame = 0;
          }
        }
      } finally {
        terminal.puts(Capability.cursor_normal);
        terminal.puts(Capability.keypad_local);
        terminal.puts(Capability.exit_ca_mode);
        terminal.setAttributes(original);
        long elapsed = (end == 0 ? System.currentTimeMillis() : end) - start;
        terminal.writer().println(summary(screen, elapsed, logs.getHiddenCount()));
        terminal.flush();
      }
    } finally {
      logs.close();
    }
  }

  private static LiveResultScreen.Status status(LiveResult result, State state, long elapsed) {
    @Nullable String message = null;
    RuntimeException failure = result.getFailure();
    if (failure != null) {
      // The innermost cause says what went wrong. The exceptions around it only wrap it.
      Throwable cause = failure;
      for (Throwable next = cause.getCause();
          next != null && next != cause;
          next = cause.getCause()) {
        cause = next;
      }
      @Nullable String text = cause.getMessage();
      message =
          "failed: "
              + LiveResultScreen.format(text != null ? text : cause.getClass().getSimpleName());
    }
    return new LiveResultScreen.Status(state, elapsed, message);
  }

  private static String summary(LiveResultScreen screen, long elapsedMillis, int hiddenLogs) {
    NumberFormat numbers = NumberFormat.getIntegerInstance(Locale.US);
    long rows = screen.getReceivedCount();
    String summary =
        String.format(
            "Live view closed after %s %s in %s.",
            numbers.format(rows),
            rows == 1 ? "row" : "rows",
            LiveResultScreen.elapsed(elapsedMillis));
    if (hiddenLogs > 0) {
      summary +=
          String.format(
              " Hid %s log %s while it was open.",
              numbers.format(hiddenLogs), hiddenLogs == 1 ? "message" : "messages");
    }
    return summary;
  }

  private static KeyMap<Runnable> keys(Terminal terminal, LiveResultScreen screen, Runnable quit) {
    KeyMap<Runnable> keys = new KeyMap<>();
    bind(keys, terminal, Capability.key_up, screen::lineUp, "\033[A", "\033OA", "k");
    bind(keys, terminal, Capability.key_down, screen::lineDown, "\033[B", "\033OB", "j");
    bind(keys, terminal, Capability.key_ppage, screen::pageUp, "\033[5~", "b");
    bind(keys, terminal, Capability.key_npage, screen::pageDown, "\033[6~", " ");
    bind(keys, terminal, Capability.key_home, screen::home, "\033[H", "\033OH", "\033[1~", "g");
    bind(keys, terminal, Capability.key_end, screen::end, "\033[F", "\033OF", "\033[4~", "G");
    bind(keys, terminal, Capability.key_left, screen::left, "\033[D", "\033OD", "h");
    bind(keys, terminal, Capability.key_right, screen::right, "\033[C", "\033OC", "l");
    // Ctrl+C only arrives as a key when the terminal doesn't turn it into a signal for the shell.
    keys.bind(quit, "q", "Q", KeyMap.esc(), KeyMap.ctrl('C'));
    keys.setNomatch(() -> {});
    keys.setUnicode(() -> {});
    // How long a lone Esc waits to see if it starts a longer key, like an arrow.
    keys.setAmbiguousTimeout(ESCAPE_TIMEOUT_MILLIS);
    return keys;
  }

  /**
   * Binds the terminal's own sequence for {@code key}, and the {@code fallbacks} that other
   * terminals send for it.
   */
  private static void bind(
      KeyMap<Runnable> keys,
      Terminal terminal,
      Capability key,
      Runnable action,
      String... fallbacks) {
    @Nullable String sequence = KeyMap.key(terminal, key);
    if (sequence != null && !sequence.isEmpty()) {
      keys.bind(action, sequence);
    }
    for (String fallback : fallbacks) {
      keys.bindIfNotBound(action, fallback);
    }
  }
}
