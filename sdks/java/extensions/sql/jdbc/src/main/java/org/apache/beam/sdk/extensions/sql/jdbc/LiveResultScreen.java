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

import java.math.BigDecimal;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueKind;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.joda.time.ReadableInstant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * What the live view shows: the newest rows of a query under a pinned header, scrolled to where the
 * user wants. It only works out the lines of the screen. {@link LiveResultView} draws them on the
 * terminal and turns keys into calls to this class.
 *
 * <p>Column widths start at the width of the column names and grow with the data, up to {@link
 * #MAX_COLUMN_WIDTH}. Longer values are cut off with "…".
 */
class LiveResultScreen {
  static final int MAX_COLUMN_WIDTH = 30;

  /** How far the left and right keys scroll. */
  private static final int SCROLL_COLUMNS = 8;

  /** Lines used by the title, the column names, the line under them, and the footer. */
  private static final int CHROME_LINES = 4;

  private static final String SEPARATOR = " │ ";
  private static final String NULL = "NULL";
  private static final String ELLIPSIS = "…";
  private static final String KEYS = "↑↓ PgUp PgDn Home End ←→ scroll · q quit";

  private static final DateTimeFormatter TIMESTAMP_FORMAT =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZoneUTC();

  private static final AttributedStyle BAR = AttributedStyle.INVERSE;
  private static final AttributedStyle HEADER = AttributedStyle.BOLD;
  private static final AttributedStyle RULE = AttributedStyle.DEFAULT.faint();
  private static final AttributedStyle ADDED =
      AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN);
  private static final AttributedStyle RETRACTED =
      AttributedStyle.DEFAULT.foreground(AttributedStyle.RED);

  /** The state of the pipeline, shown in the title and the footer. */
  static class Status {
    final State state;
    final long elapsedMillis;
    final @Nullable String failure;

    Status(State state, long elapsedMillis, @Nullable String failure) {
      this.state = state;
      this.elapsedMillis = elapsedMillis;
      this.failure = failure;
    }
  }

  /** A row of the result, ready to print. */
  private static class Line {
    final ValueKind kind;
    final String[] cells;

    Line(ValueKind kind, String[] cells) {
      this.kind = kind;
      this.cells = cells;
    }
  }

  private final String title;
  private final List<String> names;
  private final boolean[] rightAligned;
  private final int[] widths;
  private final int maxRows;

  /** The newest rows, oldest first, in a ring of {@code maxRows} slots. */
  private final @Nullable Line[] ring;

  private int first;
  private int size;
  private long received;
  private long evicted;

  /** Whether the view keeps the newest rows on screen. */
  private boolean following = true;

  /** The row at the top of the screen while not following. */
  private int top;

  /** Rows that came in since the user scrolled away from the newest rows. */
  private long unseen;

  private int offsetX;
  private int bodyHeight = 1;

  LiveResultScreen(String title, Schema schema, int maxRows) {
    this.title = title;
    this.maxRows = maxRows;
    this.ring = new Line[maxRows];
    List<String> names = new ArrayList<>();
    names.add("op");
    names.addAll(schema.getFieldNames());
    this.names = Collections.unmodifiableList(names);
    this.rightAligned = new boolean[names.size()];
    this.widths = new int[names.size()];
    for (int i = 0; i < names.size(); i++) {
      widths[i] = Math.max(NULL.length(), width(names.get(i)));
    }
    for (int i = 0; i < schema.getFieldCount(); i++) {
      rightAligned[i + 1] = isNumeric(schema.getField(i).getType());
    }
  }

  /** Adds the next row of the result. */
  void add(ValueKind kind, Row row) {
    String[] cells = new String[names.size()];
    cells[0] = opCode(kind);
    for (int i = 0; i < row.getFieldCount(); i++) {
      cells[i + 1] = format(row.getValue(i));
    }
    for (int i = 0; i < cells.length; i++) {
      widths[i] = Math.max(widths[i], Math.min(MAX_COLUMN_WIDTH, width(cells[i])));
    }

    received++;
    if (size == maxRows) {
      first = (first + 1) % maxRows;
      size--;
      evicted++;
      top = Math.max(0, top - 1);
    }
    ring[(first + size) % maxRows] = new Line(kind, cells);
    size++;
    if (!following) {
      unseen++;
    }
  }

  long getReceivedCount() {
    return received;
  }

  boolean isFollowing() {
    return following;
  }

  void lineUp() {
    scrollTo(currentTop() - 1);
  }

  void lineDown() {
    scrollTo(currentTop() + 1);
  }

  void pageUp() {
    scrollTo(currentTop() - bodyHeight);
  }

  void pageDown() {
    scrollTo(currentTop() + bodyHeight);
  }

  void home() {
    scrollTo(0);
  }

  void end() {
    following = true;
    unseen = 0;
  }

  void left() {
    offsetX = Math.max(0, offsetX - SCROLL_COLUMNS);
  }

  void right() {
    offsetX += SCROLL_COLUMNS;
  }

  /** Scrolls so that row {@code row} is at the top, and follows the newest rows at the bottom. */
  private void scrollTo(int row) {
    int bottom = Math.max(0, size - bodyHeight);
    if (row >= bottom) {
      end();
    } else {
      following = false;
      top = Math.max(0, row);
    }
  }

  private int currentTop() {
    return following ? Math.max(0, size - bodyHeight) : top;
  }

  /** Returns the lines of a screen {@code width} columns wide and {@code height} lines high. */
  List<AttributedString> render(int width, int height, Status status) {
    List<AttributedString> lines = new ArrayList<>();
    if (height < CHROME_LINES + 1 || width < 20) {
      for (int i = 0; i < height; i++) {
        lines.add(i == 0 ? fit("Make the terminal bigger to see the rows", width) : blank());
      }
      return lines;
    }
    bodyHeight = height - CHROME_LINES;

    int tableWidth = tableWidth();
    offsetX = Math.max(0, Math.min(offsetX, tableWidth - width));

    lines.add(titleBar(width, status));
    lines.add(shift(header(), width));
    lines.add(shift(rule(), width));
    int start = currentTop();
    for (int i = 0; i < bodyHeight; i++) {
      int row = start + i;
      lines.add(row < size ? shift(line(get(row)), width) : blank());
    }
    lines.add(footer(width, status));
    return lines;
  }

  private Line get(int row) {
    Line line = ring[(first + row) % maxRows];
    if (line == null) {
      throw new IllegalStateException("No row at " + row);
    }
    return line;
  }

  private AttributedString titleBar(int width, Status status) {
    String right = status.state + "  " + elapsed(status.elapsedMillis) + " ";
    String left = " " + title;
    int room = Math.max(0, width - width(right));
    String fitted = cut(left, room);
    return new AttributedStringBuilder()
        .style(BAR)
        .append(fitted)
        .append(spaces(room - width(fitted)))
        .append(right)
        .toAttributedString()
        .columnSubSequence(0, width);
  }

  private AttributedString footer(int width, Status status) {
    NumberFormat numbers = NumberFormat.getIntegerInstance(Locale.US);
    StringBuilder left =
        new StringBuilder(" ")
            .append(numbers.format(received))
            .append(received == 1 ? " row" : " rows");
    if (evicted > 0) {
      left.append(" · ").append(numbers.format(evicted)).append(" dropped");
    }
    if (following) {
      left.append(" · live");
    } else {
      left.append(" · paused, ").append(numbers.format(unseen)).append(" new");
    }
    String failure = status.failure;
    if (failure != null) {
      left.append(" · ").append(failure);
    }
    String right = KEYS + " ";
    int room = Math.max(0, width - width(left.toString()));
    String keys = width(right) <= room ? right : "";
    // Leave the last column empty, so the terminal doesn't scroll when the line ends there.
    int filler = Math.max(0, width - 1 - width(left.toString()) - width(keys));
    return new AttributedStringBuilder()
        .style(BAR)
        .append(left)
        .append(spaces(filler))
        .append(keys)
        .toAttributedString()
        .columnSubSequence(0, width - 1);
  }

  private AttributedString header() {
    AttributedStringBuilder builder = new AttributedStringBuilder();
    for (int i = 0; i < names.size(); i++) {
      if (i > 0) {
        builder.append(SEPARATOR, RULE);
      }
      builder.append(pad(cut(names.get(i), widths[i]), widths[i], false), HEADER);
    }
    return builder.toAttributedString();
  }

  private AttributedString rule() {
    StringBuilder rule = new StringBuilder();
    for (int i = 0; i < names.size(); i++) {
      if (i > 0) {
        rule.append("─┼─");
      }
      rule.append("─".repeat(widths[i]));
    }
    return new AttributedString(rule, RULE);
  }

  private AttributedString line(Line line) {
    AttributedStringBuilder builder = new AttributedStringBuilder();
    builder.append(pad(line.cells[0], widths[0], false), kindStyle(line.kind));
    for (int i = 1; i < line.cells.length; i++) {
      builder.append(SEPARATOR, RULE);
      builder.append(pad(cut(line.cells[i], widths[i]), widths[i], rightAligned[i]));
    }
    return builder.toAttributedString();
  }

  private AttributedString shift(AttributedString line, int width) {
    int length = line.columnLength();
    if (offsetX >= length) {
      return blank();
    }
    return line.columnSubSequence(offsetX, Math.min(length, offsetX + width));
  }

  private int tableWidth() {
    int total = SEPARATOR.length() * (widths.length - 1);
    for (int width : widths) {
      total += width;
    }
    return total;
  }

  private static AttributedStyle kindStyle(ValueKind kind) {
    switch (kind) {
      case UPDATE_BEFORE:
      case DELETE:
        return RETRACTED;
      default:
        return ADDED;
    }
  }

  /** The short form of a change kind that Flink's SQL client uses too. */
  static String opCode(ValueKind kind) {
    switch (kind) {
      case UPDATE_BEFORE:
        return "-U";
      case UPDATE_AFTER:
        return "+U";
      case DELETE:
        return "-D";
      default:
        return "+I";
    }
  }

  /** Formats a value on one line. */
  static String format(@Nullable Object value) {
    return printable(rawFormat(value));
  }

  private static String rawFormat(@Nullable Object value) {
    if (value == null) {
      return NULL;
    } else if (value instanceof ReadableInstant) {
      return TIMESTAMP_FORMAT.print((ReadableInstant) value);
    } else if (value instanceof BigDecimal) {
      return ((BigDecimal) value).toPlainString();
    } else if (value instanceof byte[]) {
      StringBuilder hex = new StringBuilder("0x");
      for (byte b : (byte[]) value) {
        hex.append(String.format("%02x", b));
      }
      return hex.toString();
    } else if (value instanceof Row) {
      List<String> values = new ArrayList<>();
      for (Object field : ((Row) value).getValues()) {
        values.add(rawFormat(field));
      }
      return "{" + String.join(", ", values) + "}";
    } else if (value instanceof Iterable) {
      List<String> values = new ArrayList<>();
      for (Object element : (Iterable<?>) value) {
        values.add(rawFormat(element));
      }
      return "[" + String.join(", ", values) + "]";
    } else if (value instanceof Map) {
      List<String> entries = new ArrayList<>();
      for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
        entries.add(rawFormat(entry.getKey()) + "=" + rawFormat(entry.getValue()));
      }
      return "{" + String.join(", ", entries) + "}";
    }
    return String.valueOf(value);
  }

  /** Replaces line breaks, tabs and other control characters, which would break the layout. */
  private static String printable(String value) {
    StringBuilder printable = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      printable.append(Character.isISOControl(c) ? ' ' : c);
    }
    return printable.toString();
  }

  private static boolean isNumeric(Schema.FieldType type) {
    switch (type.getTypeName()) {
      case BYTE:
      case INT16:
      case INT32:
      case INT64:
      case DECIMAL:
      case FLOAT:
      case DOUBLE:
        return true;
      default:
        return false;
    }
  }

  /** How long the pipeline has run, as mm:ss, or h:mm:ss after an hour. */
  static String elapsed(long millis) {
    long seconds = millis / 1000;
    long hours = seconds / 3600;
    long minutes = (seconds / 60) % 60;
    return hours > 0
        ? String.format("%d:%02d:%02d", hours, minutes, seconds % 60)
        : String.format("%02d:%02d", minutes, seconds % 60);
  }

  /** The number of terminal columns that {@code text} takes up. */
  private static int width(String text) {
    return new AttributedString(text).columnLength();
  }

  /** Cuts {@code text} to {@code width} columns, ending it with "…" if it was longer. */
  private static String cut(String text, int width) {
    if (width(text) <= width) {
      return text;
    }
    if (width <= 0) {
      return "";
    }
    return new AttributedString(text).columnSubSequence(0, width - 1).toString() + ELLIPSIS;
  }

  private static String pad(String text, int width, boolean right) {
    String padding = spaces(width - width(text));
    return right ? padding + text : text + padding;
  }

  private static String spaces(int count) {
    return " ".repeat(Math.max(0, count));
  }

  private static AttributedString fit(String text, int width) {
    return new AttributedString(cut(text, width));
  }

  private static AttributedString blank() {
    return AttributedString.EMPTY;
  }
}
