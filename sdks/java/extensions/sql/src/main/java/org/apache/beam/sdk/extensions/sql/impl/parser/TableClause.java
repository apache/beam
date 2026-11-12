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
package org.apache.beam.sdk.extensions.sql.impl.parser;

import static java.util.stream.Collectors.joining;
import static org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.Static.RESOURCE;

import com.google.auto.value.AutoValue;
import java.io.StringReader;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.beam.sdk.extensions.sql.impl.parser.impl.BeamSqlParserImpl;
import org.apache.beam.sdk.extensions.sql.impl.parser.impl.TokenMgrError;
import org.apache.beam.sdk.extensions.sql.meta.ChangeRange;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.avatica.util.Casing;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.runtime.CalciteContextException;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.runtime.CalciteException;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlCall;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlFunction;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlLiteral;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNodeList;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlUnknownLiteral;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlUtil;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlWith;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlWithItem;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.util.SqlShuttle;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.TimestampString;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.Util;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * The {@code CHANGES(...)} call and {@code WITH} clause of one table occurrence in a statement.
 *
 * <p>Calcite resolves a table occurrence by its name alone, so the parser folds them into the last
 * component of the table's name as canonical SQL text. {@link #decode} reads it back and breaks it
 * apart. {@code FROM cat.db.orders WITH ('k' = 1)} becomes the name {@code cat}, {@code db}, {@code
 * `orders` WITH ('k' = 1)} aliased as {@code orders}. {@code FROM CHANGES(cat.db.orders, 5)}
 * becomes {@code cat}, {@code db}, {@code CHANGES(`orders`, start_version => 5)}, also aliased as
 * {@code orders}. The INSERT target {@code INTO orders WITH ('k' = 1)} becomes the name {@code INTO
 * `orders` WITH ('k' = 1)}.
 *
 * <p>This is not a {@code SqlCall} like the DDL nodes in this package: Beam executes those itself,
 * while a table occurrence, including an INSERT target, goes through Calcite's validator and
 * SqlToRelConverter, which accept only their own node kinds and look tables up by name.
 */
@AutoValue
public abstract class TableClause {
  private static final String CHANGES_FORM = "CHANGES(table [, start [, end]])";
  private static final List<String> BOUND_PARAMETERS =
      ImmutableList.of("start_version", "end_version", "start_timestamp", "end_timestamp");

  /** An ISO 8601 date with an optional time and offset, such as 2026-10-01T12:30:00.5-05:00. */
  private static final DateTimeFormatter ISO_TIMESTAMP =
      new DateTimeFormatterBuilder()
          .appendValue(ChronoField.YEAR, 4)
          .appendPattern("-MM-dd['T'HH:mm[:ss[")
          .appendFraction(ChronoField.NANO_OF_SECOND, 1, 3, true)
          .appendPattern("]][XXX]]")
          .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
          .parseDefaulting(ChronoField.OFFSET_SECONDS, 0)
          .toFormatter()
          .withResolverStyle(ResolverStyle.STRICT);

  /** Seconds with more than 3 fractional digits. */
  private static final Pattern SUB_MILLIS = Pattern.compile(":\\d\\d\\.\\d{4}");

  /** Whether a table occurrence is read (in FROM) or written (as an INSERT target). */
  public enum Role {
    READ,
    WRITE
  }

  /** The last component of the original table name. */
  public abstract String baseName();

  public abstract Role role();

  /** The range of a {@code CHANGES(...)} call, or null without one. */
  public abstract @Nullable ChangeRange changes();

  /**
   * The {@code WITH} options in clause order. Values are String, Long, Double, Boolean, List or
   * Map.
   */
  public abstract Map<String, Object> options();

  public static TableClause create(String baseName, Role role, Map<String, Object> options) {
    return new AutoValue_TableClause(baseName, role, null, ImmutableMap.copyOf(options));
  }

  /** Returns the read of a table through {@code CHANGES(...)}, with its {@code WITH} options. */
  public static TableClause createChanges(
      String baseName, ChangeRange changes, Map<String, Object> options) {
    return new AutoValue_TableClause(baseName, Role.READ, changes, ImmutableMap.copyOf(options));
  }

  /**
   * Returns the canonical SQL text of this table reference (e.g. "CHANGES(`orders`, start_version
   * => 1, end_version => 5) WITH ('streaming' = TRUE)" for a read).
   */
  public String encode() {
    String table = "`" + baseName().replace("`", "``") + "`";
    StringBuilder sql = new StringBuilder();
    @Nullable ChangeRange changes = changes();
    if (role() == Role.WRITE) {
      sql.append("INTO ").append(table);
    } else if (changes == null) {
      sql.append(table);
    } else {
      sql.append("CHANGES(").append(table);
      appendBound(sql, "start_version", changes.startVersion());
      appendBound(sql, "end_version", changes.endVersion());
      appendBound(sql, "start_timestamp", changes.startTimestamp());
      appendBound(sql, "end_timestamp", changes.endTimestamp());
      sql.append(')');
    }
    if (!options().isEmpty()) {
      sql.append(
          options().entrySet().stream()
              .map(option -> quote(option.getKey()) + " = " + toSql(option.getValue()))
              .collect(joining(", ", " WITH (", ")")));
    }
    return sql.toString();
  }

  /**
   * Returns the table reference encoded in a name component, or null if the component is a plain
   * name.
   */
  public static @Nullable TableClause decode(String name) {
    if (!name.startsWith("`") && !name.startsWith("INTO `") && !name.startsWith("CHANGES(`")) {
      return null;
    }
    BeamSqlParserImpl parser = new BeamSqlParserImpl(new StringReader(name));
    parser.switchTo(SqlAbstractParserImpl.LexicalState.BTID);
    parser.setQuotedCasing(Casing.UNCHANGED);
    parser.setUnquotedCasing(Casing.UNCHANGED);
    parser.setIdentifierMaxLength(Integer.MAX_VALUE);
    parser.setConformance(SqlConformanceEnum.DEFAULT);
    try {
      TableClause clause = parser.TableClauseEof();
      return clause.encode().equals(name) ? clause : null;
    } catch (Exception | TokenMgrError e) {
      return null;
    }
  }

  /**
   * Folds the {@code WITH} clause of a table in FROM or of an INSERT target into its name. A read
   * is aliased as the base name so references like {@code orders.id} keep working. Called by the
   * parser.
   */
  public static SqlNode fold(SqlNode tableRef, Role role, Map<String, Object> options) {
    return fold(tableRef, role, null, options);
  }

  /**
   * Folds a {@code CHANGES(...)} call in FROM and its {@code WITH} clause into the name of the
   * table it reads. Returns any other table function call unchanged, and rejects a {@code WITH}
   * clause after it. Called by the parser.
   */
  public static SqlNode foldChanges(SqlNode tableFunction, Map<String, Object> options) {
    SqlCall call = ((SqlCall) tableFunction).operand(0);
    if (!isChanges(call)) {
      if (!options.isEmpty()) {
        throw error(
            call.getParserPosition(), "WITH cannot follow a table function other than CHANGES");
      }
      return tableFunction;
    }
    return fold(changesTable(call), Role.READ, changeRange(call), options);
  }

  /**
   * Returns the read of a {@code CHANGES(...)} call and its {@code WITH} clause. Called by the
   * parser to decode them.
   */
  public static TableClause readChanges(SqlNode tableFunction, Map<String, Object> options) {
    SqlCall call = ((SqlCall) tableFunction).operand(0);
    return createChanges(Util.last(changesTable(call).names), changeRange(call), options);
  }

  /**
   * Rejects {@code TABLE(CHANGES(...))}, so that {@code FROM CHANGES(...)} is the one way to read
   * changes. Called by the parser.
   */
  public static void checkNotChanges(SqlNode tableFunction) {
    SqlCall call = ((SqlCall) tableFunction).operand(0);
    if (isChanges(call)) {
      throw error(
          call.getParserPosition(),
          "CHANGES cannot be called in TABLE(...), only as FROM " + CHANGES_FORM);
    }
  }

  /**
   * Replaces the implicit alias of a folded read with the alias the statement gives it, so that
   * {@code AS(AS(folded, base), alias, columns...)} becomes {@code AS(folded, alias, columns...)},
   * also under TABLESAMPLE. Called by the parser at the end of a table reference.
   */
  public static SqlNode collapseImplicitAlias(SqlNode tableRef) {
    if (tableRef.getKind() == SqlKind.TABLESAMPLE) {
      SqlCall sample = (SqlCall) tableRef;
      sample.setOperand(0, collapseImplicitAlias(sample.operand(0)));
      return sample;
    }
    if (tableRef.getKind() != SqlKind.AS || !isFoldedRead(((SqlCall) tableRef).operand(0))) {
      return tableRef;
    }
    SqlCall alias = (SqlCall) tableRef;
    List<SqlNode> operands = new ArrayList<>(alias.getOperandList());
    operands.set(0, ((SqlCall) alias.operand(0)).operand(0));
    return SqlStdOperatorTable.AS.createCall(alias.getParserPosition(), operands);
  }

  /**
   * Rejects {@code CHANGES(...)} and {@code WITH} on references to the common table expressions of
   * a WITH list, which Calcite would resolve to catalog tables instead. Called by the parser for
   * every query with a WITH list.
   */
  public static void checkCteReferences(@Nullable SqlNodeList withList, SqlNode body) {
    if (withList == null) {
      return;
    }
    Set<String> inScope = new HashSet<>();
    for (SqlNode node : withList) {
      SqlWithItem item = (SqlWithItem) node;
      if (item.recursive.booleanValue()) {
        inScope.add(item.name.getSimple());
      }
      checkNoClauseOn(inScope, item.query);
      inScope.add(item.name.getSimple());
    }
    checkNoClauseOn(inScope, body);
  }

  /**
   * Also rejects them in the ORDER BY of a WITH query, which the parser attaches after {@link
   * #checkCteReferences} and Calcite validates in the WITH's scope. Called by the parser.
   */
  public static void checkCteReferencesInOrderBy(SqlNode query, @Nullable SqlNodeList orderBy) {
    if (query instanceof SqlWith && orderBy != null) {
      checkCteReferences(((SqlWith) query).withList, orderBy);
    }
  }

  /**
   * Adds a {@code WITH} option, converting its value to a plain Java value. Called by the parser.
   */
  public static void addOption(
      Map<String, Object> options, String key, SqlParserPos keyPos, SqlNode value) {
    if (options.containsKey(key)) {
      throw error(keyPos, String.format("Duplicate option '%s'", key));
    }
    options.put(key, toJava(key, value));
  }

  /** Rejects a table name that is written in the encoded form of a {@link TableClause}. */
  private static void checkNotEncoded(SqlNode tableRef) {
    SqlNode name = tableRef;
    while (name instanceof SqlCall) { // table hints, EXTEND, OVER, FOR SYSTEM_TIME AS OF
      name = ((SqlCall) name).operand(0);
    }
    if (name instanceof SqlIdentifier) {
      String last = Util.last(((SqlIdentifier) name).names);
      if (decode(last) != null) {
        throw error(
            name.getParserPosition(),
            String.format("Table name '%s' is reserved for CHANGES and WITH", last));
      }
    }
  }

  /** Folds an optional {@code CHANGES(...)} range and the {@code WITH} options into the name. */
  private static SqlNode fold(
      SqlNode tableRef, Role role, @Nullable ChangeRange changes, Map<String, Object> options) {
    checkNotEncoded(tableRef);
    if (changes == null && options.isEmpty()) {
      return tableRef;
    }
    SqlIdentifier name = plainTableName(tableRef);
    int last = name.names.size() - 1;
    String baseName = name.names.get(last);
    TableClause clause =
        new AutoValue_TableClause(baseName, role, changes, ImmutableMap.copyOf(options));
    SqlIdentifier folded = name.setName(last, encode(clause, name));
    return role == Role.WRITE
        ? folded
        : SqlStdOperatorTable.AS.createCall(
            name.getParserPosition(),
            folded,
            new SqlIdentifier(baseName, name.getComponentParserPosition(last)));
  }

  private static String encode(TableClause clause, SqlIdentifier name) {
    String encoded = clause.encode();
    if (!clause.equals(decode(encoded))) {
      throw error(name.getParserPosition(), "Unsupported CHANGES or WITH: " + encoded);
    }
    return encoded;
  }

  private static boolean isChanges(SqlCall call) {
    @Nullable SqlIdentifier function = ((SqlFunction) call.getOperator()).getSqlIdentifier();
    return function != null
        && function.isSimple()
        && function.getSimple().equalsIgnoreCase("CHANGES");
  }

  private static SqlIdentifier changesTable(SqlCall call) {
    SqlNode table = call.operandCount() == 0 ? call : call.operand(0);
    if (!(table instanceof SqlIdentifier) || ((SqlIdentifier) table).isStar()) {
      throw error(
          table.getParserPosition(),
          "CHANGES expects a table name as its first argument: " + CHANGES_FORM);
    }
    return (SqlIdentifier) table;
  }

  /** Converts the bounds that follow the table name of a {@code CHANGES(...)} call. */
  private static ChangeRange changeRange(SqlCall call) {
    @Nullable Object[] bounds = new @Nullable Object[2];
    boolean[] given = new boolean[2];
    int positional = 0;
    boolean named = false;
    for (SqlNode arg : Util.skip(call.getOperandList())) {
      @Nullable String parameter = null;
      SqlNode value = arg;
      int side;
      if (arg.getKind() == SqlKind.ARGUMENT_ASSIGNMENT) {
        SqlIdentifier name = ((SqlCall) arg).operand(1);
        parameter = name.getSimple().toLowerCase(Locale.ROOT);
        if (!BOUND_PARAMETERS.contains(parameter)) {
          throw error(
              name.getParserPosition(),
              String.format(
                  "Unknown CHANGES parameter '%s'. Accepted parameters: %s",
                  name.getSimple(), String.join(", ", BOUND_PARAMETERS)));
        }
        side = parameter.startsWith("start_") ? 0 : 1;
        value = ((SqlCall) arg).operand(0);
        named = true;
      } else if (named) {
        throw error(
            arg.getParserPosition(), "A positional CHANGES bound cannot follow a named one");
      } else if (positional == 2) {
        throw error(
            arg.getParserPosition(), "CHANGES takes at most 2 positional bounds: " + CHANGES_FORM);
      } else {
        side = positional++;
      }
      if (given[side]) {
        throw error(
            arg.getParserPosition(),
            String.format("CHANGES %s bound is given twice", side == 0 ? "start" : "end"));
      }
      given[side] = true;
      @Nullable Object bound = bound(parameter, value);
      @Nullable Object other = bounds[1 - side];
      if (bound != null && other != null && bound.getClass() != other.getClass()) {
        throw error(
            value.getParserPosition(),
            "CHANGES bounds must both be versions or both be timestamps");
      }
      bounds[side] = bound;
    }
    return bounds[0] instanceof Instant || bounds[1] instanceof Instant
        ? ChangeRange.timestamps((Instant) bounds[0], (Instant) bounds[1])
        : ChangeRange.versions((Long) bounds[0], (Long) bounds[1]);
  }

  /**
   * Converts a {@code CHANGES(...)} bound to a version (Long), a timestamp (Instant) or null for
   * NULL. A named bound takes only the kind its parameter names.
   */
  private static @Nullable Object bound(@Nullable String parameter, SqlNode node) {
    SqlNode value = node.getKind() == SqlKind.LITERAL_CHAIN ? SqlLiteral.unchain(node) : node;
    boolean takesVersion = parameter == null || parameter.endsWith("_version");
    boolean takesTimestamp = parameter == null || parameter.endsWith("_timestamp");
    if (SqlUtil.isNullLiteral(value, false)) {
      return null;
    } else if (takesVersion
        && value instanceof SqlNumericLiteral
        && ((SqlNumericLiteral) value).isExact()
        && ((SqlNumericLiteral) value).isInteger()) {
      return version((SqlNumericLiteral) value);
    } else if (takesTimestamp && value instanceof SqlCharStringLiteral) {
      return timestamp(
          ((SqlCharStringLiteral) value).getValueAs(String.class), value.getParserPosition());
    } else if (takesTimestamp
        && value instanceof SqlUnknownLiteral
        && ((SqlUnknownLiteral) value).tag.equals("TIMESTAMP")) {
      return timestamp(((SqlUnknownLiteral) value).getValue(), value.getParserPosition());
    }
    throw error(
        value.getParserPosition(),
        parameter == null
            ? "A CHANGES bound must be a version (an integer literal),"
                + " a timestamp (a string or TIMESTAMP literal) or NULL"
            : String.format(
                "CHANGES parameter '%s' must be %s or NULL",
                parameter, takesVersion ? "an integer literal" : "a string or TIMESTAMP literal"));
  }

  private static long version(SqlNumericLiteral literal) {
    BigDecimal version = literal.getValueAs(BigDecimal.class);
    if (version.signum() < 0 || version.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0) {
      throw error(
          literal.getParserPosition(),
          String.format("CHANGES version %s must be between 0 and %d", literal, Long.MAX_VALUE));
    }
    return version.longValue();
  }

  /** Converts an ISO 8601 timestamp, read as UTC without an offset. A space may replace the T. */
  private static Instant timestamp(String timestamp, SqlParserPos pos) {
    if (SUB_MILLIS.matcher(timestamp).find()) {
      throw illegalTimestamp(timestamp, pos, "CHANGES supports at most 3 fractional digits");
    }
    OffsetDateTime utc;
    try {
      utc =
          ISO_TIMESTAMP
              .parse(timestamp.trim().replace(' ', 'T'), OffsetDateTime::from)
              .withOffsetSameInstant(ZoneOffset.UTC);
    } catch (DateTimeParseException e) {
      // A well-formed but invalid value, such as 2026-02-30, has a cause that names the field.
      throw illegalTimestamp(
          timestamp,
          pos,
          e.getCause() != null
              ? String.valueOf(e.getCause().getMessage())
              : "expected yyyy-MM-dd, optionally followed by T or a space, HH:mm[:ss[.SSS]]"
                  + " and an optional Z, +hh:mm or -hh:mm");
    }
    // TimestampString keeps the last 4 digits of the year.
    if (utc.getYear() < 0 || utc.getYear() > 9999) {
      throw illegalTimestamp(timestamp, pos, "CHANGES supports years 0000 to 9999 in UTC");
    }
    return new Instant(utc.toInstant().toEpochMilli());
  }

  private static void checkNoClauseOn(Set<String> ctes, SqlNode query) {
    query.accept(
        new SqlShuttle() {
          @Override
          public SqlNode visit(SqlIdentifier id) {
            TableClause clause = id.isSimple() ? decode(id.getSimple()) : null;
            if (clause != null && ctes.contains(clause.baseName())) {
              throw error(
                  id.getParserPosition(),
                  String.format(
                      "CHANGES and WITH cannot be applied to common table expression '%s'",
                      clause.baseName()));
            }
            return id;
          }
        });
  }

  private static SqlIdentifier plainTableName(SqlNode tableRef) {
    if (tableRef instanceof SqlIdentifier) {
      return (SqlIdentifier) tableRef;
    }
    throw error(
        tableRef.getParserPosition(),
        "WITH cannot be combined with table hints, EXTEND, FOR SYSTEM_TIME AS OF or OVER");
  }

  private static boolean isFoldedRead(SqlNode node) {
    if (node.getKind() != SqlKind.AS) {
      return false;
    }
    SqlNode table = ((SqlCall) node).operand(0);
    if (!(table instanceof SqlIdentifier)) {
      return false;
    }
    TableClause clause = decode(Util.last(((SqlIdentifier) table).names));
    return clause != null && clause.role() == Role.READ;
  }

  private static Object toJava(String key, SqlNode node) {
    SqlNode value = node.getKind() == SqlKind.LITERAL_CHAIN ? SqlLiteral.unchain(node) : node;
    if (value instanceof SqlCharStringLiteral) {
      return ((SqlCharStringLiteral) value).getValueAs(String.class);
    }
    if (value instanceof SqlNumericLiteral) {
      return number(key, (SqlNumericLiteral) value);
    }
    if (value instanceof SqlLiteral) {
      Object literal = ((SqlLiteral) value).getValue();
      if (literal instanceof Boolean) {
        return literal;
      }
    }
    if (value.getKind() == SqlKind.ARRAY_VALUE_CONSTRUCTOR) {
      SqlCall elements = (SqlCall) value;
      ImmutableList.Builder<Object> list = ImmutableList.builder();
      for (int i = 0; i < elements.operandCount(); i++) {
        list.add(toJava(key, elements.operand(i)));
      }
      return list.build();
    }
    if (value.getKind() == SqlKind.MAP_VALUE_CONSTRUCTOR
        && ((SqlCall) value).operandCount() % 2 == 0) {
      SqlCall entries = (SqlCall) value;
      Map<Object, Object> map = new LinkedHashMap<>();
      for (int i = 0; i < entries.operandCount(); i += 2) {
        Object mapKey = toJava(key, entries.operand(i));
        if (map.put(mapKey, toJava(key, entries.operand(i + 1))) != null) {
          throw error(
              entries.operand(i).getParserPosition(),
              String.format("Duplicate MAP key %s in option '%s'", toSql(mapKey), key));
        }
      }
      return ImmutableMap.copyOf(map);
    }
    throw error(
        node.getParserPosition(),
        String.format(
            "Option '%s' must be a string, numeric or boolean literal,"
                + " or an ARRAY[...] or MAP[...] of them",
            key));
  }

  private static Object number(String key, SqlNumericLiteral literal) {
    BigDecimal value = literal.getValueAs(BigDecimal.class);
    if (literal.isExact() && literal.isInteger()) {
      try {
        return value.longValueExact();
      } catch (ArithmeticException e) {
        throw outOfRange(key, literal);
      }
    }
    double number = value.doubleValue();
    if (Double.isInfinite(number) || (number == 0 && value.signum() != 0)) {
      throw outOfRange(key, literal);
    }
    return number;
  }

  private static CalciteContextException outOfRange(String key, SqlNumericLiteral literal) {
    return error(
        literal.getParserPosition(),
        String.format("Option '%s' value %s is out of range", key, literal));
  }

  private static void appendBound(StringBuilder sql, String parameter, @Nullable Object bound) {
    if (bound == null) {
      return;
    }
    sql.append(", ").append(parameter).append(" => ");
    if (bound instanceof Instant) {
      long millis = ((Instant) bound).getMillis();
      sql.append("TIMESTAMP ")
          .append(quote(TimestampString.fromMillisSinceEpoch(millis).toString()));
    } else {
      sql.append(bound);
    }
  }

  private static String toSql(@Nullable Object value) {
    if (value instanceof String) {
      return quote((String) value);
    }
    if (value instanceof List) {
      return ((List<?>) value)
          .stream().map(TableClause::toSql).collect(joining(", ", "ARRAY[", "]"));
    }
    if (value instanceof Map) {
      return ((Map<?, ?>) value)
          .entrySet().stream()
              .map(entry -> toSql(entry.getKey()) + ", " + toSql(entry.getValue()))
              .collect(joining(", ", "MAP[", "]"));
    }
    if (value instanceof Boolean) {
      return (Boolean) value ? "TRUE" : "FALSE";
    }
    return String.valueOf(value);
  }

  private static String quote(String value) {
    return "'" + value.replace("'", "''") + "'";
  }

  private static CalciteContextException illegalTimestamp(
      String timestamp, SqlParserPos pos, String reason) {
    return SqlUtil.newContextException(
        pos, RESOURCE.illegalLiteral("TIMESTAMP", timestamp, reason));
  }

  private static CalciteContextException error(SqlParserPos pos, String message) {
    return new CalciteContextException(
        message,
        new CalciteException(message, null),
        pos.getLineNum(),
        pos.getColumnNum(),
        pos.getEndLineNum(),
        pos.getEndColumnNum());
  }
}
