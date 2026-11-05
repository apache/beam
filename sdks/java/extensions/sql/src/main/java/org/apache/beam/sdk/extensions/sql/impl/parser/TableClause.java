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

import com.google.auto.value.AutoValue;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.impl.parser.impl.BeamSqlParserImpl;
import org.apache.beam.sdk.extensions.sql.impl.parser.impl.TokenMgrError;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.avatica.util.Casing;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.runtime.CalciteContextException;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.runtime.CalciteException;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlCall;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlLiteral;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNodeList;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlWith;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlWithItem;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.util.SqlShuttle;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.Util;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The {@code WITH} clause of one table occurrence in a statement.
 *
 * <p>Calcite resolves a table occurrence by its name alone, so the parser folds the clause into the
 * last component of the table's name as canonical SQL text. {@link #decode} reads it back and
 * breaks it apart. {@code FROM cat.db.orders WITH ('k' = 1)} becomes the name {@code cat}, {@code
 * db}, {@code `orders` WITH ('k' = 1)} aliased as {@code orders}, and the INSERT target {@code INTO
 * orders WITH ('k' = 1)} becomes the name {@code INTO `orders` WITH ('k' = 1)}.
 *
 * <p>This is not a {@code SqlCall} like the DDL nodes in this package: Beam executes those itself,
 * while a table occurrence, including an INSERT target, goes through Calcite's validator and
 * SqlToRelConverter, which accept only their own node kinds and look tables up by name.
 */
@AutoValue
public abstract class TableClause {
  /** Whether a table occurrence is read (in FROM) or written (as an INSERT target). */
  public enum Role {
    READ,
    WRITE
  }

  /** The last component of the original table name. */
  public abstract String baseName();

  public abstract Role role();

  /**
   * The {@code WITH} options in clause order. Values are String, Long, Double, Boolean, List or
   * Map.
   */
  public abstract Map<String, Object> options();

  public static TableClause create(String baseName, Role role, Map<String, Object> options) {
    return new AutoValue_TableClause(baseName, role, ImmutableMap.copyOf(options));
  }

  /**
   * Returns the canonical SQL text of this table reference (e.g. "`orders` WITH ('k' = TRUE)" for a
   * read).
   */
  public String encode() {
    String table = "`" + baseName().replace("`", "``") + "`";
    return (role() == Role.WRITE ? "INTO " + table : table)
        + options().entrySet().stream()
            .map(option -> quote(option.getKey()) + " = " + toSql(option.getValue()))
            .collect(joining(", ", " WITH (", ")"));
  }

  /**
   * Returns the table reference encoded in a name component, or null if the component is a plain
   * name.
   */
  public static @Nullable TableClause decode(String name) {
    if (!name.startsWith("`") && !name.startsWith("INTO `")) {
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
    checkNotEncoded(tableRef);
    if (options.isEmpty()) {
      return tableRef;
    }
    SqlIdentifier name = plainTableName(tableRef);
    int last = name.names.size() - 1;
    String baseName = name.names.get(last);
    SqlIdentifier folded = name.setName(last, encode(create(baseName, role, options), name));
    return role == Role.WRITE
        ? folded
        : SqlStdOperatorTable.AS.createCall(
            name.getParserPosition(),
            folded,
            new SqlIdentifier(baseName, name.getComponentParserPosition(last)));
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
   * Rejects {@code WITH} options on references to the common table expressions of a WITH list,
   * which Calcite would resolve to catalog tables instead. Called by the parser for every query
   * with a WITH list.
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
            String.format("Table name '%s' is reserved for WITH options", last));
      }
    }
  }

  private static String encode(TableClause clause, SqlIdentifier name) {
    String encoded = clause.encode();
    if (!clause.equals(decode(encoded))) {
      throw error(name.getParserPosition(), "Unsupported WITH options: " + encoded);
    }
    return encoded;
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
                      "WITH options cannot be applied to common table expression '%s'",
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
