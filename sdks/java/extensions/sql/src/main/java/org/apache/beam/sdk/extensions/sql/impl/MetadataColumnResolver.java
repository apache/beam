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
package org.apache.beam.sdk.extensions.sql.impl;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.impl.parser.TableClause;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.ChangeRange;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.prepare.Prepare;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.runtime.CalciteContextException;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.JoinConditionType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlCall;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlJoin;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNodeList;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlOrderBy;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlSelect;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.Util;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Finds the metadata columns that a statement names on its {@code CHANGES(...)} reads, before
 * Calcite validates it, so that queries don't have to list them in a {@code WITH} option.
 *
 * <p>A name binds to a read when it's one of the metadata columns that the read's table lists in
 * {@link BeamSqlTable#getChangeMetadataColumns}, and isn't already a column of the read. {@code
 * c._change_type} binds to the read aliased {@code c}. {@code _change_type} binds to the reads in
 * its own SELECT that have it, or else to those of an enclosing SELECT. The resolver adds the names
 * to the read's {@link TableClause}, which passes them to {@link BeamSqlTable#forChanges}. It also
 * replaces {@code *} and {@code c.*} with the read's other columns, so {@code SELECT *} returns the
 * same columns as before.
 *
 * <p>The resolver can't tell whether a name it can't bind is an alias, a column of another table or
 * a typo, so the validator still reports unknown columns. {@link #explain} adds the metadata
 * columns that such a name could have meant.
 */
final class MetadataColumnResolver {
  private final CalciteCatalogReader catalogReader;
  private final SqlNameMatcher nameMatcher;
  private final List<ChangesRead> reads = new ArrayList<>();
  private final List<Scope> selects = new ArrayList<>();

  /** What to add to an error about an unknown column at a position of the statement. */
  private final Map<SqlParserPos, String> hints = new HashMap<>();

  /** Creates a resolver that looks tables up the way the validator does. */
  MetadataColumnResolver(CalciteCatalogReader catalogReader) {
    this.catalogReader = catalogReader;
    this.nameMatcher = catalogReader.nameMatcher();
  }

  /** Adds the metadata columns that a statement names to its reads, rewriting it in place. */
  void resolve(SqlNode statement) {
    reads.clear();
    selects.clear();
    hints.clear();
    SqlKind kind = statement.getKind();
    if (kind.belongsTo(SqlKind.QUERY) || kind == SqlKind.INSERT || kind == SqlKind.EXPLAIN) {
      visit(statement, null);
      reads.forEach(ChangesRead::addNamedColumns);
      selects.forEach(Scope::expandStars);
    }
  }

  /**
   * Returns the message of a validation error about an unknown column, followed by the metadata
   * columns that the column could have meant. Returns null for any other error.
   */
  @Nullable String explain(Throwable error) {
    for (Throwable cause : Throwables.getCausalChain(error)) {
      if (cause instanceof CalciteContextException) {
        CalciteContextException e = (CalciteContextException) cause;
        @Nullable Throwable reason = e.getCause();
        @Nullable String hint =
            hints.get(
                new SqlParserPos(
                    e.getPosLine(), e.getPosColumn(), e.getEndPosLine(), e.getEndPosColumn()));
        return hint == null
                || reason == null
                || !String.valueOf(reason.getMessage()).matches("Column '.*' not found in .*")
            ? null
            : e.getMessage() + ". " + hint;
      }
    }
    return null;
  }

  /** Visits a query or an expression, binding the names in it. */
  private void visit(@Nullable SqlNode node, @Nullable Scope scope) {
    if (node instanceof SqlSelect) {
      select((SqlSelect) node, scope);
    } else if (node instanceof SqlOrderBy && ((SqlOrderBy) node).query instanceof SqlSelect) {
      // The ORDER BY of a SELECT can name the columns of its FROM clause.
      SqlOrderBy orderBy = (SqlOrderBy) node;
      visit(orderBy.orderList, select((SqlSelect) orderBy.query, scope));
    } else if (node instanceof SqlIdentifier) {
      bind((SqlIdentifier) node, scope);
    } else if (node instanceof SqlCall) {
      for (@Nullable SqlNode operand : ((SqlCall) node).getOperandList()) {
        visit(operand, scope);
      }
    } else if (node instanceof SqlNodeList) {
      for (SqlNode child : (SqlNodeList) node) {
        visit(child, scope);
      }
    }
  }

  private Scope select(SqlSelect select, @Nullable Scope parent) {
    Scope scope = new Scope(select, parent);
    List<SqlNode> joinConditions = new ArrayList<>();
    from(select.getFrom(), scope, joinConditions);
    joinConditions.forEach(condition -> visit(condition, scope));
    for (@Nullable SqlNode operand : select.getOperandList()) {
      if (operand != select.getFrom()) {
        visit(operand, scope);
      }
    }
    selects.add(scope);
    return scope;
  }

  /** Adds the items of a FROM clause to its scope, and collects the conditions of its joins. */
  private void from(@Nullable SqlNode node, Scope scope, List<SqlNode> joinConditions) {
    if (node == null) {
      return;
    }
    switch (node.getKind()) {
      case JOIN:
        SqlJoin join = (SqlJoin) node;
        from(join.getLeft(), scope, joinConditions);
        from(join.getRight(), scope, joinConditions);
        @Nullable SqlNode condition = join.getCondition();
        if (join.isNatural() || join.getConditionType() == JoinConditionType.USING) {
          scope.mergesColumns = true;
        } else if (condition != null) {
          joinConditions.add(condition);
        }
        return;
      case TABLESAMPLE:
        from(((SqlCall) node).operand(0), scope, joinConditions);
        return;
      case AS:
        SqlCall as = (SqlCall) node;
        String alias = ((SqlIdentifier) as.operand(1)).getSimple();
        subquery(as.operand(0), scope);
        scope.items.add(new FromItem(alias, changesRead(as, alias)));
        return;
      case IDENTIFIER:
        scope.items.add(new FromItem(Util.last(((SqlIdentifier) node).names), null));
        return;
      default:
        // An item without an alias, such as UNNEST(...).
        subquery(node, scope);
        scope.items.add(new FromItem(null, null));
    }
  }

  /** Visits a subquery in FROM, which only sees the SELECTs around it unless it's LATERAL. */
  private void subquery(SqlNode item, Scope scope) {
    if (item.getKind() == SqlKind.LATERAL) {
      visit(((SqlCall) item).operand(0), scope);
    } else if (item.getKind().belongsTo(SqlKind.QUERY)) {
      visit(item, scope.parent);
    }
  }

  /** Returns the read that a FROM item names, or null if it isn't a read with metadata columns. */
  private @Nullable ChangesRead changesRead(SqlCall as, String alias) {
    // A column alias list has to name every column, so the read can't take more.
    if (!(as.operand(0) instanceof SqlIdentifier) || as.operandCount() > 2) {
      return null;
    }
    SqlIdentifier table = as.operand(0);
    int last = table.names.size() - 1;
    @Nullable TableClause clause = TableClause.decode(table.names.get(last));
    @Nullable ChangeRange changes = clause == null ? null : clause.changes();
    if (clause == null || changes == null) {
      return null;
    }
    try {
      Prepare.@Nullable PreparingTable found =
          catalogReader.getTable(table.setName(last, clause.baseName()).names);
      @Nullable BeamCalciteTable base = found == null ? null : found.unwrap(BeamCalciteTable.class);
      if (base == null || base.getBeamSqlTable().getChangeMetadataColumns().isEmpty()) {
        return null;
      }
      BeamSqlTable baseTable = base.getBeamSqlTable();
      ChangesRead read =
          new ChangesRead(
              as,
              table,
              clause,
              alias,
              baseTable
                  .forChanges(changes, clause.options(), ImmutableList.of())
                  .getSchema()
                  .getFieldNames(),
              baseTable.getChangeMetadataColumns());
      reads.add(read);
      return read;
    } catch (RuntimeException e) {
      // The validator looks the table up again and reports the error.
      return null;
    }
  }

  /** Binds a name to the reads in scope that have it as a metadata column. */
  private void bind(SqlIdentifier id, @Nullable Scope scope) {
    if (id.isStar() || id.names.size() > 2) {
      return;
    }
    boolean qualified = id.names.size() == 2;
    String name = Util.last(id.names);
    List<ChangesRead> inScope = new ArrayList<>();
    for (@Nullable Scope s = scope; s != null; s = s.parent) {
      List<ChangesRead> candidates = s.reads();
      if (qualified) {
        @Nullable FromItem item = s.item(id.names.get(0));
        if (item == null) {
          continue;
        }
        candidates = item.read == null ? ImmutableList.of() : ImmutableList.of(item.read);
      }
      if (candidates.stream().anyMatch(read -> read.hasColumn(name))) {
        return;
      }
      // If two reads have it, the validator reports the unqualified name as ambiguous.
      List<ChangesRead> binding =
          candidates.stream().filter(read -> read.hasMetadataColumn(name)).collect(toList());
      if (!binding.isEmpty()) {
        binding.forEach(read -> read.name(name));
        return;
      }
      inScope.addAll(candidates);
      if (qualified) {
        break;
      }
    }
    if (!inScope.isEmpty()) {
      String hint =
          inScope.stream()
              .map(
                  read ->
                      read
                          + " also has these metadata columns: "
                          + String.join(", ", read.metadataColumns))
              .collect(joining("; "));
      // The validator reports an unknown column at the name, or at the last part of a qualified
      // name.
      hints.put(id.getParserPosition(), hint);
      hints.put(id.getComponentParserPosition(id.names.size() - 1), hint);
    }
  }

  /** The FROM items of one SELECT, which the names in it can refer to. */
  private final class Scope {
    private final SqlSelect select;
    private final @Nullable Scope parent;
    private final List<FromItem> items = new ArrayList<>();

    /** Whether the FROM clause has a NATURAL JOIN or a JOIN ... USING, which merge columns. */
    private boolean mergesColumns;

    Scope(SqlSelect select, @Nullable Scope parent) {
      this.select = select;
      this.parent = parent;
    }

    List<ChangesRead> reads() {
      List<ChangesRead> reads = new ArrayList<>();
      for (FromItem item : items) {
        if (item.read != null) {
          reads.add(item.read);
        }
      }
      return reads;
    }

    @Nullable FromItem item(String alias) {
      for (FromItem item : items) {
        if (item.alias != null && nameMatcher.matches(item.alias, alias)) {
          return item;
        }
      }
      return null;
    }

    /** Replaces each star that would return named metadata columns with the columns to return. */
    void expandStars() {
      @Nullable SqlNodeList selectList = select.getSelectList();
      if (selectList == null || reads().stream().allMatch(read -> read.named.isEmpty())) {
        return;
      }
      List<SqlNode> expanded = new ArrayList<>();
      for (SqlNode item : selectList) {
        @Nullable List<SqlNode> columns =
            item instanceof SqlIdentifier ? expand((SqlIdentifier) item) : null;
        if (columns == null) {
          expanded.add(item);
        } else {
          expanded.addAll(columns);
        }
      }
      select.setSelectList(new SqlNodeList(expanded, selectList.getParserPosition()));
    }

    /** Returns the columns that replace a star, or null to keep the item. */
    private @Nullable List<SqlNode> expand(SqlIdentifier star) {
      if (!star.isStar()) {
        return null;
      }
      List<FromItem> starred;
      if (star.names.size() == 1) {
        starred = items;
      } else {
        @Nullable FromItem qualifier = star.names.size() == 2 ? item(star.names.get(0)) : null;
        starred = qualifier == null ? ImmutableList.of() : ImmutableList.of(qualifier);
      }
      @Nullable ChangesRead read = null;
      for (FromItem item : starred) {
        if (item.read != null && !item.read.named.isEmpty()) {
          read = item.read;
        }
      }
      if (read == null) {
        return null;
      }
      if (star.names.size() == 1
          && (mergesColumns || items.stream().anyMatch(item -> item.alias == null))) {
        throw new IllegalArgumentException(
            String.format(
                "* cannot leave out metadata column '%s' of %s, because the FROM clause has a"
                    + " NATURAL JOIN, a JOIN ... USING or an item without an alias. Write %s.* or"
                    + " list the columns instead",
                read.named.iterator().next(), read, read.alias));
      }
      SqlParserPos pos = star.getParserPosition();
      List<SqlNode> columns = new ArrayList<>();
      for (FromItem item : starred) {
        if (item.read != null && !item.read.named.isEmpty()) {
          for (String column : item.read.columns) {
            columns.add(
                new SqlIdentifier(
                    ImmutableList.of(item.read.alias, column),
                    null,
                    pos,
                    ImmutableList.of(pos, pos)));
          }
        } else if (item.alias != null) {
          columns.add(
              SqlIdentifier.star(
                  ImmutableList.of(item.alias, ""), pos, ImmutableList.of(pos, pos)));
        }
      }
      return columns;
    }
  }

  /** An item of a FROM clause, with its alias if it has one and its read if it's a read. */
  private static final class FromItem {
    private final @Nullable String alias;
    private final @Nullable ChangesRead read;

    FromItem(@Nullable String alias, @Nullable ChangesRead read) {
      this.alias = alias;
      this.read = read;
    }
  }

  /** A {@code CHANGES(...)} read of a table with metadata columns. */
  private final class ChangesRead {
    private final SqlCall as;
    private final SqlIdentifier table;
    private final TableClause clause;
    private final String alias;

    /** The read's columns, without the metadata columns that the statement names. */
    private final List<String> columns;

    private final List<String> metadataColumns;
    private final Set<String> named = new HashSet<>();

    ChangesRead(
        SqlCall as,
        SqlIdentifier table,
        TableClause clause,
        String alias,
        List<String> columns,
        List<String> metadataColumns) {
      this.as = as;
      this.table = table;
      this.clause = clause;
      this.alias = alias;
      this.columns = columns;
      this.metadataColumns = metadataColumns;
    }

    boolean hasColumn(String name) {
      return nameMatcher.indexOf(columns, name) >= 0;
    }

    boolean hasMetadataColumn(String name) {
      return nameMatcher.indexOf(metadataColumns, name) >= 0;
    }

    /** Records a metadata column that the statement names, spelled the way the table spells it. */
    void name(String name) {
      named.add(metadataColumns.get(nameMatcher.indexOf(metadataColumns, name)));
    }

    /** Adds the named metadata columns to the read's table name, in the table's order. */
    void addNamedColumns() {
      if (!named.isEmpty()) {
        List<String> ordered = metadataColumns.stream().filter(named::contains).collect(toList());
        String name = clause.withMetadataColumns(ordered).encode();
        as.setOperand(0, table.setName(table.names.size() - 1, name));
      }
    }

    /** Describes the read the way the query wrote it, such as CHANGES(db.orders) AS o. */
    @Override
    public String toString() {
      List<String> names = new ArrayList<>(table.names.subList(0, table.names.size() - 1));
      names.add(clause.baseName());
      String read = "CHANGES(" + String.join(".", names) + ")";
      return nameMatcher.matches(alias, clause.baseName()) ? read : read + " AS " + alias;
    }
  }
}
