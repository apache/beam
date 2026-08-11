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

import static org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.Static.RESOURCE;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.CatalogManagerSchema;
import org.apache.beam.sdk.extensions.sql.meta.catalog.Catalog;
import org.apache.beam.sdk.extensions.sql.meta.catalog.Procedure;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlCall;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlLiteral;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlUtil;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlWriter;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.Pair;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@code CALL} statement invoking a stored {@link Procedure}:
 *
 * <pre>{@code
 * CALL [catalog_name.][system.]procedure_name(arg1, arg2, ...)
 * CALL [catalog_name.][system.]procedure_name(param2 => arg2, param1 => arg1, ...)
 * }</pre>
 *
 * <p>Arguments are passed either by position or by name, the namespace component (if any) must
 * be {@code system}, and procedure names
 * resolve case-insensitively. Procedures are provided by the target {@link Catalog} via {@link
 * Catalog#loadProcedure(String)}.
 */
public class SqlCallProcedure extends SqlCall implements BeamSqlParser.ExecutableStatement {
  private static final SqlOperator OPERATOR = new SqlSpecialOperator("CALL", SqlKind.OTHER_DDL);
  private static final String SYSTEM_NAMESPACE = "system";

  private final SqlIdentifier procedureName;
  private final List<SqlNode> args;

  public SqlCallProcedure(SqlParserPos pos, SqlIdentifier procedureName, List<SqlNode> args) {
    super(pos);
    this.procedureName = procedureName;
    this.args = ImmutableList.copyOf(args);
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.<SqlNode>builder().add(procedureName).addAll(args).build();
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CALL");
    procedureName.unparse(writer, leftPrec, rightPrec);
    SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
    for (SqlNode arg : args) {
      writer.sep(",");
      if (arg.getKind() == SqlKind.ARGUMENT_ASSIGNMENT) {
        SqlCall assignment = (SqlCall) arg;
        assignment.operand(1).unparse(writer, 0, 0);
        writer.keyword("=>");
        assignment.operand(0).unparse(writer, 0, 0);
      } else {
        arg.unparse(writer, 0, 0);
      }
    }
    writer.endList(frame);
  }

  @Override
  public void execute(CalcitePrepare.Context context) {
    List<String> path = procedureName.names;
    @Nullable String catalogName = null;
    String procName;
    switch (path.size()) {
      case 1:
        procName = path.get(0);
        break;
      case 2:
        checkSystemNamespace(path.get(0));
        procName = path.get(1);
        break;
      case 3:
        catalogName = path.get(0);
        checkSystemNamespace(path.get(1));
        procName = path.get(2);
        break;
      default:
        throw invalidProcedureNameException();
    }

    Catalog catalog = resolveCatalog(context, catalogName, procName);

    // Procedure resolution is case-insensitive
    String lookupName = procName.toLowerCase(Locale.ROOT);
    @Nullable Procedure procedure = catalog.loadProcedure(lookupName);
    if (procedure == null) {
      throw SqlUtil.newContextException(
          procedureName.getParserPosition(),
          RESOURCE.internal(
              String.format(
                  "Procedure '%s' not found in catalog '%s' (type '%s').",
                  lookupName, catalog.name(), catalog.type())));
    }

    procedure.execute(bindArguments(procedure));
  }

  /** Resolves the target {@link Catalog}, defaulting to the currently active one. */
  private Catalog resolveCatalog(
      CalcitePrepare.Context context, @Nullable String catalogName, String procName) {
    final Pair<CalciteSchema, String> pair =
        SqlDdlNodes.schema(
            context, true, new SqlIdentifier(procName, procedureName.getParserPosition()));
    org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Schema schema =
        pair.left.schema;
    if (!(schema instanceof CatalogManagerSchema)) {
      throw SqlUtil.newContextException(
          procedureName.getParserPosition(),
          RESOURCE.internal(
              "Attempting to execute 'CALL' with unexpected Calcite Schema of type "
                  + schema.getClass()));
    }
    CatalogManagerSchema catalogManagerSchema = (CatalogManagerSchema) schema;

    if (catalogName == null) {
      return catalogManagerSchema.getCurrentCatalogSchema().getCatalog();
    }
    for (Catalog catalog : catalogManagerSchema.catalogs()) {
      if (catalog.name().equals(catalogName)) {
        return catalog;
      }
    }
    throw SqlUtil.newContextException(
        procedureName.getParserPosition(),
        RESOURCE.internal(String.format("Catalog '%s' not found.", catalogName)));
  }

  private void checkSystemNamespace(String namespace) {
    if (!namespace.equalsIgnoreCase(SYSTEM_NAMESPACE)) {
      throw invalidProcedureNameException();
    }
  }

  private RuntimeException invalidProcedureNameException() {
    return SqlUtil.newContextException(
        procedureName.getParserPosition(),
        RESOURCE.internal(
            String.format(
                "Invalid procedure name '%s': expected 'procedure_name', "
                    + "'system.procedure_name', or 'catalog.system.procedure_name'.",
                String.join(".", procedureName.names))));
  }

  /**
   * Validates the arguments against the procedure's declared parameters and binds them to a {@link
   * Row} over {@link Procedure#parameters()}.
   */
  private Row bindArguments(Procedure procedure) {
    Schema parameters = procedure.parameters();
    String procName = procedure.name();
    int paramCount = parameters.getFieldCount();

    boolean hasNamed = args.stream().anyMatch(a -> a.getKind() == SqlKind.ARGUMENT_ASSIGNMENT);
    boolean hasPositional = args.stream().anyMatch(a -> a.getKind() != SqlKind.ARGUMENT_ASSIGNMENT);
    if (hasNamed && hasPositional) {
      throw SqlUtil.newContextException(
          procedureName.getParserPosition(),
          RESOURCE.internal("Mixing named and positional arguments is not supported."));
    }

    // Map of parameter name -> argument value node.
    Map<String, SqlNode> providedArgs = new HashMap<>();
    if (hasNamed) {
      for (SqlNode arg : args) {
        SqlCall assignment = (SqlCall) arg;
        SqlNode value = assignment.operand(0);
        SqlIdentifier nameId = assignment.operand(1);
        // Parameter names resolve case-insensitively; parameters are declared in lower_snake_case.
        String name = nameId.getSimple().toLowerCase(Locale.ROOT);
        if (!parameters.hasField(name)) {
          throw SqlUtil.newContextException(
              nameId.getParserPosition(),
              RESOURCE.internal(
                  String.format(
                      "Procedure '%s' does not accept an argument named '%s'. "
                          + "Expected parameters: %s",
                      procName, nameId.getSimple(), parameters.getFieldNames())));
        }
        if (providedArgs.put(name, value) != null) {
          throw SqlUtil.newContextException(
              nameId.getParserPosition(),
              RESOURCE.internal(
                  String.format(
                      "Duplicate argument name '%s' in call to procedure '%s'.", name, procName)));
        }
      }
    } else {
      if (args.size() > paramCount) {
        throw SqlUtil.newContextException(
            procedureName.getParserPosition(),
            RESOURCE.internal(
                String.format(
                    "Too many arguments for procedure '%s': expected at most %s, got %s.",
                    procName, paramCount, args.size())));
      }
      for (int i = 0; i < args.size(); i++) {
        providedArgs.put(parameters.getField(i).getName(), args.get(i));
      }
    }

    List<String> missingRequired = new ArrayList<>();
    @Nullable Object[] values = new @Nullable Object[paramCount];
    for (int i = 0; i < paramCount; i++) {
      Schema.Field field = parameters.getField(i);
      @Nullable SqlNode valueNode = providedArgs.get(field.getName());
      if (valueNode == null) {
        if (!field.getType().getNullable()) {
          missingRequired.add(field.getName());
        }
        continue;
      }
      @Nullable Object value = convertLiteral(procName, field, valueNode);
      if (value == null && !field.getType().getNullable()) {
        throw SqlUtil.newContextException(
            valueNode.getParserPosition(),
            RESOURCE.internal(
                String.format(
                    "Argument '%s' of procedure '%s' is required and cannot be NULL.",
                    field.getName(), procName)));
      }
      values[i] = value;
    }
    if (!missingRequired.isEmpty()) {
      throw SqlUtil.newContextException(
          procedureName.getParserPosition(),
          RESOURCE.internal(
              String.format(
                  "Missing required argument(s) for procedure '%s': %s.",
                  procName, missingRequired)));
    }

    return Row.withSchema(parameters).addValues(Arrays.asList(values)).build();
  }

  /** Converts a literal argument node to a Java value of the parameter's declared type. */
  private @Nullable Object convertLiteral(String procName, Schema.Field field, SqlNode node) {
    if (!(node instanceof SqlLiteral)) {
      throw SqlUtil.newContextException(
          node.getParserPosition(),
          RESOURCE.internal(
              String.format(
                  "Argument '%s' of procedure '%s' must be a literal value.",
                  field.getName(), procName)));
    }
    SqlLiteral literal = (SqlLiteral) node;
    if (literal.getTypeName() == SqlTypeName.NULL) {
      return null;
    }

    Schema.TypeName typeName = field.getType().getTypeName();
    try {
      switch (typeName) {
        case STRING:
          checkLiteralType(procName, field, literal, literal.getTypeName() == SqlTypeName.CHAR);
          return literal.getValueAs(String.class);
        case BOOLEAN:
          checkLiteralType(procName, field, literal, literal.getTypeName() == SqlTypeName.BOOLEAN);
          return literal.getValueAs(Boolean.class);
        case BYTE:
          return exactNumeric(procName, field, literal).byteValueExact();
        case INT16:
          return exactNumeric(procName, field, literal).shortValueExact();
        case INT32:
          return exactNumeric(procName, field, literal).intValueExact();
        case INT64:
          return exactNumeric(procName, field, literal).longValueExact();
        case FLOAT:
          checkLiteralType(procName, field, literal, literal instanceof SqlNumericLiteral);
          return literal.getValueAs(BigDecimal.class).floatValue();
        case DOUBLE:
          checkLiteralType(procName, field, literal, literal instanceof SqlNumericLiteral);
          return literal.getValueAs(BigDecimal.class).doubleValue();
        case DECIMAL:
          checkLiteralType(procName, field, literal, literal instanceof SqlNumericLiteral);
          return literal.getValueAs(BigDecimal.class);
        default:
          throw SqlUtil.newContextException(
              node.getParserPosition(),
              RESOURCE.internal(
                  String.format(
                      "Parameter '%s' of procedure '%s' has type %s, which is not yet supported "
                          + "for CALL arguments.",
                      field.getName(), procName, typeName)));
      }
    } catch (ArithmeticException e) {
      throw typeMismatchException(procName, field, literal);
    }
  }

  private BigDecimal exactNumeric(String procName, Schema.Field field, SqlLiteral literal) {
    checkLiteralType(
        procName,
        field,
        literal,
        literal instanceof SqlNumericLiteral && ((SqlNumericLiteral) literal).isExact());
    return literal.getValueAs(BigDecimal.class);
  }

  private void checkLiteralType(
      String procName, Schema.Field field, SqlLiteral literal, boolean typeMatches) {
    if (!typeMatches) {
      throw typeMismatchException(procName, field, literal);
    }
  }

  private RuntimeException typeMismatchException(
      String procName, Schema.Field field, SqlLiteral literal) {
    return SqlUtil.newContextException(
        literal.getParserPosition(),
        RESOURCE.internal(
            String.format(
                "Argument '%s' of procedure '%s' expects type %s, but got: %s",
                field.getName(), procName, field.getType().getTypeName(), literal)));
  }
}
