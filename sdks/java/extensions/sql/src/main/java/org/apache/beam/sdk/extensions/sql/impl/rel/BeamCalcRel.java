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
package org.apache.beam.sdk.extensions.sql.impl.rel;

import static org.apache.beam.sdk.schemas.Schema.FieldType;
import static org.apache.beam.vendor.calcite.v1_20_0.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.extensions.sql.impl.JavaUdfLoader;
import org.apache.beam.sdk.extensions.sql.impl.ScalarFunctionImpl;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamJavaTypeFactory;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils.CharType;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils.TimeWithLocalTzType;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.DataContext;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.avatica.util.ByteString;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.linq4j.QueryProvider;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.linq4j.tree.Expression;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.linq4j.tree.Expressions;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.linq4j.tree.Types;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptPredicateList;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Calc;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexBuilder;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexProgram;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexSimplify;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexUtil;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.runtime.SqlFunctions;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.validate.SqlConformance;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ScriptEvaluator;
import org.joda.time.DateTime;
import org.joda.time.Instant;

/** BeamRelNode to replace {@code Project} and {@code Filter} node. */
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "keyfor",
  "nullness"
}) // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
public class BeamCalcRel extends AbstractBeamCalcRel {

  private static final long NANOS_PER_MILLISECOND = 1000000L;
  private static final long MILLIS_PER_DAY = 86400000L;

  private static final ParameterExpression rowParam = Expressions.parameter(Row.class, "row");

  public BeamCalcRel(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexProgram program) {
    super(cluster, traits, input, program);
  }

  @Override
  public Calc copy(RelTraitSet traitSet, RelNode input, RexProgram program) {
    return new BeamCalcRel(getCluster(), traitSet, input, program);
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    return new Transform();
  }

  private class Transform extends PTransform<PCollectionList<Row>, PCollection<Row>> {

    /**
     * expand is based on calcite's EnumerableCalc.implement(). This function generates java code
     * executed in the processElement in CalcFn using Calcite's linq4j library. It generates a block
     * of code using a BlockBuilder. The root of the block is an if statement with any conditions.
     * Inside that if statement, a new record is output with a row containing transformed fields.
     * The InputGetterImpl class generates code to read from the input record and convert to Calcite
     * types. Calcite then generates code for any function calls or other operations. Outputs are in
     * Calcite types, toBeamRow converts back to Beam types.
     */
    @Override
    public PCollection<Row> expand(PCollectionList<Row> pinput) {
      checkArgument(
          pinput.size() == 1,
          "Wrong number of inputs for %s: %s",
          BeamCalcRel.class.getSimpleName(),
          pinput);
      PCollection<Row> upstream = pinput.get(0);
      Schema outputSchema = CalciteUtils.toSchema(getRowType());

      final SqlConformance conformance = SqlConformanceEnum.MYSQL_5;
      final JavaTypeFactory typeFactory = BeamJavaTypeFactory.INSTANCE;
      final BlockBuilder builder = new BlockBuilder();

      final PhysType physType =
          PhysTypeImpl.of(typeFactory, getRowType(), JavaRowFormat.ARRAY, false);

      final RexBuilder rexBuilder = getCluster().getRexBuilder();
      final RelMetadataQuery mq = RelMetadataQuery.instance();
      final RelOptPredicateList predicates = mq.getPulledUpPredicates(getInput());
      final RexSimplify simplify = new RexSimplify(rexBuilder, predicates, RexUtil.EXECUTOR);
      final RexProgram program = getProgram().normalize(rexBuilder, simplify);

      Expression condition =
          RexToLixTranslator.translateCondition(
              program,
              typeFactory,
              builder,
              new InputGetterImpl(rowParam, upstream.getSchema()),
              null,
              conformance);

      List<Expression> expressions =
          RexToLixTranslator.translateProjects(
              program,
              typeFactory,
              conformance,
              builder,
              physType,
              DataContext.ROOT,
              new InputGetterImpl(rowParam, upstream.getSchema()),
              null);

      builder.add(
          Expressions.ifThenElse(
              condition,
              Expressions.return_(null, physType.record(expressions)),
              Expressions.return_(null, Expressions.constant(null))));

      BeamSqlPipelineOptions options =
          pinput.getPipeline().getOptions().as(BeamSqlPipelineOptions.class);

      CalcFn calcFn =
          new CalcFn(
              builder.toBlock().toString(),
              outputSchema,
              options.getVerifyRowValues(),
              getJarPaths(program));

      // validate generated code
      calcFn.compile();

      return upstream.apply(ParDo.of(calcFn)).setRowSchema(outputSchema);
    }
  }

  /** {@code CalcFn} is the executor for a {@link BeamCalcRel} step. */
  private static class CalcFn extends DoFn<Row, Row> {
    private final String processElementBlock;
    private final Schema outputSchema;
    private final boolean verifyRowValues;
    private final List<String> jarPaths;
    private transient @Nullable ScriptEvaluator se = null;

    public CalcFn(
        String processElementBlock,
        Schema outputSchema,
        boolean verifyRowValues,
        List<String> jarPaths) {
      this.processElementBlock = processElementBlock;
      this.outputSchema = outputSchema;
      this.verifyRowValues = verifyRowValues;
      this.jarPaths = jarPaths;
    }

    ScriptEvaluator compile() {
      ScriptEvaluator se = new ScriptEvaluator();
      if (!jarPaths.isEmpty()) {
        try {
          JavaUdfLoader udfLoader = new JavaUdfLoader();
          ClassLoader classLoader = udfLoader.createClassLoader(jarPaths);
          se.setParentClassLoader(classLoader);
        } catch (IOException e) {
          throw new RuntimeException("Failed to load user-provided jar(s).", e);
        }
      }
      se.setParameters(
          new String[] {rowParam.name, DataContext.ROOT.name},
          new Class[] {(Class) rowParam.getType(), (Class) DataContext.ROOT.getType()});
      se.setReturnType(Object[].class);
      try {
        se.cook(processElementBlock);
      } catch (CompileException e) {
        throw new UnsupportedOperationException(
            "Could not compile CalcFn: " + processElementBlock, e);
      }
      return se;
    }

    @Setup
    public void setup() {
      this.se = compile();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      assert se != null;
      final Object[] v;
      try {
        v = (Object[]) se.evaluate(new Object[] {c.element(), CONTEXT_INSTANCE});
      } catch (InvocationTargetException e) {
        throw new RuntimeException(
            "CalcFn failed to evaluate: " + processElementBlock, e.getCause());
      }
      if (v != null) {
        Row row = toBeamRow(Arrays.asList(v), outputSchema, verifyRowValues);
        c.output(row);
      }
    }
  }

  private static List<String> getJarPaths(RexProgram program) {
    ImmutableList.Builder<String> jarPaths = new ImmutableList.Builder<>();
    for (RexNode node : program.getExprList()) {
      if (node instanceof RexCall) {
        SqlOperator op = ((RexCall) node).op;
        if (op instanceof SqlUserDefinedFunction) {
          Function function = ((SqlUserDefinedFunction) op).function;
          if (function instanceof ScalarFunctionImpl) {
            String jarPath = ((ScalarFunctionImpl) function).getJarPath();
            if (!jarPath.isEmpty()) {
              jarPaths.add(jarPath);
            }
          }
        }
      }
    }
    return jarPaths.build();
  }

  static Object toBeamObject(Object value, FieldType fieldType, boolean verifyValues) {
    if (value == null) {
      return null;
    }
    switch (fieldType.getTypeName()) {
        // BEAM-12176: Numbers aren't always the type we expect.
      case BYTE:
        return ((Number) value).byteValue();
      case INT16:
        return ((Number) value).shortValue();
      case INT32:
        return ((Number) value).intValue();
      case INT64:
        return ((Number) value).longValue();
      case FLOAT:
        return ((Number) value).floatValue();
      case DOUBLE:
        return ((Number) value).doubleValue();
      case DECIMAL:
        if (value instanceof BigDecimal) {
          return (BigDecimal) value;
        } else if (value instanceof Long) {
          return BigDecimal.valueOf((Long) value);
        } else if (value instanceof Integer) {
          return BigDecimal.valueOf((Integer) value);
        }
        return new BigDecimal(((Number) value).toString());
      case STRING:
        return (String) value;
      case BOOLEAN:
        return (Boolean) value;
      case DATETIME:
        if (value instanceof Timestamp) {
          value = SqlFunctions.toLong((Timestamp) value);
        }
        return Instant.ofEpochMilli(((Number) value).longValue());
      case BYTES:
        if (value instanceof byte[]) {
          return value;
        }
        return ((ByteString) value).getBytes();
      case ARRAY:
        return toBeamList((List<Object>) value, fieldType.getCollectionElementType(), verifyValues);
      case MAP:
        return toBeamMap(
            (Map<Object, Object>) value,
            fieldType.getMapKeyType(),
            fieldType.getMapValueType(),
            verifyValues);
      case ROW:
        if (value instanceof Object[]) {
          value = Arrays.asList((Object[]) value);
        }
        return toBeamRow((List<Object>) value, fieldType.getRowSchema(), verifyValues);
      case LOGICAL_TYPE:
        String identifier = fieldType.getLogicalType().getIdentifier();
        if (CharType.IDENTIFIER.equals(identifier)) {
          return (String) value;
        } else if (TimeWithLocalTzType.IDENTIFIER.equals(identifier)) {
          return Instant.ofEpochMilli(((Number) value).longValue());
        } else if (SqlTypes.DATE.getIdentifier().equals(identifier)) {
          if (value instanceof Date) {
            value = SqlFunctions.toInt((Date) value);
          }
          // BEAM-12175: value should always be Integer here, but it isn't.
          return LocalDate.ofEpochDay(((Number) value).longValue());
        } else if (SqlTypes.TIME.getIdentifier().equals(identifier)) {
          if (value instanceof Time) {
            value = SqlFunctions.toInt((Time) value);
          }
          // BEAM-12175: value should always be Integer here, but it isn't.
          return LocalTime.ofNanoOfDay(((Number) value).longValue() * NANOS_PER_MILLISECOND);
        } else if (SqlTypes.DATETIME.getIdentifier().equals(identifier)) {
          if (value instanceof Timestamp) {
            value = SqlFunctions.toLong((Timestamp) value);
          }
          return LocalDateTime.of(
              LocalDate.ofEpochDay(((Number) value).longValue() / MILLIS_PER_DAY),
              LocalTime.ofNanoOfDay(
                  (((Number) value).longValue() % MILLIS_PER_DAY) * NANOS_PER_MILLISECOND));
        } else {
          throw new UnsupportedOperationException("Unable to convert logical type " + identifier);
        }
      default:
        throw new UnsupportedOperationException("Unable to convert " + fieldType.getTypeName());
    }
  }

  private static List<Object> toBeamList(
      List<Object> arrayValue, FieldType elementType, boolean verifyValues) {
    return arrayValue.stream()
        .map(e -> toBeamObject(e, elementType, verifyValues))
        .collect(Collectors.toList());
  }

  private static Map<Object, Object> toBeamMap(
      Map<Object, Object> mapValue,
      FieldType keyType,
      FieldType elementType,
      boolean verifyValues) {
    Map<Object, Object> output = new HashMap<>(mapValue.size());
    for (Map.Entry<Object, Object> entry : mapValue.entrySet()) {
      output.put(
          toBeamObject(entry.getKey(), keyType, verifyValues),
          toBeamObject(entry.getValue(), elementType, verifyValues));
    }
    return output;
  }

  private static Row toBeamRow(List<Object> structValue, Schema schema, boolean verifyValues) {
    List<Object> objects = new ArrayList<>(schema.getFieldCount());
    assert structValue.size() == schema.getFieldCount();
    for (int i = 0; i < structValue.size(); i++) {
      objects.add(toBeamObject(structValue.get(i), schema.getField(i).getType(), verifyValues));
    }
    Row row =
        verifyValues
            ? Row.withSchema(schema).addValues(objects).build()
            : Row.withSchema(schema).attachValues(objects);
    return row;
  }

  private static class InputGetterImpl implements RexToLixTranslator.InputGetter {

    private final Expression input;
    private final Schema inputSchema;

    private InputGetterImpl(Expression input, Schema inputSchema) {
      this.input = input;
      this.inputSchema = inputSchema;
    }

    @Override
    public Expression field(BlockBuilder list, int index, Type storageType) {
      return getBeamField(list, index, input, inputSchema);
    }

    // Read field from Beam Row
    private static Expression getBeamField(
        BlockBuilder list, int index, Expression input, Schema schema) {
      if (index >= schema.getFieldCount() || index < 0) {
        throw new IllegalArgumentException("Unable to find value #" + index);
      }

      final Expression expression = list.append(list.newName("current"), input);

      FieldType fieldType = schema.getField(index).getType();
      Expression value;
      switch (fieldType.getTypeName()) {
        case BYTE:
          value = Expressions.call(expression, "getByte", Expressions.constant(index));
          break;
        case INT16:
          value = Expressions.call(expression, "getInt16", Expressions.constant(index));
          break;
        case INT32:
          value = Expressions.call(expression, "getInt32", Expressions.constant(index));
          break;
        case INT64:
          value = Expressions.call(expression, "getInt64", Expressions.constant(index));
          break;
        case DECIMAL:
          value = Expressions.call(expression, "getDecimal", Expressions.constant(index));
          break;
        case FLOAT:
          value = Expressions.call(expression, "getFloat", Expressions.constant(index));
          break;
        case DOUBLE:
          value = Expressions.call(expression, "getDouble", Expressions.constant(index));
          break;
        case STRING:
          value = Expressions.call(expression, "getString", Expressions.constant(index));
          break;
        case DATETIME:
          value = Expressions.call(expression, "getDateTime", Expressions.constant(index));
          break;
        case BOOLEAN:
          value = Expressions.call(expression, "getBoolean", Expressions.constant(index));
          break;
        case BYTES:
          value = Expressions.call(expression, "getBytes", Expressions.constant(index));
          break;
        case ARRAY:
          value = Expressions.call(expression, "getArray", Expressions.constant(index));
          break;
        case MAP:
          value = Expressions.call(expression, "getMap", Expressions.constant(index));
          break;
        case ROW:
          value = Expressions.call(expression, "getRow", Expressions.constant(index));
          break;
        case LOGICAL_TYPE:
          String identifier = fieldType.getLogicalType().getIdentifier();
          if (CharType.IDENTIFIER.equals(identifier)) {
            value = Expressions.call(expression, "getString", Expressions.constant(index));
          } else if (TimeWithLocalTzType.IDENTIFIER.equals(identifier)) {
            value = Expressions.call(expression, "getDateTime", Expressions.constant(index));
          } else if (SqlTypes.DATE.getIdentifier().equals(identifier)) {
            value =
                Expressions.convert_(
                    Expressions.call(
                        expression,
                        "getLogicalTypeValue",
                        Expressions.constant(index),
                        Expressions.constant(LocalDate.class)),
                    LocalDate.class);
          } else if (SqlTypes.TIME.getIdentifier().equals(identifier)) {
            value =
                Expressions.convert_(
                    Expressions.call(
                        expression,
                        "getLogicalTypeValue",
                        Expressions.constant(index),
                        Expressions.constant(LocalTime.class)),
                    LocalTime.class);
          } else if (SqlTypes.DATETIME.getIdentifier().equals(identifier)) {
            value =
                Expressions.convert_(
                    Expressions.call(
                        expression,
                        "getLogicalTypeValue",
                        Expressions.constant(index),
                        Expressions.constant(LocalDateTime.class)),
                    LocalDateTime.class);
          } else {
            throw new UnsupportedOperationException("Unable to get logical type " + identifier);
          }
          break;
        default:
          throw new UnsupportedOperationException("Unable to get " + fieldType.getTypeName());
      }

      return toCalciteValue(value, fieldType);
    }

    // Value conversion: Beam => Calcite
    private static Expression toCalciteValue(Expression value, FieldType fieldType) {
      switch (fieldType.getTypeName()) {
        case BYTE:
          return Expressions.convert_(value, Byte.class);
        case INT16:
          return Expressions.convert_(value, Short.class);
        case INT32:
          return Expressions.convert_(value, Integer.class);
        case INT64:
          return Expressions.convert_(value, Long.class);
        case DECIMAL:
          return Expressions.convert_(value, BigDecimal.class);
        case FLOAT:
          return Expressions.convert_(value, Float.class);
        case DOUBLE:
          return Expressions.convert_(value, Double.class);
        case STRING:
          return Expressions.convert_(value, String.class);
        case BOOLEAN:
          return Expressions.convert_(value, Boolean.class);
        case DATETIME:
          return nullOr(
              value, Expressions.call(Expressions.convert_(value, DateTime.class), "getMillis"));
        case BYTES:
          return nullOr(
              value, Expressions.new_(ByteString.class, Expressions.convert_(value, byte[].class)));
        case ARRAY:
          return nullOr(value, toCalciteList(value, fieldType.getCollectionElementType()));
        case MAP:
          return nullOr(value, toCalciteMap(value, fieldType.getMapValueType()));
        case ROW:
          return nullOr(value, toCalciteRow(value, fieldType.getRowSchema()));
        case LOGICAL_TYPE:
          String identifier = fieldType.getLogicalType().getIdentifier();
          if (CharType.IDENTIFIER.equals(identifier)) {
            return Expressions.convert_(value, String.class);
          } else if (TimeWithLocalTzType.IDENTIFIER.equals(identifier)) {
            return nullOr(
                value, Expressions.call(Expressions.convert_(value, DateTime.class), "getMillis"));
          } else if (SqlTypes.DATE.getIdentifier().equals(identifier)) {
            return nullOr(
                value,
                Expressions.call(
                    Expressions.box(
                        Expressions.call(
                            Expressions.convert_(value, LocalDate.class), "toEpochDay")),
                    "intValue"));
          } else if (SqlTypes.TIME.getIdentifier().equals(identifier)) {
            return nullOr(
                value,
                Expressions.call(
                    Expressions.box(
                        Expressions.divide(
                            Expressions.call(
                                Expressions.convert_(value, LocalTime.class), "toNanoOfDay"),
                            Expressions.constant(NANOS_PER_MILLISECOND))),
                    "intValue"));
          } else if (SqlTypes.DATETIME.getIdentifier().equals(identifier)) {
            value = Expressions.convert_(value, LocalDateTime.class);
            Expression dateValue =
                Expressions.call(Expressions.call(value, "toLocalDate"), "toEpochDay");
            Expression timeValue =
                Expressions.call(Expressions.call(value, "toLocalTime"), "toNanoOfDay");
            Expression returnValue =
                Expressions.add(
                    Expressions.multiply(dateValue, Expressions.constant(MILLIS_PER_DAY)),
                    Expressions.divide(timeValue, Expressions.constant(NANOS_PER_MILLISECOND)));
            return nullOr(value, returnValue);
          } else {
            throw new UnsupportedOperationException("Unable to convert logical type " + identifier);
          }
        default:
          throw new UnsupportedOperationException("Unable to convert " + fieldType.getTypeName());
      }
    }

    private static Expression toCalciteList(Expression input, FieldType elementType) {
      ParameterExpression value = Expressions.parameter(Object.class);

      BlockBuilder block = new BlockBuilder();
      block.add(toCalciteValue(value, elementType));

      return Expressions.new_(
          WrappedList.class,
          ImmutableList.of(Types.castIfNecessary(List.class, input)),
          ImmutableList.<MemberDeclaration>of(
              Expressions.methodDecl(
                  Modifier.PUBLIC,
                  Object.class,
                  "value",
                  ImmutableList.of(value),
                  block.toBlock())));
    }

    private static Expression toCalciteMap(Expression input, FieldType mapValueType) {
      ParameterExpression value = Expressions.parameter(Object.class);

      BlockBuilder block = new BlockBuilder();
      block.add(toCalciteValue(value, mapValueType));

      return Expressions.new_(
          WrappedMap.class,
          ImmutableList.of(Types.castIfNecessary(Map.class, input)),
          ImmutableList.<MemberDeclaration>of(
              Expressions.methodDecl(
                  Modifier.PUBLIC,
                  Object.class,
                  "value",
                  ImmutableList.of(value),
                  block.toBlock())));
    }

    private static Expression toCalciteRow(Expression input, Schema schema) {
      ParameterExpression row = Expressions.parameter(Row.class);
      ParameterExpression index = Expressions.parameter(int.class);
      BlockBuilder body = new BlockBuilder(/* optimizing= */ false);

      for (int i = 0; i < schema.getFieldCount(); i++) {
        BlockBuilder list = new BlockBuilder(/* optimizing= */ false, body);
        Expression returnValue = getBeamField(list, i, row, schema);

        list.append(returnValue);

        body.append(
            "if i=" + i,
            Expressions.block(
                Expressions.ifThen(
                    Expressions.equal(index, Expressions.constant(i, int.class)), list.toBlock())));
      }

      body.add(Expressions.throw_(Expressions.new_(IndexOutOfBoundsException.class)));

      return Expressions.new_(
          WrappedRow.class,
          ImmutableList.of(Types.castIfNecessary(Row.class, input)),
          ImmutableList.<MemberDeclaration>of(
              Expressions.methodDecl(
                  Modifier.PUBLIC,
                  Object.class,
                  "field",
                  ImmutableList.of(row, index),
                  body.toBlock())));
    }
  }

  private static Expression nullOr(Expression field, Expression ifNotNull) {
    return Expressions.condition(
        Expressions.equal(field, Expressions.constant(null)),
        Expressions.constant(null),
        Expressions.box(ifNotNull));
  }

  private static final DataContext CONTEXT_INSTANCE = new SlimDataContext();

  private static class SlimDataContext implements DataContext {
    @Override
    public SchemaPlus getRootSchema() {
      return null;
    }

    @Override
    public JavaTypeFactory getTypeFactory() {
      return null;
    }

    @Override
    public QueryProvider getQueryProvider() {
      return null;
    }

    /* DataContext.get is used to fetch "global" state inside the generated code */
    @Override
    public Object get(String name) {
      if (name.equals(DataContext.Variable.UTC_TIMESTAMP.camelName)
          || name.equals(DataContext.Variable.CURRENT_TIMESTAMP.camelName)
          || name.equals(DataContext.Variable.LOCAL_TIMESTAMP.camelName)) {
        return System.currentTimeMillis();
      } else if (name.equals(Variable.TIME_ZONE.camelName)) {
        return TimeZone.getDefault();
      }
      return null;
    }
  }

  /** WrappedRow translates {@code Row} on access. */
  public abstract static class WrappedRow extends AbstractList<Object> {
    private final Row row;

    protected WrappedRow(Row row) {
      this.row = row;
    }

    @Override
    public Object get(int index) {
      return field(row, index);
    }

    // we could override get(int index) if we knew how to access `this.row` in linq4j
    // for now we keep it consistent with WrappedList
    protected abstract Object field(Row row, int index);

    @Override
    public int size() {
      return row.getFieldCount();
    }
  }

  /** WrappedMap translates {@code Map} on access. */
  public abstract static class WrappedMap<V> extends AbstractMap<Object, V> {
    private final Map<Object, Object> map;

    protected WrappedMap(Map<Object, Object> map) {
      this.map = map;
    }

    // TODO transform keys, in this case, we need to do lookup, so it should be both ways:
    //
    // public abstract Object fromKey(K key)
    // public abstract K toKey(Object key)

    @Override
    public Set<Entry<Object, V>> entrySet() {
      return Maps.transformValues(map, val -> (val == null) ? null : value(val)).entrySet();
    }

    @Override
    public V get(Object key) {
      return value(map.get(key));
    }

    protected abstract V value(Object value);
  }

  /** WrappedList translates {@code List} on access. */
  public abstract static class WrappedList<T> extends AbstractList<T> {
    private final List<Object> values;

    protected WrappedList(List<Object> values) {
      this.values = values;
    }

    @Override
    public T get(int index) {
      return value(values.get(index));
    }

    protected abstract T value(Object value);

    @Override
    public int size() {
      return values.size();
    }
  }
}
