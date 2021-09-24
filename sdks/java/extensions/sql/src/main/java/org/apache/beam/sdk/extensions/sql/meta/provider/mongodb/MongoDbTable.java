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
package org.apache.beam.sdk.extensions.sql.meta.provider.mongodb;

import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind.AND;
import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind.COMPARISON;
import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind.OR;

import com.mongodb.client.model.Filters;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.DefaultTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.ProjectSupport;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InvalidTableException;
import org.apache.beam.sdk.io.mongodb.FindQuery;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.FieldTypeDescriptors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.utils.SelectHelpers;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Experimental
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class MongoDbTable extends SchemaBaseBeamTable implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(MongoDbTable.class);
  // Should match: mongodb://username:password@localhost:27017/database/collection
  @VisibleForTesting
  final Pattern locationPattern =
      Pattern.compile(
          "(?<credsHostPort>mongodb://(?<usernamePassword>.*(?<password>:.*)?@)?.+:\\d+)/(?<database>.+)/(?<collection>.+)");

  @VisibleForTesting final String dbCollection;
  @VisibleForTesting final String dbName;
  @VisibleForTesting final String dbUri;

  MongoDbTable(Table table) {
    super(table.getSchema());

    String location = table.getLocation();
    Matcher matcher = locationPattern.matcher(location);

    if (!matcher.matches()) {
      throw new InvalidTableException(
          "MongoDb location must be in the following format:"
              + " 'mongodb://(username:password@)?localhost:27017/database/collection'"
              + " but was: "
              + location);
    }
    this.dbUri = matcher.group("credsHostPort"); // "mongodb://localhost:27017"
    this.dbName = matcher.group("database");
    this.dbCollection = matcher.group("collection");
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    // Read MongoDb Documents
    PCollection<Document> readDocuments =
        MongoDbIO.read()
            .withUri(dbUri)
            .withDatabase(dbName)
            .withCollection(dbCollection)
            .expand(begin);

    return readDocuments.apply(DocumentToRow.withSchema(getSchema()));
  }

  @Override
  public PCollection<Row> buildIOReader(
      PBegin begin, BeamSqlTableFilter filters, List<String> fieldNames) {
    MongoDbIO.Read readInstance =
        MongoDbIO.read().withUri(dbUri).withDatabase(dbName).withCollection(dbCollection);

    final FieldAccessDescriptor resolved =
        FieldAccessDescriptor.withFieldNames(fieldNames).resolve(getSchema());
    final Schema newSchema = SelectHelpers.getOutputSchema(getSchema(), resolved);

    FindQuery findQuery = FindQuery.create();

    if (!(filters instanceof DefaultTableFilter)) {
      MongoDbFilter mongoFilter = (MongoDbFilter) filters;
      if (!mongoFilter.getSupported().isEmpty()) {
        Bson filter = constructPredicate(mongoFilter.getSupported());
        LOG.info("Pushing down the following filter: " + filter.toString());
        findQuery = findQuery.withFilters(filter);
      }
    }

    if (!fieldNames.isEmpty()) {
      findQuery = findQuery.withProjection(fieldNames);
    }

    readInstance = readInstance.withQueryFn(findQuery);

    return readInstance.expand(begin).apply(DocumentToRow.withSchema(newSchema));
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    return input
        .apply(new RowToDocument())
        .apply(MongoDbIO.write().withUri(dbUri).withDatabase(dbName).withCollection(dbCollection));
  }

  @Override
  public ProjectSupport supportsProjects() {
    return ProjectSupport.WITH_FIELD_REORDERING;
  }

  @Override
  public BeamSqlTableFilter constructFilter(List<RexNode> filter) {
    return MongoDbFilter.create(filter);
  }

  /**
   * Given a predicate in a conjunctive normal form (CNF), construct a {@code Bson} filter for
   * MongoDB find query.
   *
   * @param supported A list of {@code RexNode} in CNF.
   * @return {@code Bson} filter.
   */
  private Bson constructPredicate(List<RexNode> supported) {
    assert !supported.isEmpty();
    List<Bson> cnf =
        supported.stream().map(this::translateRexNodeToBson).collect(Collectors.toList());
    if (cnf.size() == 1) {
      return cnf.get(0);
    }
    return Filters.and(cnf);
  }

  /**
   * Recursively translates a single RexNode to MongoDB Bson filter. Supports simple comparison
   * operations, negation, and nested conjunction/disjunction. Boolean fields are translated as an
   * `$eq` operation with a boolean `true`.
   *
   * @param node {@code RexNode} to translate.
   * @return {@code Bson} filter.
   */
  private Bson translateRexNodeToBson(RexNode node) {
    final IntFunction<String> fieldIdToName = i -> getSchema().getField(i).getName();
    // Supported operations are described in MongoDbFilter#isSupported
    if (node instanceof RexCall) {
      RexCall compositeNode = (RexCall) node;
      List<RexLiteral> literals = new ArrayList<>();
      List<RexInputRef> inputRefs = new ArrayList<>();

      for (RexNode operand : compositeNode.getOperands()) {
        if (operand instanceof RexLiteral) {
          literals.add((RexLiteral) operand);
        } else if (operand instanceof RexInputRef) {
          inputRefs.add((RexInputRef) operand);
        }
      }

      // Operation is a comparison, since one of the operands in a field reference.
      if (inputRefs.size() == 1) {
        RexInputRef inputRef = inputRefs.get(0);
        String inputFieldName = fieldIdToName.apply(inputRef.getIndex());
        if (literals.size() > 0) {
          // Convert literal value to the same Java type as the field we are comparing to.
          Object literal = convertToExpectedType(inputRef, literals.get(0));

          switch (node.getKind()) {
            case IN:
              return Filters.in(inputFieldName, convertToExpectedType(inputRef, literals));
            case EQUALS:
              return Filters.eq(inputFieldName, literal);
            case NOT_EQUALS:
              return Filters.not(Filters.eq(inputFieldName, literal));
            case LESS_THAN:
              return Filters.lt(inputFieldName, literal);
            case GREATER_THAN:
              return Filters.gt(inputFieldName, literal);
            case GREATER_THAN_OR_EQUAL:
              return Filters.gte(inputFieldName, literal);
            case LESS_THAN_OR_EQUAL:
              return Filters.lte(inputFieldName, literal);
            default:
              // Encountered an unexpected node kind, RuntimeException below.
              break;
          }
        } else if (node.getKind().equals(SqlKind.NOT)) {
          // Ex: `where not boolean_field`
          return Filters.not(translateRexNodeToBson(inputRef));
        } else {
          throw new RuntimeException(
              "Cannot create a filter for an unsupported node: " + node.toString());
        }
      } else { // Operation is a conjunction/disjunction.
        switch (node.getKind()) {
          case AND:
            // Recursively construct filter for each operand of conjunction.
            return Filters.and(
                compositeNode.getOperands().stream()
                    .map(this::translateRexNodeToBson)
                    .collect(Collectors.toList()));
          case OR:
            // Recursively construct filter for each operand of disjunction.
            return Filters.or(
                compositeNode.getOperands().stream()
                    .map(this::translateRexNodeToBson)
                    .collect(Collectors.toList()));
          default:
            // Encountered an unexpected node kind, RuntimeException below.
            break;
        }
      }
      throw new RuntimeException(
          "Encountered an unexpected node kind: " + node.getKind().toString());
    } else if (node instanceof RexInputRef
        && node.getType().getSqlTypeName().equals(SqlTypeName.BOOLEAN)) {
      // Boolean field, must be true. Ex: `select * from table where bool_field`
      return Filters.eq(fieldIdToName.apply(((RexInputRef) node).getIndex()), true);
    }

    throw new RuntimeException(
        "Was expecting a RexCall or a boolean RexInputRef, but received: "
            + node.getClass().getSimpleName());
  }

  private Object convertToExpectedType(RexInputRef inputRef, RexLiteral literal) {
    FieldType beamFieldType = getSchema().getField(inputRef.getIndex()).getType();

    return literal.getValueAs(
        FieldTypeDescriptors.javaTypeForFieldType(beamFieldType).getRawType());
  }

  private Object convertToExpectedType(RexInputRef inputRef, List<RexLiteral> literals) {
    return literals.stream()
        .map(l -> convertToExpectedType(inputRef, l))
        .collect(Collectors.toList());
  }

  @Override
  public IsBounded isBounded() {
    return IsBounded.BOUNDED;
  }

  @Override
  public BeamTableStatistics getTableStatistics(PipelineOptions options) {
    long count =
        MongoDbIO.read()
            .withUri(dbUri)
            .withDatabase(dbName)
            .withCollection(dbCollection)
            .getDocumentCount();

    if (count < 0) {
      return BeamTableStatistics.BOUNDED_UNKNOWN;
    }

    return BeamTableStatistics.createBoundedTableStatistics((double) count);
  }

  public static class DocumentToRow extends PTransform<PCollection<Document>, PCollection<Row>> {
    private final Schema schema;

    private DocumentToRow(Schema schema) {
      this.schema = schema;
    }

    public static DocumentToRow withSchema(Schema schema) {
      return new DocumentToRow(schema);
    }

    @Override
    public PCollection<Row> expand(PCollection<Document> input) {
      // TODO(BEAM-8498): figure out a way convert Document directly to Row.
      return input
          .apply("Convert Document to JSON", ParDo.of(new DocumentToJsonStringConverter()))
          .apply("Transform JSON to Row", JsonToRow.withSchema(schema))
          .setRowSchema(schema);
    }

    // TODO: add support for complex fields (May require modifying how Calcite parses nested
    // fields).
    @VisibleForTesting
    static class DocumentToJsonStringConverter extends DoFn<Document, String> {
      @DoFn.ProcessElement
      public void processElement(ProcessContext context) {
        context.output(
            context
                .element()
                .toJson(JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build()));
      }
    }
  }

  public static class RowToDocument extends PTransform<PCollection<Row>, PCollection<Document>> {

    private RowToDocument() {}

    public static RowToDocument convert() {
      return new RowToDocument();
    }

    @Override
    public PCollection<Document> expand(PCollection<Row> input) {
      return input
          // TODO(BEAM-8498): figure out a way convert Row directly to Document.
          .apply("Transform Rows to JSON", ToJson.of())
          .apply("Produce documents from JSON", MapElements.via(new ObjectToDocumentFn()));
    }

    @VisibleForTesting
    static class ObjectToDocumentFn extends SimpleFunction<String, Document> {
      @Override
      public Document apply(String input) {
        return Document.parse(input);
      }
    }
  }

  static class MongoDbFilter implements BeamSqlTableFilter {
    private List<RexNode> supported;
    private List<RexNode> unsupported;

    public MongoDbFilter(List<RexNode> supported, List<RexNode> unsupported) {
      this.supported = supported;
      this.unsupported = unsupported;
    }

    @Override
    public List<RexNode> getNotSupported() {
      return unsupported;
    }

    @Override
    public int numSupported() {
      return BeamSqlTableFilter.expressionsInFilter(supported);
    }

    public List<RexNode> getSupported() {
      return supported;
    }

    @Override
    public String toString() {
      String supStr =
          "supported{"
              + supported.stream().map(RexNode::toString).collect(Collectors.joining())
              + "}";
      String unsupStr =
          "unsupported{"
              + unsupported.stream().map(RexNode::toString).collect(Collectors.joining())
              + "}";

      return "[" + supStr + ", " + unsupStr + "]";
    }

    public static MongoDbFilter create(List<RexNode> predicateCNF) {
      ImmutableList.Builder<RexNode> supported = ImmutableList.builder();
      ImmutableList.Builder<RexNode> unsupported = ImmutableList.builder();

      for (RexNode node : predicateCNF) {
        if (!node.getType().getSqlTypeName().equals(SqlTypeName.BOOLEAN)) {
          throw new RuntimeException(
              "Predicate node '"
                  + node.getClass().getSimpleName()
                  + "' should be a boolean expression, but was: "
                  + node.getType().getSqlTypeName());
        }

        if (isSupported(node)) {
          supported.add(node);
        } else {
          unsupported.add(node);
        }
      }

      return new MongoDbFilter(supported.build(), unsupported.build());
    }

    /**
     * Check whether a {@code RexNode} is supported. To keep things simple:<br>
     * 1. Support comparison operations in predicate, which compare a single field to literal
     * values. 2. Support nested Conjunction (AND), Disjunction (OR) as long as child operations are
     * supported.<br>
     * 3. Support boolean fields.
     *
     * @param node A node to check for predicate push-down support.
     * @return A boolean whether an expression is supported.
     */
    private static boolean isSupported(RexNode node) {
      if (node instanceof RexCall) {
        RexCall compositeNode = (RexCall) node;

        if (node.getKind().belongsTo(COMPARISON) || node.getKind().equals(SqlKind.NOT)) {
          int fields = 0;
          for (RexNode operand : compositeNode.getOperands()) {
            if (operand instanceof RexInputRef) {
              fields++;
            } else if (operand instanceof RexLiteral) {
              // RexLiterals are expected, but no action is needed.
            } else {
              // Complex predicates are not supported. Ex: `field1+5 == 10`.
              return false;
            }
          }
          // All comparison operations should have exactly one field reference.
          // Ex: `field1 == field2` is not supported.
          // TODO: Can be supported via Filters#where.
          if (fields == 1) {
            return true;
          }
        } else if (node.getKind().equals(AND) || node.getKind().equals(OR)) {
          // Nested ANDs and ORs are supported as long as all operands are supported.
          for (RexNode operand : compositeNode.getOperands()) {
            if (!isSupported(operand)) {
              return false;
            }
          }
          return true;
        }
      } else if (node instanceof RexInputRef) {
        // When field is a boolean.
        return true;
      } else {
        throw new RuntimeException(
            "Encountered an unexpected node type: " + node.getClass().getSimpleName());
      }

      return false;
    }
  }
}
