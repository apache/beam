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

import static org.apache.beam.sdk.extensions.sql.impl.cep.CEPUtil.makeOrderKeysFromCollation;
import static org.apache.beam.vendor.calcite.v1_20_0.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPCall;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPFieldRef;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPKind;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPLiteral;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPMeasure;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPOperation;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPPattern;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPUtil;
import org.apache.beam.sdk.extensions.sql.impl.cep.OrderKey;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelCollation;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Match;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code BeamRelNode} to replace a {@code Match} node.
 *
 * <p>The {@code BeamMatchRel} is the Beam implementation of {@code MATCH_RECOGNIZE} in SQL.
 *
 * <p>For now, the underline implementation is based on java.util.regex.
 */
public class BeamMatchRel extends Match implements BeamRelNode {

  public static final Logger LOG = LoggerFactory.getLogger(BeamMatchRel.class);

  public BeamMatchRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType rowType,
      RexNode pattern,
      boolean strictStart,
      boolean strictEnd,
      Map<String, RexNode> patternDefinitions,
      Map<String, RexNode> measures,
      RexNode after,
      Map<String, ? extends SortedSet<String>> subsets,
      boolean allRows,
      List<RexNode> partitionKeys,
      RelCollation orderKeys,
      RexNode interval) {

    super(
        cluster,
        traitSet,
        input,
        rowType,
        pattern,
        strictStart,
        strictEnd,
        patternDefinitions,
        measures,
        after,
        subsets,
        allRows,
        partitionKeys,
        orderKeys,
        interval);
  }

  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return BeamCostModel.FACTORY.makeTinyCost(); // return constant costModel for now
  }

  @Override
  public NodeStats estimateNodeStats(RelMetadataQuery mq) {
    // a simple way of getting some estimate data
    // to be examined further
    NodeStats inputEstimate = BeamSqlRelUtils.getNodeStats(input, mq);
    double numRows = inputEstimate.getRowCount();
    double winSize = inputEstimate.getWindow();
    double rate = inputEstimate.getRate();

    return NodeStats.create(numRows, rate, winSize).multiply(0.5);
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {

    return new MatchTransform(
        partitionKeys, orderKeys, measures, allRows, pattern, patternDefinitions);
  }

  private static class MatchTransform extends PTransform<PCollectionList<Row>, PCollection<Row>> {

    private final List<RexNode> parKeys;
    private final RelCollation orderKeys;
    private final Map<String, RexNode> measures;
    private final boolean allRows;
    private final RexNode pattern;
    private final Map<String, RexNode> patternDefs;

    public MatchTransform(
        List<RexNode> parKeys,
        RelCollation orderKeys,
        Map<String, RexNode> measures,
        boolean allRows,
        RexNode pattern,
        Map<String, RexNode> patternDefs) {
      this.parKeys = parKeys;
      this.orderKeys = orderKeys;
      this.measures = measures;
      this.allRows = allRows;
      this.pattern = pattern;
      this.patternDefs = patternDefs;
    }

    @Override
    public PCollection<Row> expand(PCollectionList<Row> pinput) {
      checkArgument(
          pinput.size() == 1,
          "Wrong number of inputs for %s: %s",
          BeamMatchRel.class.getSimpleName(),
          pinput);
      PCollection<Row> upstream = pinput.get(0);

      Schema upstreamSchema = upstream.getSchema();

      Schema.Builder schemaBuilder = new Schema.Builder();
      for (RexNode i : parKeys) {
        RexInputRef varNode = (RexInputRef) i;
        int index = varNode.getIndex();
        schemaBuilder.addField(upstreamSchema.getField(index));
      }
      Schema partitionKeySchema = schemaBuilder.build();

      // partition according to the partition keys
      PCollection<KV<Row, Row>> keyedUpstream =
          upstream.apply(ParDo.of(new MapKeys(partitionKeySchema)));

      // group by keys
      PCollection<KV<Row, Iterable<Row>>> groupedUpstream =
          keyedUpstream
              .setCoder(KvCoder.of(RowCoder.of(partitionKeySchema), RowCoder.of(upstreamSchema)))
              .apply(GroupByKey.create());

      // sort within each keyed partition
      ArrayList<OrderKey> orderKeyList = makeOrderKeysFromCollation(orderKeys);
      // This will rely on an assumption that Fusion will fuse
      // operators here so the sorted result will be preserved
      // for the next match transform.
      // In most of the runners (if not all) this should be true.
      PCollection<KV<Row, Iterable<Row>>> orderedUpstream =
          groupedUpstream.apply(ParDo.of(new SortPerKey(orderKeyList)));

      // apply the pattern match in each partition
      ArrayList<CEPPattern> cepPattern =
          CEPUtil.getCEPPatternFromPattern(upstreamSchema, pattern, patternDefs);
      String regexPattern = CEPUtil.getRegexFromPattern(pattern);
      List<CEPMeasure> cepMeasures = new ArrayList<>();
      for (Map.Entry<String, RexNode> i : measures.entrySet()) {
        String outTableName = i.getKey();
        CEPOperation measureOperation;
        // TODO: support FINAL clause, for now, get rid of the FINAL operation
        if (i.getValue().getClass() == RexCall.class) {
          RexCall rexCall = (RexCall) i.getValue();
          if (rexCall.getOperator().getKind() == SqlKind.FINAL) {
            measureOperation = CEPOperation.of(rexCall.getOperands().get(0));
            cepMeasures.add(new CEPMeasure(upstreamSchema, outTableName, measureOperation));
            continue;
          }
        }
        measureOperation = CEPOperation.of(i.getValue());
        cepMeasures.add(new CEPMeasure(upstreamSchema, outTableName, measureOperation));
      }

      List<CEPFieldRef> cepParKeys = CEPUtil.getCEPFieldRefFromParKeys(parKeys);
      Schema outSchema = CEPUtil.decideSchema(cepMeasures, allRows, cepParKeys, upstreamSchema);
      PCollection<Row> outStream =
          orderedUpstream
              .apply(
                  ParDo.of(
                      new MatchPattern(
                          upstreamSchema,
                          cepParKeys,
                          cepPattern,
                          regexPattern,
                          cepMeasures,
                          allRows,
                          outSchema)))
              .setCoder(RowCoder.of(outSchema));

      // apply the ParDo for the measures clause
      // for now, output all rows of each pattern matched (for testing purpose)
      // for now, support FINAL only
      // TODO: add ONE ROW PER MATCH and MEASURES implementation.
      // TODO: handle the no aggregate in pattern with potentially multiple matches
      // TODO: add support for FINAL/RUNNING

      return outStream;
    }

    // TODO: support both ALL ROWS PER MATCH and ONE ROW PER MATCH.
    // support only one row per match for now.
    private static class MatchPattern extends DoFn<KV<Row, Iterable<Row>>, Row> {

      public static final Logger LOG = LoggerFactory.getLogger(MatchTransform.class);

      private final Schema upstreamSchema;
      private final Schema outSchema;
      private final List<CEPFieldRef> parKeys;
      private final ArrayList<CEPPattern> pattern;
      private final String regexPattern;
      private final List<CEPMeasure> measures;
      private final boolean allRows;

      MatchPattern(
          Schema upstreamSchema,
          List<CEPFieldRef> parKeys,
          ArrayList<CEPPattern> pattern,
          String regexPattern,
          List<CEPMeasure> measures,
          boolean allRows,
          Schema outSchema) {
        this.upstreamSchema = upstreamSchema;
        this.parKeys = parKeys;
        this.pattern = pattern;
        this.regexPattern = regexPattern;
        this.measures = measures;
        this.allRows = allRows;
        this.outSchema = outSchema;
      }

      @ProcessElement
      public void processElement(@Element KV<Row, Iterable<Row>> keyRows, OutputReceiver<Row> out) {
        ArrayList<Row> rows = new ArrayList<>();
        StringBuilder patternStringBuilder = new StringBuilder();
        for (Row i : keyRows.getValue()) {
          rows.add(i);
          // check pattern of row i
          String patternOfRow = " "; // a row with no matched pattern is marked by a space
          for (int j = 0; j < pattern.size(); ++j) {
            CEPPattern tryPattern = pattern.get(j);
            if (tryPattern.evalRow(i)) {
              patternOfRow = tryPattern.getPatternVar();
            }
          }
          patternStringBuilder.append(patternOfRow);
        }

        String patternString = patternStringBuilder.toString();

        Pattern p = Pattern.compile(regexPattern);
        Matcher m = p.matcher(patternString);

        while (m.find()) {
          // out put each matched sequence as specified by the Measure clause
          // TODO: for now (regex implementation), assume deterministic pattern match
          // (i.e. each row match to exactly one pattern or none)

          if (allRows) {
            Iterable<Row> outRows = rows.subList(m.start(), m.end());
            for (Row i : outRows) {
              out.output(i);
            }
          } else { // one row per match
            List<Row> matchedRows = rows.subList(m.start(), m.end());

            // a mapping from a pattern variable to a list of rows that match it
            // this part should be replaced by an NFA
            ImmutableMap.Builder<String, List<Row>> patternMappedRowsBuilder =
                ImmutableMap.<String, List<Row>>builder();
            int patternIndex = 0;
            for (int i = 0; i < matchedRows.size(); ) {
              ArrayList<Row> rowsOfAPattern = new ArrayList<>();
              CEPPattern patternToTest;
              if (patternIndex < pattern.size()) {
                patternToTest = pattern.get(patternIndex);
              } else {
                break;
              }
              String patternStr = patternToTest.getPatternVar();
              Row rowToTest = matchedRows.get(i);
              while (patternToTest.evalRow(rowToTest) && i < matchedRows.size()) {
                rowsOfAPattern.add(rowToTest);
                ++i;
                if (i < matchedRows.size()) {
                  rowToTest = matchedRows.get(i);
                }
              }
              patternMappedRowsBuilder.put(patternStr, rowsOfAPattern);
              ++patternIndex;
            }
            Map<String, List<Row>> patternMappedRows = patternMappedRowsBuilder.build();

            // output corresponding columns according to the measures schema
            Row.Builder newRowBuilder = Row.withSchema(outSchema);
            Row.FieldValueBuilder newFieldBuilder = null;

            // add partition key columns
            for (CEPFieldRef i : parKeys) {
              int colIndex = i.getIndex();
              Schema.Field parSchema = upstreamSchema.getField(colIndex);
              if (!matchedRows.isEmpty()) {
                Row firstRow = matchedRows.get(0);
                if (newFieldBuilder == null) {
                  newFieldBuilder =
                      newRowBuilder.withFieldValue(
                          parSchema.getName(), firstRow.getValue(colIndex));
                } else {
                  newFieldBuilder =
                      newFieldBuilder.withFieldValue(
                          parSchema.getName(), firstRow.getValue(colIndex));
                }
              } else {
                break;
              }
            }

            // add measure columns
            for (CEPMeasure i : measures) {
              String outName = i.getName();
              CEPFieldRef patternRef = i.getField();
              String patternVar = patternRef.getAlpha();
              List<Row> patternRows = patternMappedRows.get(patternVar);

              // implement CEPOperation as functions
              CEPOperation opr = i.getOperation();
              if (opr.getClass() == CEPCall.class) {
                CEPCall call = (CEPCall) opr;
                CEPKind funcName = call.getOperator().getCepKind();
                switch (funcName) {
                  case FIRST:
                    CEPFieldRef colFirstField = (CEPFieldRef) call.getOperands().get(0);
                    CEPLiteral colFirstIndex = (CEPLiteral) call.getOperands().get(1);
                    Row rowFirstToProc = patternRows.get(colFirstIndex.getInt32());
                    if (newFieldBuilder == null) {
                      newFieldBuilder =
                          newRowBuilder.withFieldValue(
                              outName, rowFirstToProc.getValue(colFirstField.getIndex()));
                    } else {
                      newFieldBuilder =
                          newFieldBuilder.withFieldValue(
                              outName, rowFirstToProc.getValue(colFirstField.getIndex()));
                    }
                    break;
                  case LAST:
                    CEPFieldRef colLastField = (CEPFieldRef) call.getOperands().get(0);
                    CEPLiteral colLastIndex = (CEPLiteral) call.getOperands().get(1);
                    Row rowLastToProc =
                        patternRows.get(
                            patternRows.size() - 1 - colLastIndex.getDecimal().intValue());
                    if (newFieldBuilder == null) {
                      newFieldBuilder =
                          newRowBuilder.withFieldValue(
                              outName, rowLastToProc.getValue(colLastField.getIndex()));
                    } else {
                      newFieldBuilder =
                          newFieldBuilder.withFieldValue(
                              outName, rowLastToProc.getValue(colLastField.getIndex()));
                    }
                    break;
                  default:
                    throw new UnsupportedOperationException(
                        "The measure function is not recognized: " + funcName.name());
                }
              } else if (opr.getClass() == CEPFieldRef.class) {
                Row rowToProc = patternRows.get(0);
                CEPFieldRef fieldRef = (CEPFieldRef) opr;
                if (newFieldBuilder == null) {
                  newFieldBuilder =
                      newRowBuilder.withFieldValue(
                          outName, rowToProc.getValue(fieldRef.getIndex()));
                } else {
                  newFieldBuilder =
                      newFieldBuilder.withFieldValue(
                          outName, rowToProc.getValue(fieldRef.getIndex()));
                }
              } else {
                throw new UnsupportedOperationException(
                    "CEP operation is not recognized: " + opr.getClass().getName());
              }
            }
            Row newRow;
            if (newFieldBuilder == null) {
              newRow = newRowBuilder.build();
            } else {
              newRow = newFieldBuilder.build();
            }
            out.output(newRow);
          }
        }
      }
    }

    private static class SortPerKey extends DoFn<KV<Row, Iterable<Row>>, KV<Row, Iterable<Row>>> {

      private final ArrayList<OrderKey> orderKeys;

      public SortPerKey(ArrayList<OrderKey> orderKeys) {
        this.orderKeys = orderKeys;
      }

      @ProcessElement
      public void processElement(
          @Element KV<Row, Iterable<Row>> keyRows, OutputReceiver<KV<Row, Iterable<Row>>> out) {
        ArrayList<Row> rows = new ArrayList<>();
        for (Row i : keyRows.getValue()) {
          rows.add(i);
        }

        ArrayList<Integer> fIndexList = new ArrayList<>();
        ArrayList<Boolean> dirList = new ArrayList<>();
        ArrayList<Boolean> nullDirList = new ArrayList<>();

        // reversely traverse the order key list
        for (int i = (orderKeys.size() - 1); i >= 0; --i) {
          OrderKey thisKey = orderKeys.get(i);
          fIndexList.add(thisKey.getIndex());
          dirList.add(thisKey.getDir());
          nullDirList.add(thisKey.getNullFirst());
        }

        rows.sort(new BeamSortRel.BeamSqlRowComparator(fIndexList, dirList, nullDirList));

        out.output(KV.of(keyRows.getKey(), rows));
      }
    }
  }

  private static class MapKeys extends DoFn<Row, KV<Row, Row>> {

    private final Schema partitionKeySchema;

    public MapKeys(Schema partitionKeySchema) {
      this.partitionKeySchema = partitionKeySchema;
    }

    @ProcessElement
    public void processElement(@Element Row eleRow, OutputReceiver<KV<Row, Row>> out) {
      Row.Builder newRowBuilder = Row.withSchema(partitionKeySchema);

      // no partition specified would result in empty row as keys for rows
      for (Schema.Field i : partitionKeySchema.getFields()) {
        String fieldName = i.getName();
        newRowBuilder.addValue(eleRow.getValue(fieldName));
      }
      KV kvPair = KV.of(newRowBuilder.build(), eleRow);
      out.output(kvPair);
    }
  }

  @Override
  public Match copy(
      RelNode input,
      RelDataType rowType,
      RexNode pattern,
      boolean strictStart,
      boolean strictEnd,
      Map<String, RexNode> patternDefinitions,
      Map<String, RexNode> measures,
      RexNode after,
      Map<String, ? extends SortedSet<String>> subsets,
      boolean allRows,
      List<RexNode> partitionKeys,
      RelCollation orderKeys,
      RexNode interval) {

    return new BeamMatchRel(
        getCluster(),
        getTraitSet(),
        input,
        rowType,
        pattern,
        strictStart,
        strictEnd,
        patternDefinitions,
        measures,
        after,
        subsets,
        allRows,
        partitionKeys,
        orderKeys,
        interval);
  }
}
