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
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexVariable;
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

    return new MatchTransform(partitionKeys, orderKeys, pattern, patternDefinitions);
  }

  private static class MatchTransform extends PTransform<PCollectionList<Row>, PCollection<Row>> {

    private final List<RexNode> parKeys;
    private final RelCollation orderKeys;
    private final RexNode pattern;
    private final Map<String, RexNode> patternDefs;

    public MatchTransform(
        List<RexNode> parKeys,
        RelCollation orderKeys,
        RexNode pattern,
        Map<String, RexNode> patternDefs) {
      this.parKeys = parKeys;
      this.orderKeys = orderKeys;
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
        RexVariable varNode = (RexVariable) i;
        int index = Integer.parseInt(varNode.getName().substring(1)); // get rid of `$`
        schemaBuilder.addField(upstreamSchema.getField(index));
      }
      Schema mySchema = schemaBuilder.build();

      // partition according to the partition keys
      PCollection<KV<Row, Row>> keyedUpstream = upstream.apply(ParDo.of(new MapKeys(mySchema)));

      // group by keys
      PCollection<KV<Row, Iterable<Row>>> groupedUpstream =
          keyedUpstream
              .setCoder(KvCoder.of(RowCoder.of(mySchema), RowCoder.of(upstreamSchema)))
              .apply(GroupByKey.create());

      // sort within each keyed partition
      ArrayList<OrderKey> myOrderKeys = makeOrderKeysFromCollation(orderKeys);
      PCollection<KV<Row, Iterable<Row>>> orderedUpstream =
          groupedUpstream.apply(ParDo.of(new SortPerKey(upstreamSchema, myOrderKeys)));

      // apply the pattern match in each partition
      ArrayList<CEPPattern> cepPattern =
          CEPUtil.getCEPPatternFromPattern(upstreamSchema, pattern, patternDefs);
      String regexPattern = CEPUtil.getRegexFromPattern(pattern);
      PCollection<KV<Row, Iterable<Row>>> matchedUpstream =
          orderedUpstream.apply(ParDo.of(new MatchPattern(cepPattern, regexPattern)));

      // apply the ParDo for the measures clause
      // for now, output all rows of each pattern matched (for testing purpose)
      // TODO: add ONE ROW PER MATCH and MEASURES implementation.
      PCollection<Row> outStream =
          matchedUpstream.apply(ParDo.of(new Measure())).setRowSchema(upstreamSchema);

      return outStream;
    }

    private static class Measure extends DoFn<KV<Row, Iterable<Row>>, Row> {

      @ProcessElement
      public void processElement(@Element KV<Row, Iterable<Row>> keyRows, OutputReceiver<Row> out) {
        for (Row i : keyRows.getValue()) {
          out.output(i);
        }
      }
    }

    // TODO: support both ALL ROWS PER MATCH and ONE ROW PER MATCH.
    // support only one row per match for now.
    private static class MatchPattern extends DoFn<KV<Row, Iterable<Row>>, KV<Row, Iterable<Row>>> {

      private final ArrayList<CEPPattern> pattern;
      private final String regexPattern;

      MatchPattern(ArrayList<CEPPattern> pattern, String regexPattern) {
        this.pattern = pattern;
        this.regexPattern = regexPattern;
      }

      @ProcessElement
      public void processElement(
          @Element KV<Row, Iterable<Row>> keyRows, OutputReceiver<KV<Row, Iterable<Row>>> out) {
        ArrayList<Row> rows = new ArrayList<>();
        StringBuilder patternString = new StringBuilder();
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
          patternString.append(patternOfRow);
        }

        Pattern p = Pattern.compile(regexPattern);
        Matcher m = p.matcher(patternString.toString());
        // if the pattern is (A B+ C),
        // it should return a List three rows matching A B C respectively
        if (m.matches()) {
          out.output(KV.of(keyRows.getKey(), rows.subList(m.start(), m.end())));
        }
      }
    }

    private static class SortPerKey extends DoFn<KV<Row, Iterable<Row>>, KV<Row, Iterable<Row>>> {

      private final Schema cSchema;
      private final ArrayList<OrderKey> orderKeys;

      public SortPerKey(Schema cSchema, ArrayList<OrderKey> orderKeys) {
        this.cSchema = cSchema;
        this.orderKeys = orderKeys;
      }

      @ProcessElement
      public void processElement(
          @Element KV<Row, Iterable<Row>> keyRows, OutputReceiver<KV<Row, Iterable<Row>>> out) {
        ArrayList<Row> rows = new ArrayList<Row>();
        for (Row i : keyRows.getValue()) {
          rows.add(i);
        }

        ArrayList<Integer> fIndexList = new ArrayList<>();
        ArrayList<Boolean> dirList = new ArrayList<>();
        ArrayList<Boolean> nullDirList = new ArrayList<>();
        for (OrderKey i : orderKeys) {
          fIndexList.add(i.getIndex());
          dirList.add(i.getDir());
          nullDirList.add(i.getNullFirst());
        }

        rows.sort(new BeamSortRel.BeamSqlRowComparator(fIndexList, dirList, nullDirList));

        out.output(KV.of(keyRows.getKey(), rows));
      }
    }
  }

  private static class MapKeys extends DoFn<Row, KV<Row, Row>> {

    private final Schema mySchema;

    public MapKeys(Schema mySchema) {
      this.mySchema = mySchema;
    }

    @ProcessElement
    public void processElement(@Element Row eleRow, OutputReceiver<KV<Row, Row>> out) {
      Row.Builder newRowBuilder = Row.withSchema(mySchema);

      // no partition specified would result in empty row as keys for rows
      for (Schema.Field i : mySchema.getFields()) {
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
