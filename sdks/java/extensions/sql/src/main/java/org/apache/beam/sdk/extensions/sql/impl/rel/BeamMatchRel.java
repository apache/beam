package org.apache.beam.sdk.extensions.sql.impl.rel;

import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import static org.apache.beam.vendor.calcite.v1_20_0.com.google.common.base.Preconditions.checkArgument;

/** {@link BeamRelNode} to replace a {@link Match} node. */
public class BeamMatchRel extends Match implements BeamRelNode {

    private static final Logger LOG = LoggerFactory.getLogger(BeamMatchRel.class);

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

        super(cluster,
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

        return new matchTransform(this.partitionKeys);
    }

    private static class matchTransform extends PTransform<PCollectionList<Row>, PCollection<Row>> {

        private final List<RexNode> parKeys;
        public matchTransform(List<RexNode> parKeys) {
            this.parKeys = parKeys;
        }

        @Override
        public PCollection<Row> expand(PCollectionList<Row> pinput) {
            checkArgument(
                pinput.size() == 1,
                "Wrong number of inputs for %s: %s",
                BeamMatchRel.class.getSimpleName(),
                pinput);
            PCollection<Row> upstream = pinput.get(0);

            Schema collectionSchema = upstream.getSchema();

            Schema.Builder schemaBuilder = new Schema.Builder();
            for(RexNode i : parKeys) {
                RexVariable varNode = (RexVariable) i;
                int index = Integer.parseInt(varNode.getName().substring(1)); // get rid of `$`
                schemaBuilder.addField(collectionSchema.getField(index));
            }
            Schema mySchema = schemaBuilder.build();

            // partition according to the partition keys
            PCollection<KV<Row, Row>> keyedUpstream = upstream
                .apply(ParDo.of(new MapKeys(mySchema)));

            // sort within each partition
            PCollection<KV<Row, Iterable<Row>>> orderedUpstream = keyedUpstream
                .apply(Combine.<Row, Row, ArrayList<Row>>perKey());

            return null;
        }

        private static class SortParts extends Combine.CombineFn<Row, ArrayList<Row>, ArrayList<Row>> {

            @Override
            public ArrayList<Row> createAccumulator() {return new <Row>ArrayList();}

            @Override
            public ArrayList<Row> addInput(ArrayList<Row> Accum, Row inRow) {
                Accum.add(inRow);
                return Accum;
            }

            @Override
            public ArrayList<Row> mergeAccumulators(Iterable<ArrayList<Row>> Accums) {
                ArrayList<Row> aggAccum = new <Row>ArrayList();
                for(ArrayList<Row> i : Accums) {
                    aggAccum.addAll(i);
                }
                return aggAccum;
            }

            @Override
            public ArrayList<Row> extractOutput(ArrayList<Row> rawRows) {
                //TODO sort the rows
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
                for(Schema.Field i : mySchema.getFields()) {
                    String fieldName = i.getName();
                    newRowBuilder.addValue(eleRow.getValue(fieldName));
                }
                KV kvPair = KV.of(newRowBuilder.build(), eleRow);
                out.output(kvPair);
            }
        }
    }

    @Override
    public Match copy(RelNode input,
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

        return new BeamMatchRel(getCluster(),
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
