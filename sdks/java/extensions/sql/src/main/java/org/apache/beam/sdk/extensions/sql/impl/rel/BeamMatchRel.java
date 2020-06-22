package org.apache.beam.sdk.extensions.sql.impl.rel;

import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
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

import java.util.List;
import java.util.Map;
import java.util.SortedSet;

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
        // get the partition columns
        for(RexNode i : this.partitionKeys) {
            LOG.info(((RexVariable) i).getName() + " " + i.getType());
        }

        return null;
    }

//    private static class matchTransform extends PTransform<PCollectionList<Row>, PCollection<Row>> {
//        public matchTransform()
//    }

//    private class mapKeys extends DoFn<Row, KV<Row, Row>> {
//        private final Schema keySchema;
//        public mapKeys(Schema keySchema) {
//            this.keySchema = keySchema;
//        }
//    }

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
