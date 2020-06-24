package org.apache.beam.sdk.extensions.sql.impl.rel;

import org.apache.beam.sdk.extensions.sql.impl.SqlConversionException;
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
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelFieldCollation;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Match;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.util.*;

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

        return new matchTransform(partitionKeys, orderKeys);
    }

    private static class matchTransform extends PTransform<PCollectionList<Row>, PCollection<Row>> {

        private final List<RexNode> parKeys;
        private final RelCollation orderKeys;

        public matchTransform(List<RexNode> parKeys, RelCollation orderKeys) {
            this.parKeys = parKeys;
            this.orderKeys = orderKeys;
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
            for (RexNode i : parKeys) {
                RexVariable varNode = (RexVariable) i;
                int index = Integer.parseInt(varNode.getName().substring(1)); // get rid of `$`
                schemaBuilder.addField(collectionSchema.getField(index));
            }
            Schema mySchema = schemaBuilder.build();

            // partition according to the partition keys
            PCollection<KV<Row, Row>> keyedUpstream = upstream
                .apply(ParDo.of(new MapKeys(mySchema)));

            // sort within each partition
            PCollection<KV<Row, ArrayList<Row>>> orderedUpstream = keyedUpstream
                .apply(Combine.<Row, Row, ArrayList<Row>>perKey(new SortParts(mySchema, orderKeys)));

            return null;
        }

        private static class SortParts extends Combine.CombineFn<Row, ArrayList<Row>, ArrayList<Row>> {

            private final Schema mySchema;
            private final List<RelFieldCollation> orderKeys;

            public SortParts(Schema mySchema, RelCollation orderKeys) {
                this.mySchema = mySchema;
                List<RelFieldCollation> revOrderKeys = orderKeys.getFieldCollations();
                Collections.reverse(revOrderKeys);
                this.orderKeys = revOrderKeys;
            }

            @Override
            public ArrayList<Row> createAccumulator() {
                return new ArrayList<Row>();
            }

            @Override
            public ArrayList<Row> addInput(ArrayList<Row> Accum, Row inRow) {
                Accum.add(inRow);
                return Accum;
            }

            @Override
            public ArrayList<Row> mergeAccumulators(Iterable<ArrayList<Row>> Accums) {
                ArrayList<Row> aggAccum = new ArrayList<Row>();
                for (ArrayList<Row> i : Accums) {
                    aggAccum.addAll(i);
                }
                return aggAccum;
            }

            @Override
            public ArrayList<Row> extractOutput(ArrayList<Row> rawRows) {
                for (RelFieldCollation i : orderKeys) {
                    int fIndex = i.getFieldIndex();
                    RelFieldCollation.Direction dir = i.getDirection();
                    if (dir == RelFieldCollation.Direction.ASCENDING) {
                        Collections.sort(rawRows, new sortComparator(fIndex, true));
                    }
                }
                return rawRows;
            }

            private class sortComparator implements Comparator<Row> {

                private final int fIndex;
                private final int inv;

                public sortComparator(int fIndex, boolean inverse) {
                    this.fIndex = fIndex;
                    this.inv = inverse ? -1 : 1;
                }

                @Override
                public int compare(Row o1, Row o2) {
                    Schema.Field fd = mySchema.getField(fIndex);
                    Schema.FieldType dtype = fd.getType();
                    switch (dtype.getTypeName()) {
                        case BYTE:
                            return o1.getByte(fIndex).compareTo(o2.getByte(fIndex)) * inv;
                        case INT16:
                            return o1.getInt16(fIndex).compareTo(o2.getInt16(fIndex)) * inv;
                        case INT32:
                            return o1.getInt32(fIndex).compareTo(o2.getInt32(fIndex)) * inv;
                        case INT64:
                            return o1.getInt64(fIndex).compareTo(o2.getInt64(fIndex)) * inv;
                        case DECIMAL:
                            return o1.getDecimal(fIndex).compareTo(o2.getDecimal(fIndex)) * inv;
                        case FLOAT:
                            return o1.getFloat(fIndex).compareTo(o2.getFloat(fIndex)) * inv;
                        case DOUBLE:
                            return o1.getDouble(fIndex).compareTo(o2.getDouble(fIndex)) * inv;
                        case STRING:
                            return o1.getString(fIndex).compareTo(o2.getString(fIndex)) * inv;
                        case DATETIME:
                            return o1.getDateTime(fIndex).compareTo(o2.getDateTime(fIndex)) * inv;
                        case BOOLEAN:
                            return o1.getBoolean(fIndex).compareTo(o2.getBoolean(fIndex)) * inv;
                        default:
                            throw new SqlConversionException("Order not supported for specified column");
                    }
                }

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
            for(Schema.Field i : mySchema.getFields()) {
                String fieldName = i.getName();
                newRowBuilder.addValue(eleRow.getValue(fieldName));
            }
            KV kvPair = KV.of(newRowBuilder.build(), eleRow);
            out.output(kvPair);
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
