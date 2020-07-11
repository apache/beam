package org.apache.beam.sdk.extensions.sql.impl.rel;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.impl.SqlConversionException;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPPattern;
import org.apache.beam.sdk.extensions.sql.impl.cep.OrderKey;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPUtil;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
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
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.beam.vendor.calcite.v1_20_0.com.google.common.base.Preconditions.checkArgument;

/** {@link BeamRelNode} to replace a {@link Match} node. */
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

        return new matchTransform(partitionKeys, orderKeys, pattern, patternDefinitions);
    }

    private static class matchTransform extends PTransform<PCollectionList<Row>, PCollection<Row>> {

        private final List<RexNode> parKeys;
        private final RelCollation orderKeys;
        private final RexNode pattern;
        private final Map<String, RexNode> patternDefs;

        public matchTransform(
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

            // group by keys
            PCollection<KV<Row, Iterable<Row>>> groupedUpstream = keyedUpstream
                .setCoder(KvCoder.of(RowCoder.of(mySchema), RowCoder.of(collectionSchema)))
                .apply(GroupByKey.create());

            // sort within each keyed partition
            PCollection<KV<Row, Iterable<Row>>> orderedUpstream = groupedUpstream
                .apply(ParDo.of(new SortPerKey(collectionSchema, orderKeys)));

            // apply the pattern match in each partition
            ArrayList<CEPPattern> cepPattern = CEPUtil.getCEPPatternFromPattern(collectionSchema,
                    (RexCall) pattern,
                    patternDefs);
            String regexPattern = CEPUtil.getRegexFromPattern((RexCall) pattern);
            PCollection<KV<Row, Iterable<Row>>> matchedUpstream = orderedUpstream
                .apply(ParDo.of(new MatchPattern(cepPattern, regexPattern)));

            // apply the ParDo for the measures clause
            // for now, output the all rows of each pattern matched (for testing purpose)
            PCollection<Row> outStream = matchedUpstream
                    .apply(ParDo.of(new Measure()))
                    .setRowSchema(collectionSchema);

            return outStream;
        }

        private static class Measure extends DoFn<KV<Row, Iterable<Row>>, Row> {

            @ProcessElement
            public void processElement(@Element KV<Row, Iterable<Row>> keyRows,
                                       OutputReceiver<Row> out) {
                for(Row i : keyRows.getValue()) {
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
            public void processElement(@Element KV<Row, Iterable<Row>> keyRows,
                                       OutputReceiver<KV<Row, Iterable<Row>>> out) {
                ArrayList<Row> rows = new ArrayList<>();
                String patternString = "";
                for(Row i : keyRows.getValue()) {
                    rows.add(i);
                    // check pattern of row i
                    String patternOfRow = " "; // a row with no matched pattern is marked by a space
                    for(int j = 0; j < pattern.size(); ++j) {
                        CEPPattern tryPattern = pattern.get(j);
                        if(tryPattern.evalRow(i)) {
                            patternOfRow = tryPattern.toString();
                        }
                    }
                    patternString += patternOfRow;
                }

                Pattern p = Pattern.compile(regexPattern);
                Matcher m = p.matcher(patternString);
                // if the pattern is (A B+ C),
                // it should return a List three rows matching A B C respectively
                if(m.matches()) {
                    out.output(KV.of(keyRows.getKey(),
                            rows.subList(m.start(), m.end())));
                }
            }

        }

        private static class SortPerKey extends DoFn<KV<Row, Iterable<Row>>, KV<Row, Iterable<Row>>> {

            private final Schema cSchema;
            private final ArrayList<OrderKey> orderKeys;

            public SortPerKey(Schema cSchema, RelCollation orderKeys) {
                this.cSchema = cSchema;

                List<RelFieldCollation> revOrderKeys = orderKeys.getFieldCollations();
                Collections.reverse(revOrderKeys);
                ArrayList<OrderKey> revOrderKeysList = new ArrayList<>();
                for(RelFieldCollation i : revOrderKeys) {
                    int fIndex = i.getFieldIndex();
                    RelFieldCollation.Direction dir = i.getDirection();
                    if(dir == RelFieldCollation.Direction.ASCENDING) {
                        revOrderKeysList.add(new OrderKey(fIndex, false));
                    } else {
                        revOrderKeysList.add(new OrderKey(fIndex, true));
                    }
                }

                this.orderKeys = revOrderKeysList;
            }

            @ProcessElement
            public void processElement(@Element KV<Row, Iterable<Row>> keyRows,
                                       OutputReceiver<KV<Row, Iterable<Row>>> out) {
                ArrayList<Row> rows = new ArrayList<Row>();
                for(Row i : keyRows.getValue()) {
                    rows.add(i);
                }
                for(OrderKey i : orderKeys) {
                    int fIndex = i.getIndex();
                    boolean dir = i.getDir();
                    rows.sort(new sortComparator(fIndex, dir));
                }
                //TODO: Change the comparator to the row comparator:
                // https://github.com/apache/beam/blob/master/sdks/java/extensions/sql/src/main/java/org/apache/beam/sdk/extensions/sql/impl/rel/BeamSortRel.java#L373

                out.output(KV.of(keyRows.getKey(), rows));
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
                    Schema.Field fd = cSchema.getField(fIndex);
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
