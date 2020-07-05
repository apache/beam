package org.apache.beam.sdk.extensions.sql.impl.rel;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.impl.SqlConversionException;
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
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlOperator;
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
        for(Map.Entry<String, RexNode> entry : patternDefinitions.entrySet()) {
            String k = entry.getKey();
            RexCall v = (RexCall) entry.getValue();
            LOG.info("key: " + k + ", value: " + v.getOperator().getName());
            for(RexNode i: v.getOperands()) {
                LOG.info(i.toString());
                if(i.getKind() == SqlKind.LAST) {
                    RexCall j = (RexCall) i;
                    for(RexNode t: j.getOperands()) {
                        LOG.info(t.getClass().toString());
                    }
                }
            }
        }

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
                .apply(ParDo.of(new SortPerKey(mySchema, orderKeys)));

            // apply the pattern match in each partition
            PCollection<KV<Row, Iterable<Row>>> matchedUpstream = orderedUpstream
                .apply(ParDo.of(new MatchPattern(mySchema, pattern, patternDefs)));

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

            private final Schema mySchema;
            private final RexCall pattern;
            private final Map<String, RexNode> patternDefs;
            private final String regexPattern;

            public MatchPattern(Schema mySchema, RexNode pattern, Map<String, RexNode> patternDefs) {
                this.mySchema = mySchema;
                this.pattern = (RexCall) pattern;
                this.patternDefs = patternDefs;
                this.regexPattern = getRegexFromPattern(this.pattern);
            }

            // recursively change a RexNode into a regular expr
            // TODO: support quantifiers: PATTERN_QUANTIFIER('A', 1, -1, false) false?
            private String getRegexFromPattern(RexNode call) {
                if(call.getClass() == RexLiteral.class) {
                    return ((RexLiteral) call).getValueAs(String.class);
                } else {
                    RexCall oprs = (RexCall) call;
                    if(oprs.getOperands().size() == 2) {
                        return getRegexFromPattern(oprs.getOperands().get(0)) +
                                getRegexFromPattern(oprs.getOperands().get(1));
                    } else {
                        return getRegexFromPattern(oprs.getOperands().get(0));
                    }
                }
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
                    for(int j = 0; j < regexPattern.length(); ++j) {
                        String tryPattern = Character.toString(regexPattern.charAt(j));
                        RexCall call = (RexCall) patternDefs.get(tryPattern);
                        if(call != null) {
                            if (evalCondition(i,
                                    (RexCall) patternDefs.get(tryPattern))) {
                                patternOfRow = tryPattern;
                                break;
                            }
                        } else {
                            patternOfRow = tryPattern;
                            break;
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

            // Last(*.$1, 1) || NEXT(*.$1, 0)
            private Comparable evalOperation(RexCall operation, Row rowEle) {
                SqlOperator call = operation.getOperator();
                List<RexNode> operands = operation.getOperands();

                if(call.getKind() == SqlKind.LAST) {// support only simple match for now: LAST(*.$, 0)
                        RexNode opr0 = operands.get(0);
                        RexNode opr1 = operands.get(1);
                        if(opr0.getClass() == RexPatternFieldRef.class
                                && RexLiteral.intValue(opr1) == 0) {
                            int fIndex = ((RexPatternFieldRef) opr0).getIndex();
                            Schema.Field fd = mySchema.getField(fIndex);
                            Schema.FieldType dtype = fd.getType();

                            switch (dtype.getTypeName()) {
                                case BYTE:
                                    return rowEle.getByte(fIndex);
                                case INT16:
                                    return rowEle.getInt16(fIndex);
                                case INT32:
                                    return rowEle.getInt32(fIndex);
                                case INT64:
                                    return rowEle.getInt64(fIndex);
                                case DECIMAL:
                                    return rowEle.getDecimal(fIndex);
                                case FLOAT:
                                    return rowEle.getFloat(fIndex);
                                case DOUBLE:
                                    return rowEle.getDouble(fIndex);
                                case STRING:
                                    return rowEle.getString(fIndex);
                                case DATETIME:
                                    return rowEle.getDateTime(fIndex);
                                case BOOLEAN:
                                    return rowEle.getBoolean(fIndex);
                                default:
                                    throw new SqlConversionException("specified column not comparable");
                            }
                        }
                }
                throw new SqlConversionException("backward functions (PREV, NEXT) not supported for now");
            }

            // a function that evaluates whether a row is a match
            private boolean evalCondition(Row rowEle, RexCall pattern) {
                SqlOperator call = pattern.getOperator();
                List<RexNode> operands = pattern.getOperands();
                RexCall opr0 = (RexCall) operands.get(0);
                RexLiteral opr1 = (RexLiteral) operands.get(1);

                switch(call.getKind()) {
                    case EQUALS:
                        return evalOperation(opr0, rowEle).compareTo(opr1.getValue()) == 0;
                    case GREATER_THAN:
                        return evalOperation(opr0, rowEle).compareTo(opr1.getValue()) > 0;
                    case GREATER_THAN_OR_EQUAL:
                        return evalOperation(opr0, rowEle).compareTo(opr1.getValue()) >= 0;
                    case LESS_THAN:
                        return evalOperation(opr0, rowEle).compareTo(opr1.getValue()) < 0;
                    case LESS_THAN_OR_EQUAL:
                        return evalOperation(opr0, rowEle).compareTo(opr1.getValue()) <= 0;
                    default:
                        return false;
                }
            }
        }

        private static class SortPerKey extends DoFn<KV<Row, Iterable<Row>>, KV<Row, Iterable<Row>>> {

            private final Schema mySchema;
            private final List<RelFieldCollation> orderKeys;

            public SortPerKey(Schema mySchema, RelCollation orderKeys) {
                this.mySchema = mySchema;
                List<RelFieldCollation> revOrderKeys = orderKeys.getFieldCollations();
                Collections.reverse(revOrderKeys);
                this.orderKeys = revOrderKeys;
            }

            @ProcessElement
            public void processElement(@Element KV<Row, Iterable<Row>> keyRows,
                                       OutputReceiver<KV<Row, Iterable<Row>>> out) {
                ArrayList<Row> rows = new ArrayList<Row>();
                for(Row i : keyRows.getValue()) {
                    rows.add(i);
                }
                for(RelFieldCollation i : orderKeys) {
                    int fIndex = i.getFieldIndex();
                    RelFieldCollation.Direction dir = i.getDirection();
                    if (dir == RelFieldCollation.Direction.ASCENDING) {
                        rows.sort(new sortComparator(fIndex, true));
                    }
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
