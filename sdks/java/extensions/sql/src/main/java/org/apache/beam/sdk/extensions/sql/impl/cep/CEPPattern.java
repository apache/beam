package org.apache.beam.sdk.extensions.sql.impl.cep;

import org.apache.beam.sdk.extensions.sql.impl.SqlConversionException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlOperator;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;

// a pattern class that stores the definition of a single pattern
public class CEPPattern implements Serializable {

    private final Schema mySchema;
    private final String patternVar;
    private final PatternCondition patternCondition;
    private final Quantifier quant;

    private CEPPattern(Schema mySchema,
                    String patternVar,
                    @Nullable RexCall patternDef
                    ) {

        this.mySchema = mySchema;
        this.patternVar = patternVar;
        this.quant = Quantifier.NONE;

        if(patternDef == null) {
            this.patternCondition = new PatternCondition(this) {
                @Override
                public boolean eval(Row eleRow) {
                    return true;
                }
            };
            return;
        }

        // transform a rexnode into a pattern condition class
        SqlOperator call = patternDef.getOperator();
        List<RexNode> operands = patternDef.getOperands();
        RexCall opr0 = (RexCall) operands.get(0);
        RexLiteral opr1 = (RexLiteral) operands.get(1);

        switch(call.getKind()) {
            case EQUALS:
                this.patternCondition = new PatternCondition(this) {

                    @Override public boolean eval(Row rowEle) {
                    return evalOperation(opr0, rowEle)
                        .compareTo(opr1.getValue()) == 0;
                    }
                };
                break;
            case GREATER_THAN:
                this.patternCondition = new PatternCondition(this) {

                    @Override public boolean eval(Row rowEle) {
                        return evalOperation(opr0, rowEle)
                                .compareTo(opr1.getValue()) > 0;
                    }
                };
                break;
            case GREATER_THAN_OR_EQUAL:
                this.patternCondition = new PatternCondition(this) {

                    @Override public boolean eval(Row rowEle) {
                        return evalOperation(opr0, rowEle)
                                .compareTo(opr1.getValue()) >= 0;
                    }
                };
                break;
            case LESS_THAN:
                this.patternCondition = new PatternCondition(this) {

                    @Override public boolean eval(Row rowEle) {
                        return evalOperation(opr0, rowEle)
                                .compareTo(opr1.getValue()) < 0;
                    }
                };
                break;
            case LESS_THAN_OR_EQUAL:
                this.patternCondition = new PatternCondition(this) {

                    @Override public boolean eval(Row rowEle) {
                        return evalOperation(opr0, rowEle)
                                .compareTo(opr1.getValue()) <= 0;
                    }
                };
                break;
            default:
                throw new SqlConversionException(String.format("Comparison operator not supported: %s",
                        call.kind.toString()));
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

    public boolean evalRow(Row rowEle) {
        return patternCondition.eval(rowEle);
    }

    @Override
    public String toString() {
        return patternVar + quant.toString();
    }

    public static CEPPattern of(Schema theSchema, String patternVar, RexCall patternDef) {
        return new CEPPattern(theSchema, patternVar, patternDef);
    }
}
