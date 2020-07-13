package org.apache.beam.sdk.extensions.sql.impl.transform.agg;

import org.apache.beam.sdk.extensions.sql.impl.utils.BigDecimalConverter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.SerializableFunction;

import java.math.BigDecimal;

public class StandardDeviationFn<T extends Number> extends Combine.CombineFn<T, VarianceAccumulator, T> {

    private VarianceFn internal;

    private static final boolean SAMPLE = true;
    private static final boolean POP = false;
    private SerializableFunction<BigDecimal, T> decimalConverter;

    private StandardDeviationFn(boolean isSample, Schema.TypeName typeName, SerializableFunction<BigDecimal, T> decimalConverter ) {
        internal = isSample ? VarianceFn.newSample(typeName) : VarianceFn.newPopulation(typeName);
        this.decimalConverter = decimalConverter;
    }

    public static StandardDeviationFn newPopulation(Schema.TypeName typeName) {
        return new StandardDeviationFn<>(POP, typeName, BigDecimalConverter.forSqlType(typeName));
    }


    public static StandardDeviationFn newSample(Schema.TypeName typeName) {
        return new StandardDeviationFn<>(SAMPLE, typeName, BigDecimalConverter.forSqlType(typeName));
    }



    @Override
    public VarianceAccumulator createAccumulator() {
        return internal.createAccumulator();
    }

    @Override
    public VarianceAccumulator addInput(VarianceAccumulator mutableAccumulator, T input) {
        return internal.addInput(mutableAccumulator, input);
    }

    @Override
    public VarianceAccumulator mergeAccumulators(Iterable<VarianceAccumulator> accumulators) {
        return internal.mergeAccumulators(accumulators);
    }

    @Override
    public T extractOutput(VarianceAccumulator accumulator) {
        BigDecimal result = BigDecimal.valueOf(Math.sqrt(internal.extractOutput(accumulator).doubleValue()));
        return decimalConverter.apply(result);
    }


}
