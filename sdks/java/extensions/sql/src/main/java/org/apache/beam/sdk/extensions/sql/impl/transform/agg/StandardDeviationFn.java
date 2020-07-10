package org.apache.beam.sdk.extensions.sql.impl.transform.agg;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;

public class StandardDeviationFn<T extends Number> extends Combine.CombineFn<T, VarianceAccumulator, Double> {

    private VarianceFn internal;

    private static final boolean SAMPLE = true;
    private static final boolean POP = false;

    private <V extends Number> StandardDeviationFn(boolean isSample, Schema.TypeName typeName) {
        internal = isSample ? VarianceFn.newSample(typeName) : VarianceFn.newPopulation(typeName);
    }

    public static StandardDeviationFn newPopulation(Schema.TypeName typeName) {
        return new StandardDeviationFn<>(POP, typeName);
    }


    public static StandardDeviationFn newSample(Schema.TypeName typeName) {
        return new StandardDeviationFn<>(SAMPLE, typeName);
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
    public Double extractOutput(VarianceAccumulator accumulator) {
        return Math.sqrt(internal.extractOutput(accumulator).doubleValue());
    }


}
