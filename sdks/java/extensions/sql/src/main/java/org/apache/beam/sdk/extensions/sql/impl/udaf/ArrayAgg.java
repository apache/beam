package org.apache.beam.sdk.extensions.sql.impl.udaf;

import org.apache.beam.sdk.transforms.Combine;

public class ArrayAgg {

    public static class ArrayAggArray extends Combine.CombineFn<Object, Object[], Object[]> {

        @Override
        public Object[] createAccumulator() {
            return new Object[0];
        }

        @Override
        public Object[] addInput(Object[] mutableAccumulator, Object input) {

            if(input != null){
                if(mutableAccumulator != null) {
                    mutableAccumulator[mutableAccumulator.length + 1] = input;
                }
            }
            return  mutableAccumulator;
        }

        @Override
        public Object[] mergeAccumulators(Iterable<Object[]> accumulators) {
            Object[] mergeObject= new Object[]{};

        }

        @Override
        public Object[] extractOutput(Object[] accumulator) {
            return accumulator;
        }
    }

    }
