package org.apache.beam.sdk.extensions.sql.impl.udaf;

import org.apache.beam.sdk.transforms.Combine;
import java.lang.Object;

public class ArrayAgg {

    public static class ArrayAggArray extends Combine.CombineFn<Object, Object[], Object[]> {


        @Override
        public Object[] createAccumulator() {
            return new Object[10]; //size is kept constant for now, will later make it dynamic
        }

        @Override
        public Object[] addInput(Object[] mutableAccumulator, Object input) {
            mutableAccumulator[0] = input;
            return mutableAccumulator;
        }

        @Override
        public Object[] mergeAccumulators(Iterable<Object[]> accumulators) {
            Object[] mergeObject= new Object[]{};
            for (Object[] accum: accumulators ){
                if (accum != null){
                    mergeObject = accum;
                }else{
                    mergeObject = accum;
                }
            }
            return  mergeObject;
        }

        @Override
        public Object[] extractOutput(Object[] accumulator) {
            return new Object[0];
        }
    }
}
