package org.apache.beam.sdk.extensions.sql.impl.transform.agg;

import org.apache.beam.sdk.transforms.Combine;

public class CountIf {

    private CountIf(){}

    /** Returns a {@link Combine.CombineFn} that counts the number of its inputs.
     * @return*/
    public static CountIfFn combineFn() {
        return new CountIf.CountIfFn();
    }

    public static class CountIfFn extends Combine.CombineFn<Boolean, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer addInput(Integer mutableAccumulator, Boolean input) {
            if (input)
                mutableAccumulator += 1;
            return  mutableAccumulator;
        }

        @Override
        public Integer mergeAccumulators(Iterable<Integer> accumulators) {
            Integer count = 0;
            for (Integer accum : accumulators) {
                count += accum;
            }
            return count;
        }

        @Override
        public Integer extractOutput(Integer accumulator) {
            return accumulator;
        }

    }

}
