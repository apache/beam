package org.apache.beam.sdk.extensions.sql.impl.udaf;

import org.apache.beam.sdk.transforms.Combine;

public class ArrayAgg {

    public static class ArrayAggArray extends Combine.CombineFn<Object, Object, Object> {

        private static final String delimiter = ",";

        @Override
        public Object createAccumulator() {
            return new Object();
        }

        @Override
        public Object addInput(Object currentElement, Object nextElement) {
            if(nextElement != null){
               if (currentElement != null){
                   currentElement += ArrayAggArray.delimiter + nextElement;
               }else {
                   currentElement = nextElement;
               }
            }
            return currentElement;
        }

        @Override
        public Object mergeAccumulators(Iterable<Object> accumulators) {

            Object mergeObject = new Object();

            for (Object accum: accumulators ){
                if (accum != null){
                    if (mergeObject != null) {
                        mergeObject += ArrayAggArray.delimiter + accum;
                    }else{
                        mergeObject = accum;
                    }
                }
            }
            return mergeObject;
        }

        @Override
        public Object extractOutput(Object output) {
            return output;
        }
    }
}
